package changeflag

import (
	"errors"

	flagUtil "github.com/databahn-ai/common-utils/changeflag"
	"github.com/databahn-ai/common-utils/configuration"
	"github.com/databahn-ai/go-logging/logger"
	"go.uber.org/zap"
)

type ParsedChangeFlags[T WithSecret] struct {
	ParsedChangeFlag T
	ChangeFlag       *flagUtil.ChangeFlag
	Error            error
}

func (p ParsedChangeFlags[T]) BuildAck() (*flagUtil.Acknowledgement, bool) {
	if p.Error != nil {
		return flagUtil.ErrorAcknowledgementFromChangeFlag(*p.ChangeFlag, p.Error.Error()), false
	}
	return flagUtil.SuccessAcknowledgementFromChangeFlag(*p.ChangeFlag), true
}

func FilterSelectAll[T WithSecret](ws T) bool {
	return true
}

var FilterSelectAllDest = FilterSelectAll[FlagDestination]
var FilterSelectAllSource = FilterSelectAll[FlagSource]
var FilterSelectAllRouteProcessor = FilterSelectAll[FlagRouteProcessor]

func ParseDestinationFlag(configReader configuration.ConfigReader, changeFlag flagUtil.ChangeFlag,
	filter func(dest FlagDestination) bool) (*ParsedChangeFlags[FlagDestination], error) {
	return parseChangeFlag[FlagDestination](configReader, changeFlag, filter, parseDestination, validateDestination)
}

func ParseSourceFlag(configReader configuration.ConfigReader, changeFlag flagUtil.ChangeFlag,
	filter func(dest FlagSource) bool) (*ParsedChangeFlags[FlagSource], error) {
	return parseChangeFlag[FlagSource](configReader, changeFlag, filter, parseSource, validateSource)
}

func ParseRouteProcessorFlag(configReader configuration.ConfigReader, changeFlag flagUtil.ChangeFlag,
	filter func(dest FlagRouteProcessor) bool) (*ParsedChangeFlags[FlagRouteProcessor], error) {
	return parseChangeFlag[FlagRouteProcessor](configReader, changeFlag, filter, parseRouteProcessor, validateRouteProcessorFlag)
}

func ParseDestinationFlags(configReader configuration.ConfigReader, changeFlags []flagUtil.ChangeFlag,
	filter func(dest FlagDestination) bool) ([]*ParsedChangeFlags[FlagDestination], error) {
	return parseFlagsWithSecrets[FlagDestination](configReader, changeFlags, filter, parseDestination, validateDestination)
}

func ParseSourceFlags(configReader configuration.ConfigReader, changeFlags []flagUtil.ChangeFlag,
	filter func(dest FlagSource) bool) ([]*ParsedChangeFlags[FlagSource], error) {
	return parseFlagsWithSecrets[FlagSource](configReader, changeFlags, filter, parseSource, validateSource)
}

func ParseRouteProcessorFlags(configReader configuration.ConfigReader, changeFlags []flagUtil.ChangeFlag,
	filter func(dest FlagRouteProcessor) bool) ([]*ParsedChangeFlags[FlagRouteProcessor], error) {
	return parseFlagsWithSecrets[FlagRouteProcessor](configReader, changeFlags, filter, parseRouteProcessor, validateRouteProcessorFlag)
}

func parseChangeFlag[T WithSecret](configReader configuration.ConfigReader, changeFlag flagUtil.ChangeFlag,
	filter func(dest T) bool, parser func([]byte) (T, error), validator func(T) error) (*ParsedChangeFlags[T], error) {
	parsedChangeFlag, err := parser(changeFlag.Entity)
	if err != nil {
		return &ParsedChangeFlags[T]{Error: err, ChangeFlag: &changeFlag}, nil
	}
	if filter(parsedChangeFlag) {
		secretId := parsedChangeFlag.GetSecretId()
		if secretId != "" {
			log := logger.GetLogger()
			log.Debug("changeflag parseChangeFlag: loading single secret",
				zap.String("entityId", changeFlag.EntityId),
				zap.String("tenantId", changeFlag.TenantId))
			secretIdsByTenant := make(map[string][]string)
			secretIdsByTenant[changeFlag.TenantId] = []string{secretId}
			secrets, err := LoadSecrets(configReader, secretIdsByTenant)
			if err != nil {
				log.Debug("changeflag parseChangeFlag: LoadSecrets failed",
					zap.String("entityId", changeFlag.EntityId),
					zap.String("tenantId", changeFlag.TenantId),
					zap.Error(err))
				return nil, err
			}
			for _, secret := range secrets {
				if errMsg, ok := secret.Errors[secretId]; ok {
					log.Debug("changeflag parseChangeFlag: API returned error for secret",
						zap.String("entityId", changeFlag.EntityId),
						zap.String("tenantId", changeFlag.TenantId),
						zap.String("errorMessage", errMsg))
					return &ParsedChangeFlags[T]{Error: errors.New(errMsg), ChangeFlag: &changeFlag}, nil
				} else if secretMap, ok := secret.Secrets[secretId]; ok {
					log.Debug("changeflag parseChangeFlag: secret resolved, applying config (values omitted)",
						zap.String("entityId", changeFlag.EntityId),
						zap.String("tenantId", changeFlag.TenantId),
						zap.Int("configKeyCount", len(secretMap)))
					parsedChangeFlag.AddConfig(secretMap)
					errr := validator(parsedChangeFlag)
					if errr != nil {
						log.Debug("changeflag parseChangeFlag: validation failed after secret merge",
							zap.String("entityId", changeFlag.EntityId),
							zap.String("tenantId", changeFlag.TenantId),
							zap.Error(errr))
						return &ParsedChangeFlags[T]{Error: errr, ChangeFlag: &changeFlag}, nil
					}
					log.Debug("changeflag parseChangeFlag: success",
						zap.String("entityId", changeFlag.EntityId),
						zap.String("tenantId", changeFlag.TenantId))
					return &ParsedChangeFlags[T]{ParsedChangeFlag: parsedChangeFlag, ChangeFlag: &changeFlag}, nil
				} else {
					log.Debug("changeflag parseChangeFlag: secret id missing from success payload",
						zap.String("entityId", changeFlag.EntityId),
						zap.String("tenantId", changeFlag.TenantId),
						zap.Int("responseSecretsKeys", len(secret.Secrets)),
						zap.Int("responseErrorsKeys", len(secret.Errors)))
					return &ParsedChangeFlags[T]{Error: errors.New("secret not found"), ChangeFlag: &changeFlag}, nil
				}
			}

		} else {
			log := logger.GetLogger()
			log.Debug("changeflag parseChangeFlag: skipping data plane secret fetch",
				zap.String("reason", "entity has no secret id"),
				zap.String("entityId", changeFlag.EntityId),
				zap.String("tenantId", changeFlag.TenantId))
			return &ParsedChangeFlags[T]{ParsedChangeFlag: parsedChangeFlag, ChangeFlag: &changeFlag}, nil
		}
	}
	return nil, nil
}

func parseFlagsWithSecrets[T WithSecret](configReader configuration.ConfigReader, changeFlags []flagUtil.ChangeFlag,
	filter func(dest T) bool, parser func([]byte) (T, error), validator func(T) error) ([]*ParsedChangeFlags[T], error) {
	var parsedCfList []*ParsedChangeFlags[T]
	secretIdToParsedChangeFlag := make(map[string]T)
	secretIdToChangeFlag := make(map[string]*flagUtil.ChangeFlag)
	secretMaps := make(map[string]map[string]string)

	var latestChangeFlagList []*flagUtil.ChangeFlag
	latestChangeFlags := make(map[string]*flagUtil.ChangeFlag)
	for _, lcf := range changeFlags {
		cf := lcf
		latestChangeFlags[cf.EntityId] = &cf
	}
	for _, lcf := range latestChangeFlags {
		cf := lcf
		latestChangeFlagList = append(latestChangeFlagList, cf)
	}

	for _, lcf := range latestChangeFlagList {
		cf := lcf
		parsedEntity, err := parser(cf.Entity)
		if err != nil {
			parsedCfList = append(parsedCfList, &ParsedChangeFlags[T]{Error: err, ChangeFlag: cf})
			continue
		}
		if filter(parsedEntity) {
			secretId := parsedEntity.GetSecretId()
			if secretId != "" {
				secretIdToParsedChangeFlag[secretId] = parsedEntity
				secretIdToChangeFlag[secretId] = cf
			} else {
				parsedCfList = append(parsedCfList, &ParsedChangeFlags[T]{ParsedChangeFlag: parsedEntity, ChangeFlag: cf})
			}
		}
	}
	log := logger.GetLogger()

	if len(secretIdToChangeFlag) == 0 {
		log.Debug("changeflag parseFlagsWithSecrets: skipping data plane secret fetch",
			zap.String("reason", "no parsed flags reference a secret id"),
			zap.Int("changeFlagCount", len(latestChangeFlagList)),
			zap.Int("parsedWithoutSecretSoFar", len(parsedCfList)))
	} else {
		secretIdsByTenant := make(map[string][]string)
		for secretId, cf := range secretIdToChangeFlag {
			secretIdsByTenant[cf.TenantId] = append(secretIdsByTenant[cf.TenantId], secretId)
		}

		log.Debug("changeflag parseFlagsWithSecrets: collected secret refs",
			zap.Int("uniqueSecretIds", len(secretIdToChangeFlag)),
			zap.Int("parsedWithoutSecret", len(parsedCfList)),
			zap.Int("tenantsWithSecrets", len(secretIdsByTenant)))
		for tid, ids := range secretIdsByTenant {
			log.Debug("changeflag parseFlagsWithSecrets: tenant secret id list",
				zap.String("tenantId", tid),
				zap.Int("count", len(ids)))
		}

		secrets, err := LoadSecrets(configReader, secretIdsByTenant)
		if err != nil {
			log.Debug("changeflag parseFlagsWithSecrets: LoadSecrets failed",
				zap.Error(err))
			return nil, err
		}
		for _, secret := range secrets {
			log.Debug("changeflag parseFlagsWithSecrets: merging shard",
				zap.String("tenantId", secret.TenantId),
				zap.Int("secretsKeys", len(secret.Secrets)),
				zap.Int("errorsKeys", len(secret.Errors)))
			for secretId, configMap := range secret.Secrets {
				secretMaps[secretId] = configMap
				log.Debug("changeflag parseFlagsWithSecrets: cached config for secret (values omitted)",
					zap.String("tenantId", secret.TenantId),
					zap.Int("configKeyCount", len(configMap)))
			}
			for secretId, errMsg := range secret.Errors {
				if cf, ok := secretIdToChangeFlag[secretId]; ok {
					log.Debug("changeflag parseFlagsWithSecrets: enqueue error result",
						zap.String("tenantId", secret.TenantId),
						zap.String("entityId", cf.EntityId),
						zap.String("errorMessage", errMsg))
					parsedCfList = append(parsedCfList, &ParsedChangeFlags[T]{Error: errors.New(errMsg), ChangeFlag: cf})
				} else {
					log.Debug("changeflag parseFlagsWithSecrets: API error for unknown secret id (no matching change flag)",
						zap.String("tenantId", secret.TenantId))
				}
			}
		}
	}

	for secretId, configMap := range secretMaps {
		if pcf, ok := secretIdToParsedChangeFlag[secretId]; ok {
			cf := secretIdToChangeFlag[secretId]
			log.Debug("changeflag parseFlagsWithSecrets: apply secret to parsed entity (values omitted)",
				zap.String("entityId", cf.EntityId),
				zap.String("tenantId", cf.TenantId),
				zap.Int("configKeyCount", len(configMap)))
			pcf.AddConfig(configMap)
			errr := validator(pcf)
			if errr != nil {
				log.Debug("changeflag parseFlagsWithSecrets: validation failed",
					zap.String("entityId", cf.EntityId),
					zap.Error(errr))
				parsedCfList = append(parsedCfList, &ParsedChangeFlags[T]{Error: errr, ChangeFlag: cf})
			} else {
				log.Debug("changeflag parseFlagsWithSecrets: validation ok",
					zap.String("entityId", cf.EntityId))
				parsedCfList = append(parsedCfList, &ParsedChangeFlags[T]{ParsedChangeFlag: pcf, ChangeFlag: cf})
			}
		} else {
			log.Debug("changeflag parseFlagsWithSecrets: secret in maps but no parsed entity (unexpected)")
		}
	}

	nErr := 0
	nOk := 0
	for _, p := range parsedCfList {
		if p.Error != nil {
			nErr++
		} else {
			nOk++
		}
	}
	log.Debug("changeflag parseFlagsWithSecrets: done",
		zap.Int("resultCount", len(parsedCfList)),
		zap.Int("successCount", nOk),
		zap.Int("errorCount", nErr))
	return parsedCfList, nil
}
