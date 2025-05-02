package changeflag

import (
	"errors"

	flagUtil "github.com/databahn-ai/common-utils/changeflag"
	"github.com/databahn-ai/common-utils/configuration"
)

type ParsedChangeFlags[T WithSecret] struct {
	ParsedChangeFlag T
	ChangeFlag       *flagUtil.ChangeFlag
	Error            error
}

func (p ParsedChangeFlags[T]) BuildAck() (*flagUtil.Acknowledgement, bool) {
	if p.Error != nil {
		return flagUtil.ErrorAcknowledgement(p.ChangeFlag.RequestId, p.ChangeFlag.EntityType,
			p.ChangeFlag.EntityId, p.ChangeFlag.TenantId, p.ChangeFlag.Action, p.Error.Error()), false
	}
	return flagUtil.SuccessAcknowledgement(p.ChangeFlag.RequestId, p.ChangeFlag.EntityType,
		p.ChangeFlag.EntityId, p.ChangeFlag.TenantId, p.ChangeFlag.Action), true
}

func FilterSelectAll[T WithSecret](ws T) bool {
	return true
}

var FilterSelectAllDest = FilterSelectAll[FlagDestination]
var FilterSelectAllSource = FilterSelectAll[FlagSource]
var FilterSelectAllRouteProcessor = FilterSelectAll[FlagRouteProcessor]

func ParseDestinationFlag(configReader configuration.ConfigReader, changeFlag flagUtil.ChangeFlag,
	filter func(dest FlagDestination) bool) *ParsedChangeFlags[FlagDestination] {
	return parseChangeFlag[FlagDestination](configReader, changeFlag, filter, parseDestination, validateDestination)
}

func ParseSourceFlag(configReader configuration.ConfigReader, changeFlag flagUtil.ChangeFlag,
	filter func(dest FlagSource) bool) *ParsedChangeFlags[FlagSource] {
	return parseChangeFlag[FlagSource](configReader, changeFlag, filter, parseSource, validateSource)
}

func ParseRouteProcessorFlag(configReader configuration.ConfigReader, changeFlag flagUtil.ChangeFlag,
	filter func(dest FlagRouteProcessor) bool) *ParsedChangeFlags[FlagRouteProcessor] {
	return parseChangeFlag[FlagRouteProcessor](configReader, changeFlag, filter, parseRouteProcessor, validateRouteProcessorFlag)
}

func ParseDestinationFlags(configReader configuration.ConfigReader, changeFlags []flagUtil.ChangeFlag,
	filter func(dest FlagDestination) bool) []*ParsedChangeFlags[FlagDestination] {
	return parseFlagsWithSecrets[FlagDestination](configReader, changeFlags, filter, parseDestination, validateDestination)
}

func ParseSourceFlags(configReader configuration.ConfigReader, changeFlags []flagUtil.ChangeFlag,
	filter func(dest FlagSource) bool) []*ParsedChangeFlags[FlagSource] {
	return parseFlagsWithSecrets[FlagSource](configReader, changeFlags, filter, parseSource, validateSource)
}

func ParseRouteProcessorFlags(configReader configuration.ConfigReader, changeFlags []flagUtil.ChangeFlag,
	filter func(dest FlagRouteProcessor) bool) []*ParsedChangeFlags[FlagRouteProcessor] {
	return parseFlagsWithSecrets[FlagRouteProcessor](configReader, changeFlags, filter, parseRouteProcessor, validateRouteProcessorFlag)
}

func parseChangeFlag[T WithSecret](configReader configuration.ConfigReader, changeFlag flagUtil.ChangeFlag,
	filter func(dest T) bool, parser func([]byte) (T, error), validator func(T) error) *ParsedChangeFlags[T] {
	parsedChangeFlag, err := parser(changeFlag.Entity)
	if err != nil {
		return &ParsedChangeFlags[T]{Error: err, ChangeFlag: &changeFlag}
	}
	if filter(parsedChangeFlag) {
		secretId := parsedChangeFlag.GetSecretId()
		if secretId != "" {
			secretIdsByTenant := make(map[string][]string)
			secretIdsByTenant[changeFlag.TenantId] = []string{secretId}
			secrets, err := LoadSecrets(configReader, secretIdsByTenant)
			if err != nil {
				return &ParsedChangeFlags[T]{Error: err, ChangeFlag: &changeFlag}
			}
			for _, secret := range secrets {
				if errMsg, ok := secret.Errors[secretId]; ok {
					return &ParsedChangeFlags[T]{Error: errors.New(errMsg), ChangeFlag: &changeFlag}
				} else if secretMap, ok := secret.Secrets[secretId]; ok {
					parsedChangeFlag.AddConfig(secretMap)
					errr := validator(parsedChangeFlag)
					if errr != nil {
						return &ParsedChangeFlags[T]{Error: errr, ChangeFlag: &changeFlag}
					}
					return &ParsedChangeFlags[T]{ParsedChangeFlag: parsedChangeFlag, ChangeFlag: &changeFlag}
				} else {
					return &ParsedChangeFlags[T]{Error: errors.New("secret not found"), ChangeFlag: &changeFlag}
				}
			}

		} else {
			return &ParsedChangeFlags[T]{ParsedChangeFlag: parsedChangeFlag, ChangeFlag: &changeFlag}
		}
	}
	return nil
}

func parseFlagsWithSecrets[T WithSecret](configReader configuration.ConfigReader, changeFlags []flagUtil.ChangeFlag,
	filter func(dest T) bool, parser func([]byte) (T, error), validator func(T) error) []*ParsedChangeFlags[T] {
	var parsedCfList []*ParsedChangeFlags[T]
	secretIdToParsedChaneFlag := make(map[string]T)
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
				secretIdToParsedChaneFlag[secretId] = parsedEntity
				secretIdToChangeFlag[secretId] = cf
			} else {
				parsedCfList = append(parsedCfList, &ParsedChangeFlags[T]{ParsedChangeFlag: parsedEntity, ChangeFlag: cf})
			}
		}
	}
	secretIdsByTenant := make(map[string][]string)
	for secretId, cf := range secretIdToChangeFlag {
		secretIdsByTenant[cf.TenantId] = append(secretIdsByTenant[cf.TenantId], secretId)
	}
	secrets, err := LoadSecrets(configReader, secretIdsByTenant)
	if err != nil {
		for _, cf := range secretIdToChangeFlag {
			parsedCfList = append(parsedCfList, &ParsedChangeFlags[T]{Error: err, ChangeFlag: cf})
		}
	}
	for _, secret := range secrets {
		for secretId, configMap := range secret.Secrets {
			secretMaps[secretId] = configMap
		}
		for secretId, errMsg := range secret.Errors {
			if cf, ok := secretIdToChangeFlag[secretId]; ok {
				parsedCfList = append(parsedCfList, &ParsedChangeFlags[T]{Error: errors.New(errMsg), ChangeFlag: cf})
			}
		}
	}

	for secretId, configMap := range secretMaps {
		if pcf, ok := secretIdToParsedChaneFlag[secretId]; ok {
			pcf.AddConfig(configMap)
			errr := validator(pcf)
			cf := secretIdToChangeFlag[secretId]
			if errr != nil {
				parsedCfList = append(parsedCfList, &ParsedChangeFlags[T]{Error: errr, ChangeFlag: cf})
			} else {
				parsedCfList = append(parsedCfList, &ParsedChangeFlags[T]{ParsedChangeFlag: pcf, ChangeFlag: cf})
			}
		}
	}
	return parsedCfList
}
