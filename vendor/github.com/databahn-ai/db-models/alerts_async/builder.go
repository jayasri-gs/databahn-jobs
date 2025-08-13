package alerts_async

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

type NoSecondaryEntityId struct{}

func (NoSecondaryEntityId) GetSecondaryEntityId() string {
	return ""
}

type AlertEntity interface {
	GetEntityId() string
	GetEntityName() string
	GetDataPlaneId() string
	GetTenantId() string
	GetSecondaryEntityId() string
}

type AlertOption func(alert *Alert)

/*
NewAlert is a builder for an Alert.
It allows you to create an alert with various options.
Some of the fields are optional and some are required.
Many fields can have limited values, such as functionality, criticality, and alert type.
Please use appropriate enums for those options and if new values are needed, please add them to the enums.
It internally creates a unique ID for the alert based on the tenant ID, entity ID, entity name, functionality, and functionality type.
*/
func NewAlert(functionality Functionality, options ...AlertOption) (*Alert, error) {
	alert := defaultAlert()
	alert.Functionality = functionality.String()
	for _, opt := range options {
		opt(alert)
	}
	err := validateAlert(alert)
	if err != nil {
		return nil, err
	}
	alert.Id = buildId(alert)
	alert.Status = AlertOpen.Value()
	if alert.AlertType == "" {
		alert.AlertType = External.String()
	}
	return alert, nil
}

func buildId(a *Alert) string {
	alertIdBuilder := fmt.Sprintf(
		"tenantId=%s&entityId=%s&entityName=%s&functionality=%s&type=%s",
		a.TenantId,
		a.FunctionalityEntityId,
		a.FunctionalityEntityName,
		a.Functionality,
		a.FunctionalityType,
	)
	if a.SecondaryEntityId != "" {
		alertIdBuilder += fmt.Sprintf("&secondaryEntityId=%s", a.SecondaryEntityId)
	}
	hash := sha256.Sum256([]byte(alertIdBuilder))
	return hex.EncodeToString(hash[:])
}

func validateAlert(alert *Alert) error {
	if alert.Functionality == "" {
		return errors.New("functionality is required for alert")
	}
	if alert.Criticality == "" {
		return errors.New("criticality is required for alert")
	}
	if alert.Title == "" {
		return errors.New("title is required for alert")
	}
	if alert.Message == "" {
		return errors.New("message is required for alert")
	}
	if alert.TenantId == "" {
		return errors.New("tenant id is required for alert")
	}
	if alert.DataPlaneId == "" {
		return errors.New("data plane id is required for alert")
	}
	if alert.FunctionalityEntityId == "" {
		return errors.New("entity id is required for alert")
	}
	if alert.FunctionalityEntityName == "" {
		return errors.New("entity name is required for alert")
	}
	if alert.FunctionalityType == "" {
		return errors.New("functionality type is required for alert")
	}
	if alert.ErrorCode == "" {
		return errors.New("error code is required for alert")
	}
	return nil
}

func defaultAlert() *Alert {
	now := time.Now().UTC().UnixMilli()
	return &Alert{
		Dismissed:       false,
		Status:          AlertOpen.Value(),
		FirstObservedAt: now,
		LastObservedAt:  now,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
}

func WithEntity(entity AlertEntity) AlertOption {
	return func(alert *Alert) {
		alert.FunctionalityEntityId = entity.GetEntityId()
		alert.FunctionalityEntityName = entity.GetEntityName()
		alert.DataPlaneId = entity.GetDataPlaneId()
		alert.TenantId = entity.GetTenantId()
		alert.SecondaryEntityId = entity.GetSecondaryEntityId()
	}
}

func WithEntityDetails(entityId, entityName string, dataPlaneId string, tenantId string) AlertOption {
	return func(alert *Alert) {
		alert.FunctionalityEntityId = entityId
		alert.FunctionalityEntityName = entityName
		alert.DataPlaneId = dataPlaneId
		alert.TenantId = tenantId
	}
}

func WithEntityDetailsWithSecondaryEntityId(entityId, entityName string, dataPlaneId string, tenantId string, secondaryEntityId string) AlertOption {
	return func(alert *Alert) {
		alert.FunctionalityEntityId = entityId
		alert.FunctionalityEntityName = entityName
		alert.DataPlaneId = dataPlaneId
		alert.TenantId = tenantId
		alert.SecondaryEntityId = secondaryEntityId
	}
}

func WithCriticality(criticality Criticality) AlertOption {
	return func(alert *Alert) {
		alert.Criticality = criticality.String()
	}
}

func WithFunctionalityType(functionalityType FunctionalityType) AlertOption {
	return func(alert *Alert) {
		alert.FunctionalityType = functionalityType.String()
	}
}

func WithErrorCode(errorCode ErrorCode, messageTemplateParam string) AlertOption {
	return func(alert *Alert) {
		alert.ErrorCode = errorCode.Value()
		alert.ErrorMessage = strings.ReplaceAll(errorCode.MessageTemplate(), "{0}", messageTemplateParam)
	}
}

func WithAlertType(alertType AlertType) AlertOption {
	return func(alert *Alert) {
		alert.AlertType = alertType.String()
	}
}

func WithTitle(title string) AlertOption {
	return func(alert *Alert) {
		alert.Title = title
	}
}

func WithMessage(message string) AlertOption {
	return func(alert *Alert) {
		alert.Message = message
	}
}
