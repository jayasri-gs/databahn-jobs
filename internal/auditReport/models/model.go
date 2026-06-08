package models

import (
	"github.com/databahn-ai/databahn-jobs/internal/auditReport/consts"
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"time"
)

type AuditReport struct {
	Id                  uuid.UUID      `json:"id"`
	Name                string         `json:"name"`
	Description         string         `json:"description"`
	ReportType          string         `json:"report_type"`
	Status              string         `json:"status"`
	TenantId            string         `json:"tenant_id"`
	AuditReportFilter   datatypes.JSON `json:"audit_report_filter"`
	ReportConfiguration datatypes.JSON `json:"report_configuration"`
	DownloadLink        string         `json:"download_link"`
	DownloadLinkExpiry  string         `json:"download_link_expiry"`
	Retries             int            `json:"retries"`
}

type FailedRequests struct {
	RequestId string `json:"requestId"`
	Name      string `json:"name"`
	TenantId  string `json:"tenantId"`
	Retry     int    `json:"retry"`
	Error     string `json:"error"`
}

func NewFailedRequest(reqId string, name string, tenantId string, retry int, err string) FailedRequests {
	return FailedRequests{
		RequestId: reqId,
		Name:      name,
		TenantId:  tenantId,
		Retry:     retry,
		Error:     err,
	}
}

func GetAllReportRequests(db *gorm.DB) ([]AuditReport, error) {
	var auditReportRequests []AuditReport
	var status = []string{consts.REQUESTED, consts.FAILED}
	err := db.Table("audit_report").Where("status in ? and retries < ? ", status, consts.MaxRetries).Find(&auditReportRequests).Error
	if err != nil {
		return nil, err
	}
	return auditReportRequests, nil
}

func UpdateRequestStatus(db *gorm.DB, id string, status string) error {
	err := db.Table("audit_report").Where("id = ?", id).Update("status", status).Error
	if err != nil {
		return err
	}
	return nil
}
func UpdateRequestStatusAndDownloadLink(db *gorm.DB, id string, status string, link string, expiry time.Time) error {
	err := db.Table("audit_report").Where("id = ?", id).Updates(map[string]interface{}{"status": status, "download_link": link, "download_link_expiry": expiry}).Error
	if err != nil {
		return err
	}
	return nil
}
func UpdateRequestStatusAndRetries(db *gorm.DB, id string, status string, retry int) error {
	err := db.Table("audit_report").Where("id = ?", id).Updates(map[string]interface{}{"status": status, "retries": retry}).Error
	if err != nil {
		return err
	}
	return nil
}
