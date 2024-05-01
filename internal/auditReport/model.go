package auditReport

import (
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"time"
)

type AuditReport struct {
	Id                 uuid.UUID      `json:"id"`
	Name               string         `json:"name"`
	Description        string         `json:"description"`
	Status             string         `json:"status"`
	TenantId           string         `json:"tenant_id"`
	Configuration      datatypes.JSON `json:"configuration"`
	DownloadLink       string         `json:"download_link"`
	DownloadLinkExpiry string         `json:"download_link_expiry"`
	Retries            int            `json:"retries"`
}

type FailedRequests struct {
	RequestId string `json:"requestId"`
	TenantId  string `json:"tenantId"`
	Retry     int    `json:"retry"`
	Error     string `json:"error"`
}

func NewFailedRequest(reqId string, tenantId string, retry int, err string) FailedRequests {
	return FailedRequests{
		RequestId: reqId,
		Retry:     retry,
		Error:     err,
	}
}

func getAllReportRequests(db *gorm.DB) ([]AuditReport, error) {
	var auditReportRequests []AuditReport
	var status = []string{STATUS_REQUESTED, STATUS_FAILED}
	maxRetries := 3
	err := db.Table("audit_report").Where("status in ? and retries <= ? ", status, maxRetries).Find(&auditReportRequests).Error
	if err != nil {
		return nil, err
	}
	return auditReportRequests, nil
}

func updateRequestStatus(db *gorm.DB, id string, status string) error {
	err := db.Table("audit_report").Where("id = ?", id).Update("status", status).Error
	if err != nil {
		return err
	}
	return nil
}
func updateRequestStatusAndDownloadLink(db *gorm.DB, id string, status string, link string, expiry time.Time) error {
	err := db.Table("audit_report").Where("id = ?", id).Updates(map[string]interface{}{"status": status, "download_link": link, "download_link_expiry": expiry}).Error
	if err != nil {
		return err
	}
	return nil
}
func updateRequestStatusAndRetries(db *gorm.DB, id string, status string, retry int) error {
	err := db.Table("audit_report").Where("id = ?", id).Updates(map[string]interface{}{"status": status, "retries": retry}).Error
	if err != nil {
		return err
	}
	return nil
}
