package auditReport

import (
	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type AuditReport struct {
	Id            uuid.UUID      `json:"id"`
	Name          string         `json:"name"`
	Description   string         `json:"description"`
	Status        string         `json:"status"`
	TenantId      string         `json:"tenant_id"`
	Configuration datatypes.JSON `json:"configuration"`
	DownloadLink  string         `json:"download_link"`
}

func getAllReportRequests(db *gorm.DB) ([]AuditReport, error) {
	var auditReportRequests []AuditReport
	err := db.Table("audit_report").Where(" status = ?", STATUS_REQUESTED).Find(&auditReportRequests).Error
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
func updateRequestStatusAndDownloadLink(db *gorm.DB, id string, status string, link string) error {
	err := db.Table("audit_report").Where("id = ?", id).Updates(map[string]interface{}{"status": status, "download_link": link}).Error
	if err != nil {
		return err
	}
	return nil
}
