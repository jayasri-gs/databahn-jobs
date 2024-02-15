package fleet

import (
	"context"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type FleetNode struct {
	Id            uuid.UUID      `gorm:"primarykey;type:uuid"`
	Name          string         `json:"name" validate:"required"`
	FleetId       uuid.UUID      `gorm:"type:uuid" json:"fleet_id"`
	TenantUUID    uuid.UUID      `gorm:"type:uuid" validate:"required" json:"-"`
	CustomerId    uuid.UUID      `gorm:"type:uuid" validate:"required" json:"-"`
	CreatedBy     uuid.UUID      `validate:"required" gorm:"type:uuid"`
	CpuCount      uint           `json:"cpu_count"  validate:"required"`
	IsDeleted     bool           `json:"is_deleted"`
	IsDisabled    bool           `json:"is_disabled"`
	Status        int            `validate:"min=0,max=5" json:"status"`
	Role          string         `json:"role" validate:"required"`
	CpuArch       string         `json:"cpu_arch"  validate:"required"`
	PrivateIP     string         `gorm:"size:20" validate:"required" json:"private_ip"`
	PublicIP      string         `gorm:"size:20" validate:"required" json:"public_ip"`
	Hostname      string         `gorm:"size:100" validate:"required" json:"hostname"`
	Uptime        uint64         `json:"uptime"  validate:"required"`
	BootTime      uint64         `json:"boot_time"  validate:"required"`
	OS            string         `gorm:"size:100" validate:"required" json:"os"`
	Platform      string         `gorm:"size:100" validate:"required" json:"platform"`
	KernelVersion string         `gorm:"size:100" validate:"required" json:"kernel_version"`
	KernelArch    string         `gorm:"size:100" validate:"required" json:"kernel_arch"`
	CreatedAt     time.Time      `json:"created_at"`
	UpdatedAt     time.Time      `json:"updated_at"`
	HeartbeatAt   time.Time      `json:"heatbeat_at" gorm:"autoUpdateTime"`
	IsInitNode    bool           `json:"is_init_node"`
	Usage         FleetNodeStats `gorm:"foreignKey:node_id;references:id"`
}

func (*FleetNode) Migrate(db *gorm.DB) error {
	return db.AutoMigrate(&FleetNode{})
}

func (nd *FleetNode) Create(ctx context.Context, db *gorm.DB) error {
	return db.WithContext(ctx).Create(nd).Error
}

func (nd *FleetNode) Update(ctx context.Context, db *gorm.DB) error {
	return db.WithContext(ctx).Save(nd).Error
}

func DeleteLeader(ctx context.Context, db *gorm.DB, id, fleetId, tenantId string) error {
	return db.WithContext(ctx).Delete(FleetNode{}, "id = ? AND fleet_id = ? AND tenant_uuid = ?", id, fleetId, tenantId).Error
}

func (nd *FleetNode) UpdateIps(ctx context.Context, db *gorm.DB, publicIp, privateIp string) error {
	return db.WithContext(ctx).Model(&nd).Where("tenant_id = ? AND id = ?", nd.TenantUUID, nd.Id).UpdateColumn("public_ip", publicIp).UpdateColumn("private_ip", privateIp).Error
}

func GetNodes(ctx context.Context, db *gorm.DB, role string, fleetId, tenantId uuid.UUID) (w []*FleetNode, err error) {
	return getNodes(ctx, db, role, fleetId, tenantId)
}

func GetNode(ctx context.Context, db *gorm.DB, nodeId, fleetId, tenantId uuid.UUID) (n *FleetNode, err error) {
	err = db.WithContext(ctx).Find(&n, "tenant_uuid = ? AND fleet_id = ? AND id = ?", tenantId, fleetId, nodeId).Error

	return n, err
}

func GetNodeById(ctx context.Context, db *gorm.DB, nodeId, tenantId uuid.UUID) (n *FleetNode, err error) {
	err = db.WithContext(ctx).Find(&n, "tenant_uuid = ? AND id = ?", tenantId, nodeId).Error
	return n, err
}

func GetLeaders(ctx context.Context, db *gorm.DB, fleetId, tenantId uuid.UUID) (w []*FleetNode, err error) {
	return getNodes(ctx, db, RoleLeader, fleetId, tenantId)
}

func GetLeaderCount(ctx context.Context, db *gorm.DB, fleetId, tenantId uuid.UUID) (count int64, err error) {
	return getNodeCount(ctx, db, RoleLeader, fleetId, tenantId)
}

func GetWorkerCount(ctx context.Context, db *gorm.DB, fleetId, tenantId uuid.UUID) (count int64, err error) {
	return getNodeCount(ctx, db, RoleWorker, fleetId, tenantId)
}

func GetWorkers(ctx context.Context, db *gorm.DB, fleetId, tenantId uuid.UUID) (w []*FleetNode, err error) {
	return getNodes(ctx, db, RoleWorker, fleetId, tenantId)
}

func GetFleetNodes(ctx context.Context, db *gorm.DB, fleetId, tenantId uuid.UUID) (w []*FleetNode, err error) {
	err = db.WithContext(ctx).Find(&w, "tenant_uuid = ? AND fleet_id = ?", tenantId, fleetId).Error
	return w, err
}

func getNodes(ctx context.Context, db *gorm.DB, role string, fleetId, tenantId uuid.UUID) (w []*FleetNode, err error) {
	err = db.WithContext(ctx).Find(&w, "tenant_uuid = ? AND fleet_id = ? AND role = ?", tenantId, fleetId, role).Error
	return w, err
}

func getNodeCount(ctx context.Context, db *gorm.DB, role string, fleetId, tenantId uuid.UUID) (count int64, err error) {
	err = db.WithContext(ctx).Model(&FleetNode{}).Where("tenant_uuid = ? AND fleet_id = ? AND role = ?", tenantId, fleetId, role).Count(&count).Error
	return count, err
}

func HeartbeatNode(ctx context.Context, db *gorm.DB, id, tenant_id string) error {
	return db.Model(FleetNode{}).Where("id =? AND tenant_uuid = ?", id, tenant_id).UpdateColumn("heartbeat_at", time.Now()).Error
}
