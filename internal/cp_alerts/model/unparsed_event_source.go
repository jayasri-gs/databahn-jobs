package model

import "github.com/databahn-ai/databahn-jobs/internal/store/source"

type UnparsedEventSource struct {
	Source *source.Source
}

func NewUnparsedEventSource(source *source.Source) *UnparsedEventSource {
	return &UnparsedEventSource{
		Source: source,
	}
}

func (ias UnparsedEventSource) GetEntityId() string {
	return ias.Source.ID.String()
}
func (ias UnparsedEventSource) GetEntityName() string {
	return ias.Source.Name
}
func (ias UnparsedEventSource) GetDataPlaneId() string {
	return ias.Source.DataPlaneId.String()
}
func (ias UnparsedEventSource) GetTenantId() string {
	return ias.Source.TenantID.String()
}
