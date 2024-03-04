package replaymanager

import (
	"github.com/databahn-ai/databahn-jobs/internal/eventsequencing/model"
	"strings"
	"sync"
)

type SortStore struct {
	sync.Mutex
	sortMap map[string][]model.RawEvent
}

func (sst *SortStore) GetSortStore() map[string][]model.RawEvent {
	return sst.sortMap
}

func NewSortStore() (*SortStore, error) {

	sortStore := SortStore{
		sortMap: make(map[string][]model.RawEvent),
	}

	return &sortStore, nil
}

func (sst *SortStore) AddToSortStore(outerKey string, value []model.RawEvent) {
	sst.Mutex.Lock()
	innerSlice := sst.sortMap[outerKey]
	innerSlice = value
	sst.sortMap[outerKey] = innerSlice
	sst.Mutex.Unlock()
}

func (sst *SortStore) Get(outerKey string) []model.RawEvent {
	sst.Mutex.Lock()
	innerSlice := sst.sortMap[outerKey]
	if innerSlice == nil {
		innerSlice = make([]model.RawEvent, 0)
	}
	sst.Mutex.Unlock()
	return innerSlice
}

func (sst *SortStore) DeleteSStData(key string) {
	sst.Mutex.Lock()
	delete(sst.sortMap, key)
	sst.Mutex.Unlock()
}

func (sst *SortStore) GetOuterkey(tenant string, source string, destination string) string {

	return tenant + "-" + source + "-" + destination
}

func (sst *SortStore) GetReverseKeys(key string) (tenant string, source string, destination string) {

	keys := strings.Split(key, "-")
	return keys[0], keys[1], keys[2]
}

func (sst *SortStore) CleanUp() {

	sst.sortMap = make(map[string][]model.RawEvent)
}
