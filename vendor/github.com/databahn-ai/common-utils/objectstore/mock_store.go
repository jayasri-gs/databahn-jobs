package objectstore

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

// MockStore is an in-memory ObjectStore implementation for testing.
type MockStore struct {
	mu      sync.RWMutex
	objects map[string]map[string][]byte // container -> key -> data
}

// NewMockStore creates a new in-memory mock object store.
func NewMockStore() *MockStore {
	return &MockStore{
		objects: make(map[string]map[string][]byte),
	}
}

func (m *MockStore) Get(ctx context.Context, container, key string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	keys, ok := m.objects[container]
	if !ok {
		return nil, fmt.Errorf("object not found: %s/%s", container, key)
	}
	data, ok := keys[key]
	if !ok {
		return nil, fmt.Errorf("object not found: %s/%s", container, key)
	}
	// Return a copy to prevent mutation
	result := make([]byte, len(data))
	copy(result, data)
	return result, nil
}

func (m *MockStore) Put(ctx context.Context, container, key string, data []byte, opts ...*PutOptions) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.objects[container] == nil {
		m.objects[container] = make(map[string][]byte)
	}
	m.objects[container][key] = append([]byte(nil), data...)
	return nil
}

func (m *MockStore) PutStream(ctx context.Context, container, key string, reader io.Reader, opts ...*PutOptions) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("failed to read from reader: %w", err)
	}
	return m.Put(ctx, container, key, data, opts...)
}

func (m *MockStore) Post(ctx context.Context, container, key string, data []byte) error {
	return m.Put(ctx, container, key, data)
}

func (m *MockStore) Delete(ctx context.Context, container, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	keys, ok := m.objects[container]
	if !ok {
		return nil // idempotent
	}
	if _, ok := keys[key]; !ok {
		return nil // idempotent
	}
	delete(keys, key)
	if len(keys) == 0 {
		delete(m.objects, container)
	}
	return nil
}

func (m *MockStore) List(ctx context.Context, container, prefix string) ([]ObjectInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var objects []ObjectInfo
	keys, ok := m.objects[container]
	if !ok {
		return objects, nil
	}

	for key := range keys {
		if strings.HasPrefix(key, prefix) {
			objects = append(objects, ObjectInfo{
				Key:          key,
				LastModified: time.Now(),
			})
		}
	}

	sort.Slice(objects, func(i, j int) bool {
		return objects[i].Key < objects[j].Key
	})

	return objects, nil
}

func (m *MockStore) GetPresignedURL(ctx context.Context, container, key string, expiry time.Duration) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	keys, ok := m.objects[container]
	if !ok {
		return "", fmt.Errorf("object not found: %s/%s", container, key)
	}
	if _, ok := keys[key]; !ok {
		return "", fmt.Errorf("object not found: %s/%s", container, key)
	}

	return fmt.Sprintf("https://mock.storage/%s/%s?expiry=%s", container, key, expiry), nil
}
