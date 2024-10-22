package persistent

import (
	"github.com/outofforest/quantum/types"
)

// NewDummyStore creates new dummy store.
func NewDummyStore() *DummyStore {
	return &DummyStore{}
}

// DummyStore defines persistent no-op store.
type DummyStore struct{}

// Write is a no-op implementation.
func (s *DummyStore) Write(_ types.PhysicalAddress, _ []byte) error {
	return nil
}

// Sync does nothing.
func (s *DummyStore) Sync() error {
	return nil
}
