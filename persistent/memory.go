package persistent

import (
	"syscall"

	"github.com/pkg/errors"

	"github.com/outofforest/quantum/types"
)

// NewMemoryStore creates new in-memory "persistent" store.
func NewMemoryStore(size uint64, useHugePages bool) (*MemoryStore, func(), error) {
	opts := syscall.MAP_SHARED | syscall.MAP_ANONYMOUS | syscall.MAP_POPULATE
	if useHugePages {
		opts |= syscall.MAP_HUGETLB
	}
	data, err := syscall.Mmap(-1, 0, int(size), syscall.PROT_READ|syscall.PROT_WRITE, opts)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "memory allocation failed")
	}

	return &MemoryStore{
			data: data,
		}, func() {
			_ = syscall.Munmap(data)
		}, nil
}

// MemoryStore defines "persistent" in-memory store. Used for testing.
type MemoryStore struct {
	data []byte
}

// Write writes data to the store.
func (s *MemoryStore) Write(address types.PersistentAddress, data []byte) error {
	copy(s.data[address:], data)
	return nil
}

// Sync does nothing.
func (s *MemoryStore) Sync() error {
	return nil
}
