package persistent

import (
	"syscall"

	"github.com/pkg/errors"
)

// NewMemoryStore creates new in-memory "persistent" store.
func NewMemoryStore(size uint64, useHugePages bool) (*MemoryStore, func(), error) {
	opts := syscall.MAP_SHARED | syscall.MAP_ANONYMOUS | syscall.MAP_NORESERVE | syscall.MAP_POPULATE
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

// Size returns size of the store.
func (s *MemoryStore) Size() uint64 {
	return uint64(len(s.data))
}

// Write writes data to the store.
func (s *MemoryStore) Write(offset uint64, data []byte) error {
	copy(s.data[offset:], data)
	return nil
}
