package persistent

import (
	"os"
	"syscall"

	"github.com/pkg/errors"

	"github.com/outofforest/quantum/types"
)

// NewFileStore creates new file-based store.
func NewFileStore(file *os.File, size uint64) (*FileStore, func(), error) {
	data, err := syscall.Mmap(int(file.Fd()), 0, int(size), syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "memory allocation failed")
	}

	return &FileStore{
			file: file,
			data: data,
		}, func() {
			_ = syscall.Munmap(data)
			_ = file.Close()
		}, nil
}

// FileStore defines persistent file-based store.
type FileStore struct {
	file *os.File
	data []byte
}

// Write writes data to the store.
func (s *FileStore) Write(address types.PhysicalAddress, data []byte) error {
	copy(s.data[address:], data)
	return nil
}

// Sync syncs pending writes.
func (s *FileStore) Sync() error {
	return errors.WithStack(s.file.Sync())
}
