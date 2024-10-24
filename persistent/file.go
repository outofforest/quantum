package persistent

import (
	"os"

	"github.com/pkg/errors"
	"golang.org/x/sys/unix"

	"github.com/outofforest/quantum/types"
)

// NewFileStore creates new file-based store.
func NewFileStore(file *os.File, size uint64) (*FileStore, func(), error) {
	data, err := unix.Mmap(int(file.Fd()), 0, int(size), unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_SHARED)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "memory allocation failed")
	}

	return &FileStore{
			file: file,
			data: data,
		}, func() {
			_ = unix.Munmap(data)
			_ = file.Close()
		}, nil
}

// FileStore defines persistent file-based store.
type FileStore struct {
	file *os.File
	data []byte
}

// Write writes data to the store.
func (s *FileStore) Write(address types.PersistentAddress, data []byte) error {
	copy(s.data[address:], data)
	return nil
}

// Sync syncs pending writes.
func (s *FileStore) Sync() error {
	if err := unix.Msync(s.data, unix.MS_SYNC); err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(s.file.Sync())
}
