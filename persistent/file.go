package persistent

import (
	"io"
	"os"

	"github.com/pkg/errors"

	"github.com/outofforest/quantum/types"
)

// NewFileStore creates new file-based store.
func NewFileStore(file *os.File) *FileStore {
	return &FileStore{
		file: file,
	}
}

// FileStore defines persistent file-based store.
type FileStore struct {
	file *os.File
}

// Write writes data to the store.
func (s *FileStore) Write(address types.PhysicalAddress, data []byte) error {
	if _, err := s.file.Seek(int64(address), io.SeekStart); err != nil {
		return errors.WithStack(err)
	}
	_, err := s.file.Write(data)
	return errors.WithStack(err)
}

// Sync syncs pending writes.
func (s *FileStore) Sync() error {
	return errors.WithStack(s.file.Sync())
}
