package persistent

import (
	"os"

	"github.com/godzie44/go-uring/uring"
	"github.com/pkg/errors"

	"github.com/outofforest/quantum/types"
)

const ringCapacity = 50

// NewFileStore creates new file-based store.
func NewFileStore(file *os.File) (*FileStore, func(), error) {
	ring, err := uring.New(ringCapacity)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	return &FileStore{
			ring: ring,
			fd:   file.Fd(),
			file: file,
		}, func() {
			_ = ring.Close()
		}, nil
}

// FileStore defines persistent file-based store.
type FileStore struct {
	ring    *uring.Ring
	fd      uintptr
	file    *os.File
	counter uint32
}

// Write writes data to the store.
func (s *FileStore) Write(address types.NodeAddress, data []byte) error {
	if err := s.ring.QueueSQE(uring.Write(s.fd, data, uint64(address)*types.NodeLength), 0, 0); err != nil {
		return errors.WithStack(err)
	}
	s.counter++
	if s.counter < ringCapacity {
		return nil
	}

	cqe, err := s.ring.SubmitAndWaitCQEvents(s.counter)
	s.counter = 0
	if err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(cqe.Error())
}

// Sync syncs pending writes.
func (s *FileStore) Sync() error {
	if s.counter > 0 {
		cqe, err := s.ring.SubmitAndWaitCQEvents(s.counter)
		s.counter = 0
		if err != nil {
			return errors.WithStack(err)
		}
		if err := cqe.Error(); err != nil {
			return errors.WithStack(err)
		}
	}
	return errors.WithStack(s.file.Sync())
}
