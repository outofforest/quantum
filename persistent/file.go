package persistent

import (
	"io"
	"os"
	"syscall"

	"github.com/godzie44/go-uring/uring"
	"github.com/pkg/errors"

	"github.com/outofforest/quantum/types"
)

const (
	// We found that when this value is too large, together with O_DIRECT option it produces random ECANCELED errors
	// on CQE. Our guess for now is that it might be caused by some queue overflows in the NVME device.
	submitCount  = 2 << 5
	mod          = submitCount - 1
	ringCapacity = 10 * submitCount
)

// NewFileStore creates new file-based store.
func NewFileStore(file *os.File) (*FileStore, error) {
	size, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		_ = file.Close()
		return nil, errors.WithStack(err)
	}

	ringR, err := uring.New(1)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	ringW, err := uring.New(ringCapacity, uring.WithCQSize(ringCapacity))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &FileStore{
		ringR:   ringR,
		ringW:   ringW,
		fd:      file.Fd(),
		file:    file,
		size:    uint64(size),
		cqeBuff: make([]*uring.CQEvent, ringCapacity),
	}, nil
}

// FileStore defines persistent file-based store.
type FileStore struct {
	ringR, ringW *uring.Ring
	fd           uintptr
	file         *os.File
	size         uint64
	numOfEvents  uint32
	cqeBuff      []*uring.CQEvent
	err          error
}

// Size returns the size of the store.
func (s *FileStore) Size() uint64 {
	return s.size
}

// Read writes data to the store.
func (s *FileStore) Read(address types.PersistentAddress, data []byte) error {
	if err := s.ringR.QueueSQE(uring.Read(s.fd, data, uint64(address)*types.NodeLength), 0, 0); err != nil {
		return errors.WithStack(err)
	}

	_, err := s.ringR.Submit()
	if err != nil {
		return errors.WithStack(err)
	}

	for {
		cqe, err := s.ringR.WaitCQEvents(1)
		switch {
		case err == nil:
			if err := cqe.Error(); err != nil {
				return errors.WithStack(err)
			}
			s.ringR.AdvanceCQ(1)
			return nil
		case errors.Is(err, syscall.EINTR):
		default:
			return errors.WithStack(err)
		}
	}
}

// Write writes data to the store.
func (s *FileStore) Write(address types.PersistentAddress, data []byte) error {
	if err := s.ringW.QueueSQE(uring.Write(s.fd, data, uint64(address)*types.NodeLength), 0, 0); err != nil {
		return errors.WithStack(err)
	}

	s.numOfEvents++

	// FIXME (wojciech): There will be more addresses representing singularity node.
	if s.numOfEvents&mod == 0 || address == 0 {
		if _, err := s.ringW.Submit(); err != nil {
			return errors.WithStack(err)
		}
	}

	if s.numOfEvents < ringCapacity && address != 0 {
		return nil
	}

	return s.awaitCompletionEvents(address == 0)
}

// Close closes the store.
func (s *FileStore) Close() {
	if s.err == nil && s.numOfEvents > 0 {
		if _, err := s.ringW.Submit(); err == nil {
			_ = s.awaitCompletionEvents(true)
		}
	}
	_ = s.ringR.Close()
	_ = s.ringW.Close()
	_ = s.file.Close()
}

func (s *FileStore) awaitCompletionEvents(finalize bool) error {
	// Due to asynchronous preemption
	// https://go.dev/doc/go1.14#runtime
	// https://unskilled.blog/posts/preemption-in-go-an-introduction/
	// Go runtime sends SIGURG to the thread to stop executing currently running goroutine.
	// Signal causes the currently long-running syscall to exit with
	// "interrupted system call" error. In that case operation must be repeated.
	// In case of urings, the affected part is the syscall awaiting events.
	// Looks like that when entire thread is dedicated to one goroutine (using runtime.LockOSThread),
	// that mechanism is turned off on that thread. But it is a rare case so we decided to not incorporate this for now.

	var minNumOfEvents uint32 = submitCount

	for {
		if finalize {
			minNumOfEvents = s.numOfEvents
		}

		numOfEvents := uint32(s.ringW.PeekCQEventBatch(s.cqeBuff))
		if numOfEvents < minNumOfEvents {
			_, err := s.ringW.WaitCQEvents(minNumOfEvents - numOfEvents)
			if err != nil && !errors.Is(err, syscall.EINTR) {
				return errors.WithStack(err)
			}
			numOfEvents = uint32(s.ringW.PeekCQEventBatch(s.cqeBuff))
		}
		for i := range numOfEvents {
			if s.cqeBuff[i].Res < 0 {
				s.err = s.cqeBuff[i].Error()
				return errors.WithStack(s.err)
			}
		}
		s.ringW.AdvanceCQ(numOfEvents)
		s.numOfEvents -= numOfEvents

		if !finalize || s.numOfEvents == 0 {
			return nil
		}
	}
}
