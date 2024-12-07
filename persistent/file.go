package persistent

import (
	"io"
	"os"
	"syscall"

	"github.com/godzie44/go-uring/uring"
	"github.com/pkg/errors"

	"github.com/outofforest/quantum/types"
)

const ringCapacity = 10000

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
	ringW, err := uring.New(ringCapacity)
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
	counter      uint
	numOfEvents  uint
	cqeBuff      []*uring.CQEvent
}

// Size returns the size of the store.
func (s *FileStore) Size() uint64 {
	return s.size
}

// Read writes data to the store.
func (s *FileStore) Read(address types.PersistentAddress, data []byte) error {
	if err := s.ringR.QueueSQE(uring.Read(s.fd, data, uint64(address)*types.NodeLength), 0, uint64(address)); err != nil {
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
			if cqe.UserData != uint64(address) {
				return errors.New("unexpected user data")
			}
			return nil
		case errors.Is(err, syscall.EINTR):
		default:
			return errors.WithStack(err)
		}
	}
}

// Write writes data to the store.
func (s *FileStore) Write(address types.PersistentAddress, data []byte) error {
	if err := s.ringW.QueueSQE(uring.Write(s.fd, data, uint64(address)*types.NodeLength), 0, uint64(address)); err != nil {
		return errors.WithStack(err)
	}

	s.counter++

	if s.counter == 1000 {
		numOfEvents, err := s.ringW.Submit()
		if err != nil {
			return errors.WithStack(err)
		}
		s.numOfEvents += numOfEvents
		s.counter = 0
	}

	if s.numOfEvents < ringCapacity {
		return nil
	}

	return s.submit()
}

// Sync syncs pending writes.
func (s *FileStore) Sync() error {
	numOfEvents, err := s.ringW.Submit()
	if err != nil {
		return errors.WithStack(err)
	}
	s.numOfEvents += numOfEvents

	if err := s.submit(); err != nil {
		return errors.WithStack(err)
	}

	// FIXME (wojciech): Fsync is implemented by io uring but not implemented in the library.
	// Fork that library or write our own, possibly a better one?
	// return errors.WithStack(s.file.Sync())

	return nil
}

// Close closes the store.
func (s *FileStore) Close() {
	_ = s.ringR.Close()
	_ = s.ringW.Close()
	_ = s.file.Close()
}

func (s *FileStore) submit() error {
	// Due to asynchronous preemption
	// https://go.dev/doc/go1.14#runtime
	// https://unskilled.blog/posts/preemption-in-go-an-introduction/
	// Go runtime sends SIGURG to the thread to stop executing currently running goroutine.
	// Signal causes the currently long-running syscall to exit with
	// "interrupted system call" error. In that case operation must be repeated.
	// In case of urings, the affected part is the syscall awaiting events.
	// Looks like that when entire thread is dedicated to one goroutine (using runtime.LockOSThread),
	// that mechanism is turned off on that thread. But it is a rare case so we decided to not incorporate this for now.

	for s.numOfEvents > 0 {
	loop:
		for {
			_, err := s.ringW.WaitCQEvents(uint32(s.numOfEvents))
			switch {
			case err == nil:
				break loop
			case errors.Is(err, syscall.EINTR):
			default:
				return errors.WithStack(err)
			}
		}

		numOfEvents := s.ringW.PeekCQEventBatch(s.cqeBuff)
		for i := range numOfEvents {
			if s.cqeBuff[i].Res < 0 {
				return errors.WithStack(s.cqeBuff[i].Error())
			}
		}
		s.ringW.AdvanceCQ(uint32(numOfEvents))
		s.numOfEvents -= uint(numOfEvents)
	}

	return nil
}
