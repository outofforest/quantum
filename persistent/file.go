package persistent

import (
	"os"
	"syscall"

	"github.com/godzie44/go-uring/uring"
	"github.com/pkg/errors"

	"github.com/outofforest/quantum/types"
)

const ringCapacity = 50

// NewFileStore creates new file-based store.
func NewFileStore(file *os.File) (*FileStore, error) {
	ring, err := uring.New(ringCapacity)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &FileStore{
		ring: ring,
		fd:   file.Fd(),
		file: file,
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

	return s.submit()
}

// Sync syncs pending writes.
func (s *FileStore) Sync() error {
	if s.counter > 0 {
		if err := s.submit(); err != nil {
			return errors.WithStack(err)
		}
	}

	// FIXME (wojciech): Fsync is implemented by io uring but not implemented in the library.
	// Fork that library or write our own, possibly a better one?
	// return errors.WithStack(s.file.Sync())

	return nil
}

// Close closes the store.
func (s *FileStore) Close() {
	_ = s.ring.Close()
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

	for {
		cqe, err := s.ring.SubmitAndWaitCQEvents(s.counter)
		switch {
		case err == nil:
			s.counter = 0
			return errors.WithStack(cqe.Error())
		case errors.Is(err, syscall.EINTR):
		default:
			return errors.WithStack(err)
		}
	}
}
