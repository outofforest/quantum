package persistent

import (
	"io"
	"os"
	"runtime"
	"syscall"
	"time"
	"unsafe"

	"github.com/pkg/errors"

	"github.com/outofforest/go-uring/uring"
	"github.com/outofforest/quantum/types"
)

const (
	// We found that when this value is too large, together with O_DIRECT option it produces random ECANCELED errors
	// on CQE. Our guess for now is that it might be caused by some queue overflows in the NVME device.
	submitCount  = 2 << 5
	mod          = submitCount - 1
	ringCapacity = 10 * submitCount
)

// NewStore creates new persistent store.
func NewStore(file *os.File) (*Store, error) {
	size, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		_ = file.Close()
		return nil, errors.WithStack(err)
	}

	return &Store{
		file: file,
		size: uint64(size),
	}, nil
}

// Store defines persistent store.
type Store struct {
	file *os.File
	size uint64
}

// Size returns the size of the store.
func (s *Store) Size() uint64 {
	return s.size
}

// NewWriter creates new store writer.
func (s *Store) NewWriter(volatileOrigin unsafe.Pointer, volatileSize uint64) (*Writer, error) {
	return newWriter(s.file, volatileOrigin, volatileSize)
}

// Close closes the store.
func (s *Store) Close() {
	_ = s.file.Close()
}

func newWriter(
	file *os.File,
	volatileOrigin unsafe.Pointer,
	volatileSize uint64,
) (*Writer, error) {
	// Required by uring.SetupSingleIssuer.
	runtime.LockOSThread()

	ring, err := uring.New(ringCapacity,
		uring.WithCQSize(ringCapacity),
		uring.WithFlags(uring.SetupSingleIssuer),
		uring.WithSQPoll(100*time.Millisecond),
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// By multiplying uring.MaxBufferSize * uring.MaxNumOfBuffers we see that currently it is possible to address
	// 16TB of space using predfined buffers.
	// The memory used by buffers is locked by the kernel which means tha `ulimit -l` command must report at least that
	// size + margin for other possible locks if needed.

	numOfBuffers := volatileSize / uring.MaxBufferSize
	if numOfBuffers > uring.MaxNumOfBuffers {
		return nil, errors.Errorf("number of buffers %d esceeds the maximum allowed number of buffers %d",
			numOfBuffers, uring.MaxNumOfBuffers)
	}
	v := make([]syscall.Iovec, 0, numOfBuffers)
	for i := range numOfBuffers {
		v = append(v, syscall.Iovec{
			Base: (*byte)(unsafe.Add(volatileOrigin, i*uring.MaxBufferSize)),
			Len:  uring.MaxBufferSize,
		})
	}

	if err := ring.RegisterBuffers(v); err != nil {
		return nil, errors.WithStack(err)
	}

	return &Writer{
		fd:      int32(file.Fd()),
		origin:  volatileOrigin,
		ring:    ring,
		cqeBuff: make([]*uring.CQEvent, ringCapacity),
	}, nil
}

// Writer writes nodes to the persistent store.
type Writer struct {
	fd      int32
	origin  unsafe.Pointer
	ring    *uring.Ring
	cqeBuff []*uring.CQEvent

	numOfEvents uint32
	err         error
}

// Write writes data to the store.
func (w *Writer) Write(dstAddress types.PersistentAddress, srcAddress types.VolatileAddress) error {
	sqe, err := w.ring.NextSQE()
	if err != nil {
		return errors.WithStack(err)
	}
	// We don't need to do this because we never set them so they are 0s in the entire ring all the time.
	// sqe.IoPrio = 0
	// sqe.UserData = 0
	// sqe.Personality = 0
	// sqe.SpliceFdIn = 0

	sqe.OpCode = uint8(uring.WriteFixed)
	sqe.Flags = 0
	sqe.OpcodeFlags = 0
	sqe.Fd = w.fd
	sqe.Len = types.NodeLength
	sqe.Off = uint64(dstAddress) * types.NodeLength
	sqe.Addr = uint64(uintptr(unsafe.Add(w.origin, uint64(srcAddress)*types.NodeLength)))
	sqe.BufIG = uint16(srcAddress * types.NodeLength / uring.MaxBufferSize)

	w.numOfEvents++

	switch {
	// Address 0 means we store singularity node. It is always the last write in the commit.
	// That's why fsync is done after writing it.
	case srcAddress == 0:
		sqe, err := w.ring.NextSQE()
		if err != nil {
			return errors.WithStack(err)
		}

		sqe.OpCode = uint8(uring.FSync)
		sqe.Flags = uring.SqeIODrainFlag
		sqe.OpcodeFlags = uring.FSyncDataSync
		sqe.Fd = w.fd
		sqe.Len = 0
		sqe.Off = 0
		sqe.Addr = 0
		sqe.BufIG = 0

		w.numOfEvents++

		if _, err := w.ring.Submit(); err != nil {
			return errors.WithStack(err)
		}
		return w.awaitCompletionEvents(true)
	case (w.numOfEvents+1)&mod == 0: // +1 is always left for fsync required during commit.
		if _, err := w.ring.Submit(); err != nil {
			return errors.WithStack(err)
		}
		if (w.numOfEvents + 1) >= ringCapacity {
			return w.awaitCompletionEvents(false)
		}
	}

	return nil
}

// Close closes the writer.
func (w *Writer) Close() {
	if w.err == nil && w.numOfEvents > 0 {
		if _, err := w.ring.Submit(); err == nil {
			_ = w.awaitCompletionEvents(true)
		}
	}
	_ = w.ring.UnRegisterBuffers()
	_ = w.ring.Close()
}

func (w *Writer) awaitCompletionEvents(finalize bool) error {
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
			minNumOfEvents = w.numOfEvents
		}

		numOfEvents := uint32(w.ring.PeekCQEventBatch(w.cqeBuff))
		for numOfEvents == 0 {
			_, err := w.ring.WaitCQEvents(minNumOfEvents)
			if err != nil && !errors.Is(err, syscall.EINTR) {
				return errors.WithStack(err)
			}
			numOfEvents = uint32(w.ring.PeekCQEventBatch(w.cqeBuff))
		}
		for i := range numOfEvents {
			if w.cqeBuff[i].Res < 0 {
				w.err = w.cqeBuff[i].Error()
				return errors.WithStack(w.err)
			}
		}
		w.ring.AdvanceCQ(numOfEvents)
		w.numOfEvents -= numOfEvents

		if !finalize || w.numOfEvents == 0 {
			return nil
		}
	}
}
