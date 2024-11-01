package queue

import (
	"runtime"
	"sync/atomic"

	"github.com/samber/lo"

	"github.com/outofforest/quantum/types"
)

// StoreRequestType defines special types of requests.
type StoreRequestType uint64

// Request type constants.
const (
	None StoreRequestType = iota
	Sync
	Close
)

// StoreCapacity is the maximum capacity of store array in store request.
const StoreCapacity = 10

// Request is used to request writing a node to the store.
type Request struct {
	ImmediateDeallocation bool
	PointersToStore       uint64
	Store                 [StoreCapacity]*types.Pointer

	RequestedRevision uint64
	Deallocate        []types.Pointer
	SyncCh            chan<- error
	Next              *Request
	Type              StoreRequestType
}

// New creates new queue.
func New() *Queue {
	head := &Request{}
	return &Queue{
		tail:           &head,
		availableCount: lo.ToPtr[uint64](0),
	}
}

// Queue is the polling queue for persistent store.
type Queue struct {
	tail           **Request
	availableCount *uint64
	count          uint64
}

// Push pushes new request into the queue.
func (q *Queue) Push(item *Request) {
	*q.tail = item
	q.tail = &item.Next

	q.count++

	if q.count == 100 || item.SyncCh != nil || item.Type == Close {
		atomic.AddUint64(q.availableCount, q.count)
		q.count = 0
	}
}

// NewReader creates new queue reader.
func (q *Queue) NewReader() *Reader {
	return &Reader{
		head:           q.tail,
		availableCount: q.availableCount,
		processedCount: lo.ToPtr[uint64](0),
	}
}

// Reader reads requests from the queue.
type Reader struct {
	head           **Request
	availableCount *uint64
	processedCount *uint64
}

// Count returns the number of available requests to process.
func (qr *Reader) Count(processedCount uint64) uint64 {
	available := atomic.LoadUint64(qr.availableCount)
	processed := atomic.AddUint64(qr.processedCount, processedCount)
	if toProcess := available - processed; toProcess > 0 {
		return toProcess
	}
	for {
		runtime.Gosched()
		available2 := atomic.LoadUint64(qr.availableCount)
		if available2 > available {
			return available2 - processed
		}
	}
}

// Acknowledge acknowledges processing of previously read requests.
func (qr *Reader) Acknowledge(processedCount uint64) {
	atomic.AddUint64(qr.processedCount, processedCount)
}

// Read reads next request from the queue.
func (qr *Reader) Read() *Request {
	h := *qr.head
	qr.head = &h.Next
	return h
}

// NewReader returns new dependant reader.
func (qr *Reader) NewReader() *Reader {
	return &Reader{
		head:           qr.head,
		availableCount: qr.processedCount,
		processedCount: lo.ToPtr[uint64](0),
	}
}
