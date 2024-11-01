package queue

import (
	"sync/atomic"
	"time"

	"github.com/samber/lo"

	"github.com/outofforest/mass"
	"github.com/outofforest/quantum/types"
)

// TransactionRequestType defines special types of transaction requests.
type TransactionRequestType uint64

// TransactionRequest type constants.
const (
	None TransactionRequestType = iota
	Sync
	Close
)

// StoreCapacity is the maximum capacity of store array in store request.
const StoreCapacity = 10

// NewTransactionRequest returns new transaction request.
func NewTransactionRequest(massTR *mass.Mass[TransactionRequest]) *TransactionRequest {
	t := massTR.New()
	t.LastStoreRequest = &t.StoreRequest
	return t
}

// TransactionRequest is used to request transaction execution.
type TransactionRequest struct {
	Transaction      any
	StoreRequest     *StoreRequest
	LastStoreRequest **StoreRequest
	SyncCh           chan<- error
	Next             *TransactionRequest
	Type             TransactionRequestType
}

// AddStoreRequest adds store request to the transaction.
func (t *TransactionRequest) AddStoreRequest(sr *StoreRequest) {
	*t.LastStoreRequest = sr
	t.LastStoreRequest = &sr.Next
}

// StoreRequest is used to request writing nodes to the store.
type StoreRequest struct {
	ImmediateDeallocation bool
	PointersToStore       uint64
	Store                 [StoreCapacity]*types.Pointer

	RequestedRevision uint64
	Deallocate        []types.Pointer
	Next              *StoreRequest
}

// New creates new queue.
func New() *Queue {
	head := &TransactionRequest{}
	return &Queue{
		tail:           &head,
		availableCount: lo.ToPtr[uint64](0),
	}
}

// Queue is the polling queue for persistent store.
type Queue struct {
	tail           **TransactionRequest
	availableCount *uint64
	count          uint64
}

// Push pushes new request into the queue.
func (q *Queue) Push(item *TransactionRequest) {
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
	head           **TransactionRequest
	availableCount *uint64
	processedCount *uint64
}

const maxProcessChunkSize = 5

// Count returns the number of available requests to process.
func (qr *Reader) Count(processedCount uint64) uint64 {
	processed := atomic.AddUint64(qr.processedCount, processedCount)
	for {
		available := atomic.LoadUint64(qr.availableCount)
		if toProcess := available - processed; toProcess > 0 {
			if toProcess > maxProcessChunkSize {
				return maxProcessChunkSize
			}
			return toProcess
		}

		time.Sleep(10 * time.Microsecond)
	}
}

// Acknowledge acknowledges processing of previously read requests.
func (qr *Reader) Acknowledge(processedCount uint64) {
	atomic.AddUint64(qr.processedCount, processedCount)
}

// Read reads next request from the queue.
func (qr *Reader) Read() *TransactionRequest {
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
