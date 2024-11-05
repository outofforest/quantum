package pipeline

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
	Transaction       any
	StoreRequest      *StoreRequest
	LastStoreRequest  **StoreRequest
	SyncCh            chan<- error
	Next              *TransactionRequest
	Type              TransactionRequestType
	ChecksumProcessed bool
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

// New creates new pipeline.
func New() *Pipeline {
	head := &TransactionRequest{}
	return &Pipeline{
		tail:           &head,
		availableCount: lo.ToPtr[uint64](0),
	}
}

// Pipeline is the pipeline processing transactions.
type Pipeline struct {
	tail           **TransactionRequest
	availableCount *uint64
	count          uint64
}

// Push pushes new request into the pipeline.
func (p *Pipeline) Push(item *TransactionRequest) {
	*p.tail = item
	p.tail = &item.Next

	p.count++

	if p.count == 96 || item.SyncCh != nil || item.Type == Close {
		atomic.AddUint64(p.availableCount, p.count)
		p.count = 0
	}
}

// NewReader creates new pipeline reader.
func (p *Pipeline) NewReader() *Reader {
	return &Reader{
		head:           p.tail,
		availableCount: p.availableCount,
		processedCount: lo.ToPtr[uint64](0),
	}
}

// Reader reads requests from the pipeline.
type Reader struct {
	head           **TransactionRequest
	availableCount *uint64
	processedCount *uint64

	currentAvailableCount uint64
	currentProcessedCount uint64
}

// Count returns the number of available requests to process.
func (qr *Reader) Count() uint64 {
	const maxChunkSize = 96

	atomic.StoreUint64(qr.processedCount, qr.currentProcessedCount)
	if toProcess := qr.currentAvailableCount - qr.currentProcessedCount; toProcess > 0 {
		if toProcess > maxChunkSize {
			return maxChunkSize
		}
		return toProcess
	}

	for {
		qr.currentAvailableCount = atomic.LoadUint64(qr.availableCount)
		if toProcess := qr.currentAvailableCount - qr.currentProcessedCount; toProcess > 0 {
			if toProcess > maxChunkSize {
				return maxChunkSize
			}
			return toProcess
		}

		time.Sleep(10 * time.Microsecond)
	}
}

// Acknowledge acknowledges processing of previously read requests.
func (qr *Reader) Acknowledge() {
	atomic.StoreUint64(qr.processedCount, qr.currentProcessedCount)
}

// Read reads next request from the pipeline.
func (qr *Reader) Read() *TransactionRequest {
	h := *qr.head
	qr.head = &h.Next
	qr.currentProcessedCount++
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
