package pipeline

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
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
	Commit
	Close
)

// StoreCapacity is the maximum capacity of store array in store request.
const StoreCapacity = 10

// NewTransactionRequestFactory creates transaction request factory.
func NewTransactionRequestFactory() *TransactionRequestFactory {
	return &TransactionRequestFactory{
		massTR: mass.New[TransactionRequest](1000),
	}
}

// TransactionRequestFactory is used to create new transaction requests.
type TransactionRequestFactory struct {
	revision uint64
	massTR   *mass.Mass[TransactionRequest]
}

// New creates transaction request.
func (trf *TransactionRequestFactory) New() *TransactionRequest {
	t := trf.massTR.New()
	t.trf = trf
	t.LastStoreRequest = &t.StoreRequest
	return t
}

// TransactionRequest is used to request transaction execution.
type TransactionRequest struct {
	trf *TransactionRequestFactory

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
	sr.RequestedRevision = t.trf.revision
	for i := range sr.PointersToStore {
		atomic.StoreUint64(&sr.Store[i].Revision, sr.RequestedRevision)
	}
	t.trf.revision++

	*t.LastStoreRequest = sr
	t.LastStoreRequest = &sr.Next
}

// StoreRequest is used to request writing nodes to the store.
type StoreRequest struct {
	ImmediateDeallocation bool
	PointersToStore       uint64
	Store                 [StoreCapacity]*types.Pointer

	RequestedRevision         uint64
	Deallocate                []types.Pointer
	DeallocateVolatileAddress types.VolatileAddress
	Next                      *StoreRequest
}

// New creates new pipeline.
func New() (*Pipeline, *Reader) {
	head := &TransactionRequest{}
	availableCount := lo.ToPtr[uint64](0)
	return &Pipeline{
			tail:           &head,
			availableCount: availableCount,
		}, &Reader{
			head:           &head,
			availableCount: availableCount,
			processedCount: lo.ToPtr[uint64](0),
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
	p.count++

	*p.tail = item
	p.tail = &item.Next

	if p.count%96 == 0 || item.SyncCh != nil || item.Type == Close {
		atomic.StoreUint64(p.availableCount, p.count)
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
func (qr *Reader) Count(ctx context.Context) (uint64, error) {
	const maxChunkSize = 96

	atomic.StoreUint64(qr.processedCount, qr.currentProcessedCount)
	if toProcess := qr.currentAvailableCount - qr.currentProcessedCount; toProcess > 0 {
		if toProcess > maxChunkSize {
			return maxChunkSize, errors.WithStack(ctx.Err())
		}
		return toProcess, errors.WithStack(ctx.Err())
	}

	for {
		qr.currentAvailableCount = atomic.LoadUint64(qr.availableCount)
		if toProcess := qr.currentAvailableCount - qr.currentProcessedCount; toProcess > 0 {
			if toProcess > maxChunkSize {
				return maxChunkSize, errors.WithStack(ctx.Err())
			}
			return toProcess, errors.WithStack(ctx.Err())
		}

		if ctx.Err() != nil {
			return 0, errors.WithStack(ctx.Err())
		}

		time.Sleep(10 * time.Microsecond)
	}
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
