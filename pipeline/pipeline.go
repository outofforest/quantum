package pipeline

import (
	"context"
	"math"
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
	revision uint32
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

	Transaction      any
	StoreRequest     *StoreRequest
	LastStoreRequest **StoreRequest
	SyncCh           chan<- struct{}
	CommitCh         chan<- error
	Next             *TransactionRequest
	Type             TransactionRequestType
}

// AddStoreRequest adds store request to the transaction.
func (t *TransactionRequest) AddStoreRequest(sr *StoreRequest) {
	sr.RequestedRevision = t.trf.revision
	for i := range sr.PointersToStore {
		atomic.StoreUint32(&sr.Store[i].Pointer.Revision, sr.RequestedRevision)
	}
	t.trf.revision++

	*t.LastStoreRequest = sr
	t.LastStoreRequest = &sr.Next
}

// StoreRequest is used to request writing nodes to the store.
type StoreRequest struct {
	ImmediateDeallocation bool
	PointersToStore       int8
	Store                 [StoreCapacity]types.NodeRoot

	RequestedRevision uint32
	Next              *StoreRequest
}

// New creates new pipeline.
func New() (*Pipeline, *Reader) {
	head := &TransactionRequest{}
	availableCount := lo.ToPtr[uint64](0)
	return &Pipeline{
			tail:           &head,
			availableCount: availableCount,
		}, &Reader{
			head:            &head,
			availableCounts: []*uint64{availableCount},
			processedCount:  lo.ToPtr[uint64](0),
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

	if p.count%96 == 0 || item.Type != None {
		atomic.StoreUint64(p.availableCount, p.count)
	}
}

// Reader reads requests from the pipeline.
type Reader struct {
	head            **TransactionRequest
	availableCounts []*uint64
	processedCount  *uint64

	currentAvailableCount uint64
	currentReadCount      uint64
}

// Read reads next request from the pipeline.
func (qr *Reader) Read(ctx context.Context) (*TransactionRequest, error) {
	var h *TransactionRequest
	var err error
	if qr.currentAvailableCount > qr.currentReadCount {
		h = *qr.head
	} else {
		// It is done once per epoch to save time on calling ctx.Err().
		err = errors.WithStack(ctx.Err())
		for {
			var minAvailableCount uint64 = math.MaxUint64
			for _, ac := range qr.availableCounts {
				availableCount := atomic.LoadUint64(ac)
				if availableCount < minAvailableCount {
					minAvailableCount = availableCount
				}
			}
			qr.currentAvailableCount = minAvailableCount
			if qr.currentAvailableCount > qr.currentReadCount {
				h = *qr.head
				break
			}

			time.Sleep(10 * time.Microsecond)

			if ctx.Err() != nil {
				return nil, errors.WithStack(ctx.Err())
			}
		}
	}

	qr.head = &h.Next
	qr.currentReadCount++
	return h, err
}

// Acknowledge acknowledges processing of requests so next worker in the pipeline might take them.
func (qr *Reader) Acknowledge(count uint64, req *TransactionRequest) {
	if *qr.processedCount+96 <= count || req.Type != None {
		atomic.StoreUint64(qr.processedCount, count)
	}
}

// NewReader creates new dependant reader.
func NewReader(parentReaders ...*Reader) *Reader {
	r := &Reader{
		head:            parentReaders[0].head,
		availableCounts: make([]*uint64, 0, len(parentReaders)),
		processedCount:  lo.ToPtr[uint64](0),
	}
	for _, pr := range parentReaders {
		r.availableCounts = append(r.availableCounts, pr.processedCount)
	}

	return r
}

// CloneReader creates new reader starting from the same head.
func CloneReader(reader *Reader) *Reader {
	return &Reader{
		head:            reader.head,
		availableCounts: reader.availableCounts,
		processedCount:  lo.ToPtr[uint64](0),
	}
}
