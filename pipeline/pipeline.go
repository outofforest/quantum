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
	Commit
	Close
)

const (
	// StoreCapacity is the maximum capacity of store array in store request.
	StoreCapacity = 10

	sleepDuration    = 5 * time.Microsecond
	maxSleepDuration = 100 * time.Microsecond
	atomicDivider    = 10
)

// NewTransactionRequestFactory creates transaction request factory.
func NewTransactionRequestFactory() *TransactionRequestFactory {
	return &TransactionRequestFactory{
		massTR: mass.New[TransactionRequest](1000),
	}
}

// TransactionRequestFactory is used to create new transaction requests.
type TransactionRequestFactory struct {
	massTR *mass.Mass[TransactionRequest]
}

// New creates transaction request.
func (trf *TransactionRequestFactory) New() *TransactionRequest {
	t := trf.massTR.New()
	t.LastStoreRequest = &t.StoreRequest
	t.LastListRequest = &t.ListRequest
	return t
}

// TransactionRequest is used to request transaction execution.
type TransactionRequest struct {
	Transaction      any
	StoreRequest     *StoreRequest
	LastStoreRequest **StoreRequest
	ListRequest      *ListRequest
	LastListRequest  **ListRequest
	CommitCh         chan<- error
	Next             *TransactionRequest
	Type             TransactionRequestType
}

// AddStoreRequest adds store request to the transaction.
func (t *TransactionRequest) AddStoreRequest(sr *StoreRequest) {
	*t.LastStoreRequest = sr
	t.LastStoreRequest = &sr.Next
}

// AddListRequest adds list store request to the transaction.
func (t *TransactionRequest) AddListRequest(lr *ListRequest) {
	*t.LastListRequest = lr
	t.LastListRequest = &lr.Next
}

// StoreRequest is used to request writing nodes to the store.
type StoreRequest struct {
	NoSnapshots     bool
	PointersToStore int8
	Store           [StoreCapacity]types.ToStore
	Next            *StoreRequest
}

// ListRequest is used to request writing list nodes to the store.
type ListRequest struct {
	List         [StoreCapacity]types.ListRoot
	ListsToStore int8
	Next         *ListRequest
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
			sleepDuration:   sleepDuration,
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

	if p.count%atomicDivider == 0 || item.Type != None {
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
	sleepDuration         time.Duration
}

// Read reads next request from the pipeline.
func (qr *Reader) Read(ctx context.Context) (*TransactionRequest, error) {
	var h *TransactionRequest
	var err error

	//nolint:nestif
	if qr.currentAvailableCount > qr.currentReadCount {
		h = *qr.head
	} else {
		var slept bool
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
				if qr.sleepDuration > 0 {
					qr.sleepDuration -= time.Microsecond
				}
				break
			}

			if slept {
				if qr.sleepDuration < maxSleepDuration {
					qr.sleepDuration += time.Microsecond
				}
				time.Sleep(sleepDuration)
			} else if qr.sleepDuration > 0 {
				time.Sleep(qr.sleepDuration)
			}

			slept = true

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
	if *qr.processedCount+atomicDivider <= count || req.Type != None {
		atomic.StoreUint64(qr.processedCount, count)
	}
}

// NewReader creates new dependant reader.
func NewReader(parentReaders ...*Reader) *Reader {
	r := &Reader{
		head:            parentReaders[0].head,
		availableCounts: make([]*uint64, 0, len(parentReaders)),
		processedCount:  lo.ToPtr[uint64](0),
		sleepDuration:   sleepDuration,
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
		sleepDuration:   reader.sleepDuration,
	}
}
