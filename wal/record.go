package wal

import (
	"unsafe"

	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/pipeline"
	qtypes "github.com/outofforest/quantum/types"
	"github.com/outofforest/quantum/wal/types"
)

// RecordType is used to specify type of the record.
type RecordType uint8

const (
	// RecordEnd means that there are no more records in the node.
	RecordEnd RecordType = iota

	// RecordSet1 records modification of 1-byte value.
	RecordSet1

	// RecordSet8 records modification of 8-byte value.
	RecordSet8

	// RecordSet32 records modification of 32-byte value.
	RecordSet32

	// RecordSet records modification of variable-length slice.
	RecordSet
)

// NewRecorder creates new WAL recorder.
func NewRecorder(
	state *alloc.State,
	volatilePool *alloc.Pool[qtypes.VolatileAddress],
) *Recorder {
	return &Recorder{
		state:        state,
		stateOrigin:  uintptr(state.Origin()),
		volatilePool: volatilePool,
		sizeCounter:  types.BlobSize,
	}
}

// Recorder records changes to be stored in WAL.
type Recorder struct {
	state        *alloc.State
	stateOrigin  uintptr
	volatilePool *alloc.Pool[qtypes.VolatileAddress]

	volatileAddress qtypes.VolatileAddress
	node            *types.Node
	sizeCounter     uintptr
}

// Commit commits pending node.
func (r *Recorder) Commit(tx *pipeline.TransactionRequest) {
	if r.node != nil {
		if r.sizeCounter < types.BlobSize {
			r.node.Blob[r.sizeCounter] = byte(RecordEnd)
		}
		tx.AddWALRequest(r.volatileAddress)
	}

	r.node = nil
	r.sizeCounter = types.BlobSize
}

func (r *Recorder) insertWithOffset(
	tx *pipeline.TransactionRequest,
	recordType RecordType,
	offset uintptr,
	size uintptr,
) (unsafe.Pointer, error) {
	if err := r.ensureSize(tx, 9); err != nil {
		return nil, err
	}
	r.node.Blob[r.sizeCounter] = byte(recordType)
	*(*uintptr)(unsafe.Pointer(&r.node.Blob[r.sizeCounter+1])) = offset
	r.sizeCounter += 9

	if err := r.ensureSize(tx, size); err != nil {
		return nil, err
	}
	p := unsafe.Pointer(&r.node.Blob[r.sizeCounter])
	r.sizeCounter += size

	return p, nil
}

func (r *Recorder) insertWithOffsetAndSize(
	tx *pipeline.TransactionRequest,
	recordType RecordType,
	offset uintptr,
	size uintptr,
) (unsafe.Pointer, error) {
	if err := r.ensureSize(tx, 11); err != nil {
		return nil, err
	}
	r.node.Blob[r.sizeCounter] = byte(recordType)
	*(*uintptr)(unsafe.Pointer(&r.node.Blob[r.sizeCounter+1])) = offset
	*(*uint16)(unsafe.Pointer(&r.node.Blob[r.sizeCounter+3])) = uint16(size)
	r.sizeCounter += 11

	if err := r.ensureSize(tx, size); err != nil {
		return nil, err
	}
	p := unsafe.Pointer(&r.node.Blob[r.sizeCounter])
	r.sizeCounter += size

	return p, nil
}

func (r *Recorder) ensureSize(tx *pipeline.TransactionRequest, size uintptr) error {
	if types.BlobSize-r.sizeCounter >= size {
		return nil
	}
	if r.node != nil {
		if r.sizeCounter < types.BlobSize {
			r.node.Blob[r.sizeCounter] = byte(RecordEnd)
		}
		tx.AddWALRequest(r.volatileAddress)
	}

	var err error
	r.volatileAddress, err = r.volatilePool.Allocate()
	if err != nil {
		return err
	}

	r.node = types.ProjectNode(r.state.Node(r.volatileAddress))
	r.sizeCounter = 0

	return nil
}

// Record reserves space in the WAL node and returns pointer to it.
func Record[T comparable](recorder *Recorder, tx *pipeline.TransactionRequest, pointer *T) (*T, error) {
	var t T

	switch size := unsafe.Sizeof(t); size {
	case 1:
		p, err := recorder.insertWithOffset(tx, RecordSet1, uintptr(unsafe.Pointer(pointer))-recorder.stateOrigin, 1)
		if err != nil {
			return nil, err
		}

		return (*T)(p), nil
	case 8:
		p, err := recorder.insertWithOffset(tx, RecordSet8, uintptr(unsafe.Pointer(pointer))-recorder.stateOrigin, 8)
		if err != nil {
			return nil, err
		}

		return (*T)(p), nil
	case 32:
		p, err := recorder.insertWithOffset(tx, RecordSet32, uintptr(unsafe.Pointer(pointer))-recorder.stateOrigin, 32)
		if err != nil {
			return nil, err
		}

		return (*T)(p), nil
	default:
		p, err := recorder.insertWithOffsetAndSize(tx, RecordSet, uintptr(unsafe.Pointer(pointer))-recorder.stateOrigin,
			size)
		if err != nil {
			return nil, err
		}

		return (*T)(p), nil
	}
}
