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

// Set1 records modification of 1-byte value.
func (r *Recorder) Set1(tx *pipeline.TransactionRequest, pointer unsafe.Pointer) (unsafe.Pointer, error) {
	p, err := r.insertWithOffset(tx, RecordSet8, uintptr(pointer)-r.stateOrigin, 1)
	if err != nil {
		return nil, err
	}

	return p, nil
}

// Set8 records modification of 8-byte value.
func (r *Recorder) Set8(tx *pipeline.TransactionRequest, pointer unsafe.Pointer) (unsafe.Pointer, error) {
	p, err := r.insertWithOffset(tx, RecordSet8, uintptr(pointer)-r.stateOrigin, 8)
	if err != nil {
		return nil, err
	}

	return p, nil
}

// Set32 records modification of 32-byte value.
func (r *Recorder) Set32(tx *pipeline.TransactionRequest, pointer unsafe.Pointer) (unsafe.Pointer, error) {
	p, err := r.insertWithOffset(tx, RecordSet32, uintptr(pointer)-r.stateOrigin, 32)
	if err != nil {
		return nil, err
	}

	return p, nil
}

// Set records modification of variable-length value.
func (r *Recorder) Set(tx *pipeline.TransactionRequest, pointer unsafe.Pointer, size uintptr) (unsafe.Pointer, error) {
	p, err := r.insertWithOffsetAndSize(tx, RecordSet, uintptr(pointer)-r.stateOrigin, size)
	if err != nil {
		return nil, err
	}

	return p, nil
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
