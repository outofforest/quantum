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
	allocator *alloc.Allocator,
) *Recorder {
	return &Recorder{
		state:       state,
		stateOrigin: uintptr(state.Origin()),
		allocator:   allocator,
		sizeCounter: types.BlobSize,
	}
}

// Recorder records changes to be stored in WAL.
type Recorder struct {
	state       *alloc.State
	stateOrigin uintptr
	allocator   *alloc.Allocator

	nodeAddress qtypes.NodeAddress
	node        *types.Node
	sizeCounter uintptr
}

// Commit commits pending node.
func (r *Recorder) Commit(tx *pipeline.TransactionRequest) {
	if r.node != nil {
		if r.sizeCounter < types.BlobSize {
			r.node.Blob[r.sizeCounter] = byte(RecordEnd)
		}
		tx.AddWALRequest(r.nodeAddress)
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
		tx.AddWALRequest(r.nodeAddress)
	}

	var err error
	r.nodeAddress, err = r.allocator.Allocate()
	if err != nil {
		return err
	}

	r.node = types.ProjectNode(r.state.Node(r.nodeAddress))
	r.sizeCounter = 0

	return nil
}

// Reserve reserves space in the WAL node and returns pointer to it.
func Reserve[T comparable](recorder *Recorder, tx *pipeline.TransactionRequest, pointer *T) (*T, error) {
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

// Set1 copies one object and stores changes in WAL node.
func Set1[T comparable](
	recorder *Recorder, tx *pipeline.TransactionRequest,
	dst, src *T,
) error {
	dst2, err := Reserve(recorder, tx, dst)
	if err != nil {
		return err
	}

	*dst = *src
	*dst2 = *src

	return nil
}

// Set2 copies two objects and stores changes in WAL node.
func Set2[T1, T2 comparable](
	recorder *Recorder, tx *pipeline.TransactionRequest,
	dst1, src1 *T1,
	dst2, src2 *T2,
) error {
	dst1B, err := Reserve(recorder, tx, dst1)
	if err != nil {
		return err
	}
	dst2B, err := Reserve(recorder, tx, dst2)
	if err != nil {
		return err
	}

	*dst1 = *src1
	*dst1B = *src1

	*dst2 = *src2
	*dst2B = *src2

	return nil
}

// Set3 copies three objects and stores changes in WAL node.
func Set3[T1, T2, T3 comparable](
	recorder *Recorder, tx *pipeline.TransactionRequest,
	dst1, src1 *T1,
	dst2, src2 *T2,
	dst3, src3 *T3,
) error {
	dst1B, err := Reserve(recorder, tx, dst1)
	if err != nil {
		return err
	}
	dst2B, err := Reserve(recorder, tx, dst2)
	if err != nil {
		return err
	}
	dst3B, err := Reserve(recorder, tx, dst3)
	if err != nil {
		return err
	}

	*dst1 = *src1
	*dst1B = *src1

	*dst2 = *src2
	*dst2B = *src2

	*dst3 = *src3
	*dst3B = *src3

	return nil
}

// Set4 copies four objects and stores changes in WAL node.
func Set4[T1, T2, T3, T4 comparable](
	recorder *Recorder, tx *pipeline.TransactionRequest,
	dst1, src1 *T1,
	dst2, src2 *T2,
	dst3, src3 *T3,
	dst4, src4 *T4,
) error {
	dst1B, err := Reserve(recorder, tx, dst1)
	if err != nil {
		return err
	}
	dst2B, err := Reserve(recorder, tx, dst2)
	if err != nil {
		return err
	}
	dst3B, err := Reserve(recorder, tx, dst3)
	if err != nil {
		return err
	}
	dst4B, err := Reserve(recorder, tx, dst4)
	if err != nil {
		return err
	}

	*dst1 = *src1
	*dst1B = *src1

	*dst2 = *src2
	*dst2B = *src2

	*dst3 = *src3
	*dst3B = *src3

	*dst4 = *src4
	*dst4B = *src4

	return nil
}
