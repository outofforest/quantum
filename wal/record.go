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

	// RecordCopy records a request to copy data from one place to another.
	RecordCopy

	// RecordImmediateDeallocation records node immediate deallocation.
	RecordImmediateDeallocation

	// RecordDelayedDeallocation records node delayed deallocation.
	RecordDelayedDeallocation
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
		sizeCounter: qtypes.NodeLength,
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
		if r.sizeCounter < qtypes.NodeLength {
			r.node[r.sizeCounter] = byte(RecordEnd)
		}
		tx.AddWALRequest(r.nodeAddress)
	}

	r.node = nil
	r.sizeCounter = qtypes.NodeLength
}

func (r *Recorder) insert(
	tx *pipeline.TransactionRequest,
	recordType RecordType,
	size uintptr,
) (unsafe.Pointer, error) {
	if err := r.ensureSize(tx, size+1); err != nil {
		return nil, err
	}
	r.node[r.sizeCounter] = byte(recordType)

	p := unsafe.Pointer(&r.node[r.sizeCounter+1])
	r.sizeCounter += size + 1

	return p, nil
}

func (r *Recorder) insertWithOffset(
	tx *pipeline.TransactionRequest,
	recordType RecordType,
	offset uintptr,
	size uintptr,
) (unsafe.Pointer, error) {
	if err := r.ensureSize(tx, size+9); err != nil {
		return nil, err
	}
	r.node[r.sizeCounter] = byte(recordType)
	*(*uintptr)(unsafe.Pointer(&r.node[r.sizeCounter+1])) = offset
	r.sizeCounter += 9

	p := unsafe.Pointer(&r.node[r.sizeCounter])
	r.sizeCounter += size

	return p, nil
}

// FIXME (wojciech): What if we need to record 4KB change? It won't fit into single WAL block.
func (r *Recorder) insertWithOffsetAndSize(
	tx *pipeline.TransactionRequest,
	recordType RecordType,
	offset, size uintptr,
) (unsafe.Pointer, error) {
	if err := r.ensureSize(tx, size+11); err != nil {
		return nil, err
	}
	r.node[r.sizeCounter] = byte(recordType)
	*(*uintptr)(unsafe.Pointer(&r.node[r.sizeCounter+1])) = offset
	*(*uint16)(unsafe.Pointer(&r.node[r.sizeCounter+9])) = uint16(size)
	r.sizeCounter += 11

	p := unsafe.Pointer(&r.node[r.sizeCounter])
	r.sizeCounter += size

	return p, nil
}

func (r *Recorder) ensureSize(tx *pipeline.TransactionRequest, size uintptr) error {
	if qtypes.NodeLength-r.sizeCounter >= size {
		return nil
	}
	if r.node != nil {
		if r.sizeCounter < qtypes.NodeLength {
			r.node[r.sizeCounter] = byte(RecordEnd)
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
func Reserve[T comparable](
	recorder *Recorder,
	tx *pipeline.TransactionRequest,
	dstNodeAddress qtypes.NodeAddress,
	dstPointer *T,
) (*T, error) {
	var t T

	offset := uintptr(dstNodeAddress*qtypes.NodeLength) + uintptr(unsafe.Pointer(dstPointer))%qtypes.NodeLength

	switch size := unsafe.Sizeof(t); size {
	case 1:
		p, err := recorder.insertWithOffset(tx, RecordSet1, offset, 1)
		if err != nil {
			return nil, err
		}

		return (*T)(p), nil
	case 8:
		p, err := recorder.insertWithOffset(tx, RecordSet8, offset, 8)
		if err != nil {
			return nil, err
		}

		return (*T)(p), nil
	case 32:
		p, err := recorder.insertWithOffset(tx, RecordSet32, offset, 32)
		if err != nil {
			return nil, err
		}

		return (*T)(p), nil
	default:
		p, err := recorder.insertWithOffsetAndSize(tx, RecordSet, offset, size)
		if err != nil {
			return nil, err
		}

		return (*T)(p), nil
	}
}

// Set1 copies one object and stores changes in WAL node.
func Set1[T comparable](
	recorder *Recorder, tx *pipeline.TransactionRequest, dstNodeAddress qtypes.NodeAddress,
	dst, src *T,
) error {
	dst2, err := Reserve(recorder, tx, dstNodeAddress, dst)
	if err != nil {
		return err
	}

	*dst = *src
	*dst2 = *src

	return nil
}

// Set2 copies two objects and stores changes in WAL node.
func Set2[T1, T2 comparable](
	recorder *Recorder, tx *pipeline.TransactionRequest, dstNodeAddress qtypes.NodeAddress,
	dst1, src1 *T1,
	dst2, src2 *T2,
) error {
	dst1B, err := Reserve(recorder, tx, dstNodeAddress, dst1)
	if err != nil {
		return err
	}
	dst2B, err := Reserve(recorder, tx, dstNodeAddress, dst2)
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
	recorder *Recorder, tx *pipeline.TransactionRequest, dstNodeAddress qtypes.NodeAddress,
	dst1, src1 *T1,
	dst2, src2 *T2,
	dst3, src3 *T3,
) error {
	dst1B, err := Reserve(recorder, tx, dstNodeAddress, dst1)
	if err != nil {
		return err
	}
	dst2B, err := Reserve(recorder, tx, dstNodeAddress, dst2)
	if err != nil {
		return err
	}
	dst3B, err := Reserve(recorder, tx, dstNodeAddress, dst3)
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
	recorder *Recorder, tx *pipeline.TransactionRequest, dstNodeAddress qtypes.NodeAddress,
	dst1, src1 *T1,
	dst2, src2 *T2,
	dst3, src3 *T3,
	dst4, src4 *T4,
) error {
	dst1B, err := Reserve(recorder, tx, dstNodeAddress, dst1)
	if err != nil {
		return err
	}
	dst2B, err := Reserve(recorder, tx, dstNodeAddress, dst2)
	if err != nil {
		return err
	}
	dst3B, err := Reserve(recorder, tx, dstNodeAddress, dst3)
	if err != nil {
		return err
	}
	dst4B, err := Reserve(recorder, tx, dstNodeAddress, dst4)
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

// Copy requests to copy data from one place to another.
func Copy[T comparable](
	recorder *Recorder, tx *pipeline.TransactionRequest, dstNodeAddress, srcNodeAddress qtypes.NodeAddress,
	dst, src *T,
) error {
	var t T

	size := uint16(unsafe.Sizeof(t))

	offset := uintptr(unsafe.Pointer(dst)) % qtypes.NodeLength
	offsetDst := uintptr(dstNodeAddress*qtypes.NodeLength) + offset
	offsetSrc := uintptr(srcNodeAddress*qtypes.NodeLength) + offset

	p, err := recorder.insert(tx, RecordCopy, 18)
	if err != nil {
		return err
	}

	*(*uintptr)(p) = offsetDst
	*(*uintptr)(unsafe.Add(p, 8)) = offsetSrc
	*(*uint16)(unsafe.Add(p, 16)) = size

	*dst = *src

	return nil
}

// Deallocate deallocate records node deallocation.
// FIXME (wojciech): nodeSnapshotID is needed only in case of delayed deallocation.
func Deallocate(
	recorder *Recorder,
	tx *pipeline.TransactionRequest,
	pointer *qtypes.Pointer,
	newPersistentAddress qtypes.NodeAddress,
	immediate bool,
) error {
	recordType := RecordDelayedDeallocation
	if immediate {
		recordType = RecordImmediateDeallocation
	}

	p, err := recorder.insert(tx, recordType, 24)
	if err != nil {
		return err
	}

	*(*qtypes.SnapshotID)(p) = pointer.SnapshotID
	*(*qtypes.NodeAddress)(unsafe.Add(p, 8)) = pointer.PersistentAddress
	*(*qtypes.NodeAddress)(unsafe.Add(p, 16)) = newPersistentAddress

	return nil
}
