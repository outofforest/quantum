package space

import (
	"unsafe"

	"github.com/pkg/errors"

	"github.com/outofforest/quantum/types"
)

// NewPointerNodeAssistant creates new pointer node assistant.
func NewPointerNodeAssistant(nodeSize uint64) (*PointerNodeAssistant, error) {
	pointerSize := uint64(unsafe.Sizeof(types.Pointer{})+types.UInt64Length-1) /
		types.UInt64Length * types.UInt64Length

	// numOfPointers must be even because 2 hashes fit into one hash block.
	numOfPointers := nodeSize / (pointerSize + types.HashLength) / 2 * 2
	if numOfPointers == 0 {
		return nil, errors.Errorf("pointer size %d is greater than node size %d",
			(pointerSize + types.HashLength), nodeSize)
	}

	return &PointerNodeAssistant{
		pointerSize:   pointerSize,
		numOfPointers: numOfPointers,
		pointerOffset: numOfPointers * types.HashLength,
	}, nil
}

// PointerNodeAssistant converts nodes from bytes to pointer objects.
type PointerNodeAssistant struct {
	pointerSize   uint64
	numOfPointers uint64
	pointerOffset uint64
}

// NumOfPointers returns number of pointers fitting in one node.
func (na *PointerNodeAssistant) NumOfPointers() uint64 {
	return na.numOfPointers
}

// Index returns index from hash.
func (na *PointerNodeAssistant) Index(hash types.KeyHash) uint64 {
	return uint64(hash) % na.numOfPointers
}

// Shift shifts bits in hash.
func (na *PointerNodeAssistant) Shift(hash types.KeyHash) types.KeyHash {
	return hash / types.KeyHash(na.numOfPointers)
}

// PointerOffset returns pointer's offset relative to the beginning of the node.
func (na *PointerNodeAssistant) PointerOffset(index uint64) uint64 {
	return na.pointerOffset + na.pointerSize*index
}

// Pointer maps the memory address given by the node address and offset to a pointer.
func (na *PointerNodeAssistant) Pointer(n unsafe.Pointer, offset uint64) *types.Pointer {
	return (*types.Pointer)(unsafe.Add(n, offset))
}

// Iterator iterates over pointers.
func (na *PointerNodeAssistant) Iterator(n unsafe.Pointer) func(func(*types.Pointer) bool) {
	return func(yield func(*types.Pointer) bool) {
		n = unsafe.Add(n, na.pointerOffset)
		for range na.numOfPointers {
			if !yield((*types.Pointer)(n)) {
				return
			}
			n = unsafe.Add(n, na.pointerSize)
		}
	}
}
