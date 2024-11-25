package space

import (
	"math/bits"
	"unsafe"

	"github.com/outofforest/quantum/types"
)

const (
	// NumOfPointers specifies the number of pointers in single pointer node.
	NumOfPointers = 64

	// NumOfBlocksForPointerNode defines how many blocks must be hashed for pointer node.
	NumOfBlocksForPointerNode = NumOfPointers * types.HashLength / types.BlockLength

	indexMask = NumOfPointers - 1
)

var (
	rightShiftBits = -1 * bits.TrailingZeros64(NumOfPointers)
)

// PointerNode represents pointer node.
type PointerNode struct {
	// Hashes must go first because this is the hashed portion of the pointer node
	// because this is the hashed region.
	Hashes   [NumOfPointers]types.Hash
	Pointers [NumOfPointers]types.Pointer
}

// ProjectPointerNode converts memory pointer to pointer node.
func ProjectPointerNode(n unsafe.Pointer) *PointerNode {
	return (*PointerNode)(n)
}

// PointerIndex returns index from hash.
func PointerIndex(hash types.KeyHash, level uint8) uint64 {
	return bits.RotateLeft64(uint64(hash), int(level)*rightShiftBits) & indexMask
}
