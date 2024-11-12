package space

import (
	"unsafe"

	"github.com/outofforest/quantum/types"
)

// NumOfPointers specifies the number of pointers in single pointer node.
const NumOfPointers = 56

// PointerNode represents pointer node.
type PointerNode struct {
	Hashes   [NumOfPointers]types.Hash
	Pointers [NumOfPointers]types.Pointer
}

// ProjectPointerNode converts memory pointer to pointer node.
func ProjectPointerNode(n unsafe.Pointer) *PointerNode {
	return (*PointerNode)(n)
}

// PointerIndex returns index from hash.
func PointerIndex(hash types.KeyHash) uint64 {
	return uint64(hash) % NumOfPointers
}

// PointerShift shifts bits in hash.
func PointerShift(hash types.KeyHash) types.KeyHash {
	return hash / NumOfPointers
}
