package types

import (
	"unsafe"

	"github.com/outofforest/quantum/types"
)

// BlobSize defines the size of space available in node.
const BlobSize = 4080

// Pointer points to the next node.
type Pointer struct {
	VolatileAddress   types.VolatileAddress
	PersistentAddress types.PersistentAddress
}

// Node represents list node.
type Node struct {
	Blob [BlobSize]byte

	Next Pointer
}

// ProjectNode projects node to list node.
func ProjectNode(n unsafe.Pointer) *Node {
	return (*Node)(n)
}
