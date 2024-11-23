package types

import (
	"unsafe"

	"github.com/outofforest/quantum/types"
)

// BlobSize defines the size of space available in node.
const BlobSize = 4088

// Node represents list node.
type Node struct {
	Blob [BlobSize]byte

	Next types.NodeAddress
}

// ProjectNode projects node to list node.
func ProjectNode(n unsafe.Pointer) *Node {
	return (*Node)(n)
}
