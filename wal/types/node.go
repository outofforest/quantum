package types

import (
	"unsafe"

	qtypes "github.com/outofforest/quantum/types"
)

// Node represents list node.
type Node [qtypes.NodeLength]byte

// ProjectNode projects node to list node.
func ProjectNode(n unsafe.Pointer) *Node {
	return (*Node)(n)
}
