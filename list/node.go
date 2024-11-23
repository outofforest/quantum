package list

import (
	"unsafe"

	"github.com/outofforest/quantum/types"
)

// NumOfAddresses defines number of available slots in the list node.
const NumOfAddresses = 511

// Node represents list node.
type Node struct {
	Slots [NumOfAddresses]types.NodeAddress

	NumOfPointerAddresses  uint16
	NumOfSideListAddresses uint16
}

// ProjectNode projects node to list node.
func ProjectNode(n unsafe.Pointer) *Node {
	return (*Node)(n)
}
