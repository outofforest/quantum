package list

import (
	"unsafe"

	"github.com/outofforest/quantum/types"
)

// NumOfAddresses defines number of available slots in the list node.
const NumOfAddresses = 510

// Node represents list node.
type Node struct {
	Slots [NumOfAddresses]types.PersistentAddress

	NumOfPointerAddresses uint16
	Next                  types.PersistentAddress
}

// ProjectNode projects node to list node.
func ProjectNode(n unsafe.Pointer) *Node {
	return (*Node)(n)
}
