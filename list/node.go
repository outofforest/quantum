package list

import (
	"unsafe"

	"github.com/outofforest/quantum/types"
)

// numOfAddresses defines number of available slots in the list node.
const numOfAddresses = 509

// node represents list node.
type node struct {
	Slots [numOfAddresses]types.PersistentAddress

	NumOfPointerAddresses uint16
	Next                  types.ListRoot
}

// projectNode projects node to list node.
func projectNode(n unsafe.Pointer) *node {
	return (*node)(n)
}
