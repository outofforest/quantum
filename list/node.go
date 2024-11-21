package list

import (
	"unsafe"
)

// NumOfSlots defines number of available slots in the list node.
const NumOfSlots = 511

// Node represents list node.
type Node struct {
	Slots [NumOfSlots]uint64

	NumOfPointerSlots  uint16
	NumOfSideListSlots uint16
}

// ProjectNode projects node to list node.
func ProjectNode(n unsafe.Pointer) *Node {
	return (*Node)(n)
}
