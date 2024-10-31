package space

import (
	"unsafe"

	"github.com/pkg/errors"

	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/types"
)

// NewNodeAssistant creates new space node assistant.
func NewNodeAssistant[T comparable](state *alloc.State) (*NodeAssistant[T], error) {
	nodeSize := uintptr(state.NodeSize())

	var t T
	itemSize := (unsafe.Sizeof(t) + types.UInt64Length - 1) / types.UInt64Length * types.UInt64Length

	numOfItems := nodeSize / itemSize
	if numOfItems == 0 {
		return nil, errors.New("node size is too small")
	}

	return &NodeAssistant[T]{
		state:      state,
		numOfItems: numOfItems,
		itemSize:   itemSize,
	}, nil
}

// NodeAssistant converts nodes from bytes to space objects.
type NodeAssistant[T comparable] struct {
	state *alloc.State

	numOfItems uintptr
	itemSize   uintptr
}

// NewNode initializes new node.
func (ns *NodeAssistant[T]) NewNode() *Node[T] {
	return &Node[T]{
		numOfItems: ns.numOfItems,
		itemSize:   ns.itemSize,
	}
}

// NumOfItems returns number of items fitting in one node.
func (ns *NodeAssistant[T]) NumOfItems() uint64 {
	return uint64(ns.numOfItems)
}

// Index returns index from hash.
func (ns *NodeAssistant[T]) Index(hash types.Hash) uintptr {
	return uintptr(hash) % ns.numOfItems
}

// Shift shifts bits in hash.
func (ns *NodeAssistant[T]) Shift(hash types.Hash) types.Hash {
	return hash / types.Hash(ns.numOfItems)
}

// Project projects node bytes to its structure.
func (ns *NodeAssistant[T]) Project(nodeAddress types.VolatileAddress, node *Node[T]) {
	node.itemsP = ns.state.Node(nodeAddress)
}

// Node represents data stored inside space node.
type Node[T comparable] struct {
	numOfItems uintptr
	itemSize   uintptr
	itemsP     unsafe.Pointer
}

// Item returns pointer to the item.
func (sn *Node[T]) Item(index uintptr) *T {
	return (*T)(unsafe.Add(sn.itemsP, sn.itemSize*index))
}

// Iterator iterates over items.
func (sn *Node[T]) Iterator() func(func(*T) bool) {
	return func(yield func(*T) bool) {
		itemsP := sn.itemsP
		for range sn.numOfItems {
			if !yield((*T)(itemsP)) {
				return
			}
			itemsP = unsafe.Add(itemsP, sn.itemSize)
		}
	}
}
