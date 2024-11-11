package space

import (
	"unsafe"

	"github.com/pkg/errors"

	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/types"
)

// NewNodeAssistant creates new space node assistant.
func NewNodeAssistant[T comparable](state *alloc.State) (*NodeAssistant[T], error) {
	nodeSize := state.NodeSize()

	var t T
	itemSize := uint64(unsafe.Sizeof(t)+types.UInt64Length-1) / types.UInt64Length * types.UInt64Length

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

	numOfItems uint64
	itemSize   uint64
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
	return ns.numOfItems
}

// Index returns index from hash.
func (ns *NodeAssistant[T]) Index(hash types.KeyHash) uint64 {
	return uint64(hash) % ns.numOfItems
}

// Shift shifts bits in hash.
func (ns *NodeAssistant[T]) Shift(hash types.KeyHash) types.KeyHash {
	return hash / types.KeyHash(ns.numOfItems)
}

// Project projects node bytes to its structure.
func (ns *NodeAssistant[T]) Project(nodeAddress types.VolatileAddress, node *Node[T]) {
	node.itemsP = ns.state.Node(nodeAddress)
}

// Node represents data stored inside space node.
type Node[T comparable] struct {
	numOfItems uint64
	itemSize   uint64
	itemsP     unsafe.Pointer
}

// Item returns pointer to the item.
func (sn *Node[T]) Item(index uint64) *T {
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
