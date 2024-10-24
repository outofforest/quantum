package space

import (
	"unsafe"

	"github.com/pkg/errors"

	"github.com/outofforest/photon"
	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/types"
)

// NewNodeAssistant creates new space node assistant.
func NewNodeAssistant[H, T comparable](state *alloc.State) (*NodeAssistant[H, T], error) {
	nodeSize := uintptr(state.NodeSize())

	var h H
	headerSize := unsafe.Sizeof(h)
	headerSize = (headerSize + types.UInt64Length - 1) / types.UInt64Length * types.UInt64Length // memory alignment
	if headerSize >= nodeSize {
		return nil, errors.New("node size is too small")
	}

	spaceLeft := nodeSize - headerSize

	var t T
	itemSize := unsafe.Sizeof(t)
	itemSize = (itemSize + types.UInt64Length - 1) / types.UInt64Length * types.UInt64Length

	numOfItems := spaceLeft / (itemSize + 1) // 1 is for slot state
	if numOfItems == 0 {
		return nil, errors.New("node size is too small")
	}
	stateSize := (numOfItems + types.UInt64Length - 1) / types.UInt64Length * types.UInt64Length
	spaceLeft -= stateSize

	numOfItems = spaceLeft / itemSize
	if numOfItems == 0 {
		return nil, errors.New("node size is too small")
	}

	return &NodeAssistant[H, T]{
		state:       state,
		numOfItems:  numOfItems,
		itemSize:    itemSize,
		stateOffset: headerSize,
		itemOffset:  headerSize + stateSize,
	}, nil
}

// NodeAssistant converts nodes from bytes to space objects.
type NodeAssistant[H, T comparable] struct {
	state *alloc.State

	numOfItems  uintptr
	itemSize    uintptr
	stateOffset uintptr
	itemOffset  uintptr
}

// NewNode initializes new node.
func (ns *NodeAssistant[H, T]) NewNode() *Node[H, T] {
	return &Node[H, T]{
		numOfItems: ns.numOfItems,
		itemSize:   ns.itemSize,
	}
}

// Index returns index from hash.
func (ns *NodeAssistant[H, T]) Index(hash types.Hash) uintptr {
	return uintptr(hash) % ns.numOfItems
}

// Shift shifts bits in hash.
func (ns *NodeAssistant[H, T]) Shift(hash types.Hash) types.Hash {
	return hash / types.Hash(ns.numOfItems)
}

// Project projects node bytes to its structure.
func (ns *NodeAssistant[H, T]) Project(nodeAddress types.LogicalAddress, node *Node[H, T]) {
	nodeP := ns.state.Node(nodeAddress)
	node.Header = photon.FromPointer[H](nodeP)
	node.statesP = unsafe.Add(nodeP, ns.stateOffset)
	node.itemsP = unsafe.Add(nodeP, ns.itemOffset)
}

// PointerNodeHeader is the header of pointer node.
type PointerNodeHeader struct {
	RevisionHeader    types.RevisionHeader
	SnapshotID        types.SnapshotID
	ParentNodeAddress types.LogicalAddress
	ParentNodeIndex   uintptr
	HashMod           uint64
}

// DataNodeHeader is the header of data node.
type DataNodeHeader struct {
	RevisionHeader types.RevisionHeader
	SnapshotID     types.SnapshotID
}

// Node represents data stored inside space node.
type Node[H, T comparable] struct {
	Header *H

	numOfItems uintptr
	itemSize   uintptr
	statesP    unsafe.Pointer
	itemsP     unsafe.Pointer
}

// Item returns pointers to the item and its state by index.
func (sn *Node[H, T]) Item(index uintptr) (*T, *types.State) {
	return (*T)(unsafe.Add(sn.itemsP, sn.itemSize*index)), (*types.State)(unsafe.Add(sn.statesP, index))
}

// Iterator iterates over items.
func (sn *Node[H, T]) Iterator() func(func(*T, *types.State) bool) {
	return func(yield func(*T, *types.State) bool) {
		itemsP := sn.itemsP
		statesP := sn.statesP
		for range sn.numOfItems {
			if !yield((*T)(itemsP), (*types.State)(statesP)) {
				return
			}
			itemsP = unsafe.Add(itemsP, sn.itemSize)
			statesP = unsafe.Add(statesP, 1)
		}
	}
}
