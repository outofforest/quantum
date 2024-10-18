package space

import (
	"unsafe"

	"github.com/pkg/errors"

	"github.com/outofforest/photon"
	"github.com/outofforest/quantum/types"
)

// NewNodeAllocator creates new space node allocator.
func NewNodeAllocator[H, T comparable](allocator types.Allocator) (*NodeAllocator[H, T], error) {
	nodeSize := uintptr(allocator.NodeSize())

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

	return &NodeAllocator[H, T]{
		allocator: allocator,
		spaceNode: &Node[H, T]{
			numOfItems: numOfItems,
			itemSize:   itemSize,
		},
		numOfItems:  numOfItems,
		stateOffset: headerSize,
		itemOffset:  headerSize + stateSize,
	}, nil
}

// NodeAllocator converts nodes from bytes to space objects.
type NodeAllocator[H, T comparable] struct {
	allocator types.Allocator

	spaceNode   *Node[H, T]
	numOfItems  uintptr
	stateOffset uintptr
	itemOffset  uintptr
}

// Get returns object for node.
func (na *NodeAllocator[H, T]) Get(nodeAddress types.LogicalAddress) *Node[H, T] {
	return na.project(na.allocator.Node(nodeAddress))
}

// Allocate allocates new object.
func (na *NodeAllocator[H, T]) Allocate(allocator types.SnapshotAllocator) (types.LogicalAddress, *Node[H, T], error) {
	n, node, err := allocator.Allocate()
	if err != nil {
		return 0, nil, err
	}
	return n, na.project(node), nil
}

// Shift shifts bits in hash.
func (na *NodeAllocator[H, T]) Shift(hash types.Hash) types.Hash {
	return hash / types.Hash(na.numOfItems)
}

func (na *NodeAllocator[H, T]) project(node unsafe.Pointer) *Node[H, T] {
	na.spaceNode.Header = photon.FromPointer[H](node)
	na.spaceNode.statesP = unsafe.Add(node, na.stateOffset)
	na.spaceNode.itemsP = unsafe.Add(node, na.itemOffset)
	return na.spaceNode
}

// PointerNodeHeader is the header of pointer node.
type PointerNodeHeader struct {
	RevisionHeader    types.RevisionHeader
	ParentNodeAddress types.LogicalAddress
	HashMod           uint64
}

// DataNodeHeader is the header of data node.
type DataNodeHeader struct {
	RevisionHeader types.RevisionHeader
}

// Node represents data stored inside space node.
type Node[H, T comparable] struct {
	Header *H

	numOfItems uintptr
	itemSize   uintptr
	statesP    unsafe.Pointer
	itemsP     unsafe.Pointer
}

// ItemByHash returns pointers to the item and its state by hash.
func (sn *Node[H, T]) ItemByHash(hash types.Hash) (*T, *types.State) {
	index := uintptr(hash) % sn.numOfItems
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
