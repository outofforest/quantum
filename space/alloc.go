package space

import (
	"unsafe"

	"github.com/pkg/errors"

	"github.com/outofforest/photon"
	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/types"
)

// NewNodeAllocator creates new space node allocator.
func NewNodeAllocator[H, T comparable](state *alloc.State) (*NodeAllocator[H, T], error) {
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

	return &NodeAllocator[H, T]{
		state:       state,
		numOfItems:  numOfItems,
		itemSize:    itemSize,
		stateOffset: headerSize,
		itemOffset:  headerSize + stateSize,
	}, nil
}

// NodeAllocator converts nodes from bytes to space objects.
type NodeAllocator[H, T comparable] struct {
	state *alloc.State

	numOfItems  uintptr
	itemSize    uintptr
	stateOffset uintptr
	itemOffset  uintptr
}

// NewNode initializes new node.
func (na *NodeAllocator[H, T]) NewNode() *Node[H, T] {
	return &Node[H, T]{
		numOfItems: na.numOfItems,
		itemSize:   na.itemSize,
	}
}

// FIXME (wojciech): Get and allocate might be called directly on State. Here I just need Project.

// Get returns object for node.
func (na *NodeAllocator[H, T]) Get(nodeAddress types.LogicalAddress, node *Node[H, T]) {
	na.project(na.state.Node(nodeAddress), node)
}

// Allocate allocates new object.
func (na *NodeAllocator[H, T]) Allocate(
	pool *alloc.Pool[types.LogicalAddress],
	node *Node[H, T],
) (types.LogicalAddress, error) {
	nodeAddress, err := pool.Allocate()
	if err != nil {
		return 0, err
	}

	na.project(na.state.Node(nodeAddress), node)
	return nodeAddress, nil
}

// Index returns index from hash.
func (na *NodeAllocator[H, T]) Index(hash types.Hash) uintptr {
	return uintptr(hash) % na.numOfItems
}

// Shift shifts bits in hash.
func (na *NodeAllocator[H, T]) Shift(hash types.Hash) types.Hash {
	return hash / types.Hash(na.numOfItems)
}

func (na *NodeAllocator[H, T]) project(nodeP unsafe.Pointer, node *Node[H, T]) {
	node.Header = photon.FromPointer[H](nodeP)
	node.statesP = unsafe.Add(nodeP, na.stateOffset)
	node.itemsP = unsafe.Add(nodeP, na.itemOffset)
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
