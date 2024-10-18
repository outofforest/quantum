package space

import (
	"unsafe"

	"github.com/pkg/errors"

	"github.com/outofforest/photon"
	"github.com/outofforest/quantum/types"
)

// NewNodeAllocator creates new space node allocator.
func NewNodeAllocator[T comparable](allocator types.Allocator) (*NodeAllocator[T], error) {
	nodeSize := uintptr(allocator.NodeSize())

	headerSize := unsafe.Sizeof(NodeHeader{})
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

	return &NodeAllocator[T]{
		allocator: allocator,
		spaceNode: &Node[T]{
			numOfItems: numOfItems,
			itemSize:   itemSize,
		},
		numOfItems:  numOfItems,
		stateOffset: headerSize,
		itemOffset:  headerSize + stateSize,
	}, nil
}

// NodeAllocator converts nodes from bytes to space objects.
type NodeAllocator[T comparable] struct {
	allocator types.Allocator

	spaceNode   *Node[T]
	numOfItems  uintptr
	stateOffset uintptr
	itemOffset  uintptr
}

// Get returns object for node.
func (na *NodeAllocator[T]) Get(nodeAddress types.NodeAddress) *Node[T] {
	return na.project(na.allocator.Node(nodeAddress))
}

// Allocate allocates new object.
func (na *NodeAllocator[T]) Allocate(allocator types.SnapshotAllocator) (types.NodeAddress, *Node[T], error) {
	n, node, err := allocator.Allocate()
	if err != nil {
		return 0, nil, err
	}
	return n, na.project(node), nil
}

// Copy allocates copy of existing object.
func (na *NodeAllocator[T]) Copy(
	allocator types.SnapshotAllocator,
	nodeAddress types.NodeAddress,
) (types.NodeAddress, *Node[T], error) {
	n, node, err := allocator.Copy(nodeAddress)
	if err != nil {
		return 0, nil, err
	}
	return n, na.project(node), nil
}

// Shift shifts bits in hash.
func (na *NodeAllocator[T]) Shift(hash types.Hash) types.Hash {
	return hash / types.Hash(na.numOfItems)
}

func (na *NodeAllocator[T]) project(node unsafe.Pointer) *Node[T] {
	na.spaceNode.Header = photon.FromPointer[NodeHeader](node)
	na.spaceNode.statesP = unsafe.Add(node, na.stateOffset)
	na.spaceNode.itemsP = unsafe.Add(node, na.itemOffset)
	return na.spaceNode
}

// NodeHeader is the header common to all space node types.
type NodeHeader struct {
	HashMod uint64
}

// Node represents data stored inside space node.
type Node[T comparable] struct {
	Header *NodeHeader

	numOfItems uintptr
	itemSize   uintptr
	statesP    unsafe.Pointer
	itemsP     unsafe.Pointer
}

// ItemByHash returns pointers to the item and its state by hash.
func (sn *Node[T]) ItemByHash(hash types.Hash) (*T, *types.State) {
	index := uintptr(hash) % sn.numOfItems
	return (*T)(unsafe.Add(sn.itemsP, sn.itemSize*index)), (*types.State)(unsafe.Add(sn.statesP, index))
}

// Iterator iterates over items.
func (sn *Node[T]) Iterator() func(func(*T, *types.State) bool) {
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
