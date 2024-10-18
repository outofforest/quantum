package space

import (
	"unsafe"

	"github.com/pkg/errors"

	"github.com/outofforest/photon"
	"github.com/outofforest/quantum/types"
)

// NewNodeAllocator creates new space node allocator.
func NewNodeAllocator[T comparable](allocator types.Allocator) (*NodeAllocator[T], error) {
	nodeSize := allocator.NodeSize()

	headerSize := uint64(unsafe.Sizeof(types.SpaceNodeHeader{}))
	headerSize = (headerSize + types.UInt64Length - 1) / types.UInt64Length * types.UInt64Length // memory alignment
	if headerSize >= allocator.NodeSize() {
		return nil, errors.New("node size is too small")
	}

	spaceLeft := nodeSize - headerSize

	var t T
	itemSize := uint64(unsafe.Sizeof(t))
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
		allocator:   allocator,
		itemSize:    itemSize,
		numOfItems:  int(numOfItems),
		stateOffset: headerSize,
		itemOffset:  headerSize + stateSize,
	}, nil
}

// NodeAllocator converts nodes from bytes to space objects.
type NodeAllocator[T comparable] struct {
	allocator types.Allocator

	itemSize    uint64
	numOfItems  int
	stateOffset uint64
	itemOffset  uint64
}

// Get returns object for node.
func (na *NodeAllocator[T]) Get(nodeAddress types.NodeAddress) types.SpaceNode[T] {
	return na.project(na.allocator.Node(nodeAddress))
}

// Allocate allocates new object.
func (na *NodeAllocator[T]) Allocate(allocator types.SnapshotAllocator) (types.NodeAddress, types.SpaceNode[T], error) {
	n, node, err := allocator.Allocate()
	if err != nil {
		return 0, types.SpaceNode[T]{}, err
	}
	return n, na.project(node), nil
}

// Copy allocates copy of existing object.
func (na *NodeAllocator[T]) Copy(
	allocator types.SnapshotAllocator,
	nodeAddress types.NodeAddress,
) (types.NodeAddress, types.SpaceNode[T], error) {
	n, node, err := allocator.Copy(nodeAddress)
	if err != nil {
		return 0, types.SpaceNode[T]{}, err
	}
	return n, na.project(node), nil
}

// Index returns element index based on hash.
func (na *NodeAllocator[T]) Index(hash types.Hash) uint64 {
	return uint64(hash) % uint64(na.numOfItems)
}

// Shift shifts bits in hash.
func (na *NodeAllocator[T]) Shift(hash types.Hash) types.Hash {
	return hash / types.Hash(na.numOfItems)
}

func (na *NodeAllocator[T]) project(node unsafe.Pointer) types.SpaceNode[T] {
	return types.SpaceNode[T]{
		Header: photon.FromPointer[types.SpaceNodeHeader](node),
		States: photon.SliceFromPointer[types.State](unsafe.Add(node, na.stateOffset), na.numOfItems),
		Items:  photon.SliceFromPointer[T](unsafe.Add(node, na.itemOffset), na.numOfItems),
	}
}
