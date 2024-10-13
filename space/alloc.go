package space

import (
	"unsafe"

	"github.com/pkg/errors"

	"github.com/outofforest/photon"
	"github.com/outofforest/quantum/types"
)

// NewNodeAllocator creates new space node allocator.
func NewNodeAllocator[T comparable](allocator types.Allocator) (NodeAllocator[T], error) {
	nodeSize := allocator.NodeSize()

	headerSize := uint64(unsafe.Sizeof(types.SpaceNodeHeader{}))
	if headerSize >= allocator.NodeSize() {
		return NodeAllocator[T]{}, errors.New("node size is too small")
	}

	stateOffset := headerSize + headerSize%types.UInt64Length
	spaceLeft := nodeSize - stateOffset

	var t T
	itemSize := uint64(unsafe.Sizeof(t))
	itemSize += itemSize % types.UInt64Length

	numOfItems := spaceLeft / (itemSize + 1) // 1 is for slot state
	if numOfItems == 0 {
		return NodeAllocator[T]{}, errors.New("node size is too small")
	}
	numOfItems, _ = highestPowerOfTwo(numOfItems)
	spaceLeft -= numOfItems
	spaceLeft -= numOfItems % types.UInt64Length

	numOfItems = spaceLeft / itemSize
	numOfItems, bitsPerHop := highestPowerOfTwo(numOfItems)
	if numOfItems == 0 {
		return NodeAllocator[T]{}, errors.New("node size is too small")
	}

	return NodeAllocator[T]{
		allocator:   allocator,
		numOfItems:  int(numOfItems),
		stateOffset: stateOffset,
		itemOffset:  nodeSize - spaceLeft,
		indexMask:   numOfItems - 1,
		bitsPerHop:  bitsPerHop,
	}, nil
}

// NodeAllocator converts nodes from bytes to space objects.
type NodeAllocator[T comparable] struct {
	allocator types.Allocator

	numOfItems  int
	stateOffset uint64
	itemOffset  uint64
	indexMask   uint64
	bitsPerHop  uint64
}

// Get returns object for node.
func (na NodeAllocator[T]) Get(nodeAddress types.NodeAddress) types.SpaceNode[T] {
	return na.project(na.allocator.Node(nodeAddress))
}

// Allocate allocates new object.
func (na NodeAllocator[T]) Allocate(allocator types.SnapshotAllocator) (types.NodeAddress, types.SpaceNode[T], error) {
	n, node, err := allocator.Allocate()
	if err != nil {
		return 0, types.SpaceNode[T]{}, err
	}
	return n, na.project(node), nil
}

// Copy allocates copy of existing object.
func (na NodeAllocator[T]) Copy(
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
func (na NodeAllocator[T]) Index(hash types.Hash) uint64 {
	return uint64(hash) & na.indexMask
}

// Shift shifts bits in hash.
func (na NodeAllocator[T]) Shift(hash types.Hash) types.Hash {
	return hash >> na.bitsPerHop
}

func (na NodeAllocator[T]) project(node []byte) types.SpaceNode[T] {
	return types.SpaceNode[T]{
		Header: photon.FromBytes[types.SpaceNodeHeader](node),
		States: photon.SliceFromBytes[types.State](node[na.stateOffset:], na.numOfItems),
		Items:  photon.SliceFromBytes[T](node[na.itemOffset:], na.numOfItems),
	}
}

func highestPowerOfTwo(n uint64) (uint64, uint64) {
	var m uint64 = 1
	var p uint64
	for m <= n {
		m <<= 1 // Multiply m by 2 (left shift)
		p++
	}
	return m >> 1, p - 1 // Divide by 2 (right shift) to get the highest power of 2 <= n
}
