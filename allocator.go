package quantum

import (
	"unsafe"

	"github.com/pkg/errors"
)

// AllocatorConfig stores configuration of allocator.
type AllocatorConfig struct {
	TotalSize uint64
	NodeSize  uint64
}

// NewAllocator creates allocator.
func NewAllocator(config AllocatorConfig) *Allocator {
	return &Allocator{
		config: config,
		data:   make([]byte, config.TotalSize),
	}
}

// Allocator in-memory node allocations.
type Allocator struct {
	config             AllocatorConfig
	data               []byte
	nextNodeToAllocate uint64
}

func (a *Allocator) node(n uint64) []byte {
	return a.data[n*a.config.NodeSize : (n+1)*a.config.NodeSize]
}

func (a *Allocator) allocateNode() uint64 {
	// FIXME (wojciech): Copy 0x00 bytes to allocated node.

	n := a.nextNodeToAllocate
	a.nextNodeToAllocate++
	return n
}

// NewNodeAllocator creates new node allocator.
func NewNodeAllocator[T comparable](allocator *Allocator) (NodeAllocator[T], error) {
	headerSize := uint64(unsafe.Sizeof(NodeHeader{}))
	if headerSize >= allocator.config.NodeSize {
		return NodeAllocator[T]{}, errors.New("node size is too small")
	}

	stateOffset := headerSize + headerSize%uint64Length
	spaceLeft := allocator.config.NodeSize - stateOffset

	var t T
	itemSize := uint64(unsafe.Sizeof(t))
	itemSize += itemSize % uint64Length

	numOfItems := spaceLeft / (itemSize + 1) // 1 is for slot state
	if numOfItems == 0 {
		return NodeAllocator[T]{}, errors.New("node size is too small")
	}
	numOfItems, _ = highestPowerOfTwo(numOfItems)
	spaceLeft -= numOfItems
	spaceLeft -= numOfItems % uint64Length

	numOfItems = spaceLeft / itemSize
	numOfItems, bitsPerHop := highestPowerOfTwo(numOfItems)
	if numOfItems == 0 {
		return NodeAllocator[T]{}, errors.New("node size is too small")
	}

	return NodeAllocator[T]{
		allocator:   allocator,
		numOfItems:  numOfItems,
		stateOffset: stateOffset,
		itemOffset:  allocator.config.NodeSize - spaceLeft,
		indexMask:   numOfItems - 1,
		bitsPerHop:  bitsPerHop,
	}, nil
}

// NodeAllocator converts nodes from bytes to objects.
type NodeAllocator[T comparable] struct {
	allocator *Allocator

	numOfItems  uint64
	stateOffset uint64
	itemOffset  uint64
	indexMask   uint64
	bitsPerHop  uint64
}

// Get returns object for node.
func (na NodeAllocator[T]) Get(n uint64) ([]byte, Node[T]) {
	node := na.allocator.node(n)
	return node, na.project(node)
}

// Allocate allocates new object.
func (na NodeAllocator[T]) Allocate() (uint64, []byte, Node[T]) {
	n := na.allocator.allocateNode()
	node := na.allocator.node(n)
	return n, node, na.project(node)
}

// Index returns element index based on hash.
func (na NodeAllocator[T]) Index(hash uint64) uint64 {
	return hash & na.indexMask
}

// Shift shifts bits in hash.
func (na NodeAllocator[T]) Shift(hash uint64) uint64 {
	return hash >> na.bitsPerHop
}

func (na NodeAllocator[T]) project(node []byte) Node[T] {
	return Node[T]{
		Header: (*NodeHeader)(unsafe.Pointer(&node[0])),
		States: unsafe.Slice((*State)(unsafe.Pointer(&node[na.stateOffset])), na.numOfItems),
		Items:  unsafe.Slice((*T)(unsafe.Pointer(&node[na.itemOffset])), na.numOfItems),
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
