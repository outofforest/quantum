package quantum

import (
	"unsafe"

	"github.com/pkg/errors"

	"github.com/outofforest/photon"
)

// AllocatorConfig stores configuration of allocator.
type AllocatorConfig struct {
	TotalSize uint64
	NodeSize  uint64
}

// NewAllocator creates allocator.
func NewAllocator(config AllocatorConfig) *Allocator {
	return &Allocator{
		config:   config,
		data:     make([]byte, config.TotalSize),
		zeroNode: make([]byte, config.NodeSize),
	}
}

// Allocator in-memory node allocations.
type Allocator struct {
	config             AllocatorConfig
	data               []byte
	zeroNode           []byte
	nextNodeToAllocate uint64
}

// Node returns node bytes.
func (a *Allocator) Node(n uint64) []byte {
	return a.data[n*a.config.NodeSize : (n+1)*a.config.NodeSize]
}

// Allocate allocates new node.
func (a *Allocator) Allocate() (uint64, []byte) {
	return a.allocate(a.zeroNode)
}

// Copy allocates new node and copies content from existing one.
func (a *Allocator) Copy(data []byte) (uint64, []byte) {
	return a.allocate(data)
}

func (a *Allocator) allocate(copyFrom []byte) (uint64, []byte) {
	n := a.nextNodeToAllocate
	a.nextNodeToAllocate++
	node := a.Node(n)
	copy(node, copyFrom)
	return n, node
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
		numOfItems:  int(numOfItems),
		stateOffset: stateOffset,
		itemOffset:  allocator.config.NodeSize - spaceLeft,
		indexMask:   numOfItems - 1,
		bitsPerHop:  bitsPerHop,
	}, nil
}

// NodeAllocator converts nodes from bytes to objects.
type NodeAllocator[T comparable] struct {
	allocator *Allocator

	numOfItems  int
	stateOffset uint64
	itemOffset  uint64
	indexMask   uint64
	bitsPerHop  uint64
}

// Get returns object for node.
func (na NodeAllocator[T]) Get(n uint64) ([]byte, Node[T]) {
	node := na.allocator.Node(n)
	return node, na.project(node)
}

// Allocate allocates new object.
func (na NodeAllocator[T]) Allocate() (uint64, Node[T]) {
	n, node := na.allocator.Allocate()
	return n, na.project(node)
}

// Copy allocates copy of existing object.
func (na NodeAllocator[T]) Copy(data []byte) (uint64, Node[T]) {
	n, node := na.allocator.Copy(data)
	return n, na.project(node)
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
		Header: photon.FromBytes[NodeHeader](node),
		States: photon.SliceFromBytes[State](node[na.stateOffset:], na.numOfItems),
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
