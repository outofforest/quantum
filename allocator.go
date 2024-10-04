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
	config            AllocatorConfig
	data              []byte
	zeroNode          []byte
	lastAllocatedNode NodeAddress
}

// Node returns node bytes.
func (a *Allocator) Node(nodeAddress NodeAddress) []byte {
	return a.data[uint64(nodeAddress)*a.config.NodeSize : uint64(nodeAddress+1)*a.config.NodeSize]
}

// Allocate allocates node and copies data into it.
func (a *Allocator) Allocate(copyFrom []byte) (NodeAddress, []byte, error) {
	a.lastAllocatedNode++
	if a.lastAllocatedNode >= NodeAddress(len(a.data)) {
		return 0, nil, errors.New("out of space")
	}
	node := a.Node(a.lastAllocatedNode)
	copy(node, copyFrom)
	return a.lastAllocatedNode, node, nil
}

// Deallocate deallocates node.
func (a *Allocator) Deallocate(nodeAddress NodeAddress) {

}

// NewSnapshotAllocator returns snapshot-level allocator.
func NewSnapshotAllocator(
	snapshotID SnapshotID,
	allocator *Allocator,
	deallocationLists *Space[SnapshotID, NodeAddress],
	listNodeAllocator ListNodeAllocator,
) SnapshotAllocator {
	return SnapshotAllocator{
		snapshotID:        snapshotID,
		allocator:         allocator,
		deallocationLists: deallocationLists,
		listNodeAllocator: listNodeAllocator,
		dirtyNodes:        map[NodeAddress]struct{}{},
	}
}

// SnapshotAllocator allocates memory on behalf of snapshot.
type SnapshotAllocator struct {
	snapshotID        SnapshotID
	allocator         *Allocator
	deallocationLists *Space[SnapshotID, NodeAddress]
	listNodeAllocator ListNodeAllocator

	dirtyNodes map[NodeAddress]struct{}
}

// Allocate allocates new node.
func (sa SnapshotAllocator) Allocate() (NodeAddress, []byte, error) {
	nodeAddress, node, err := sa.allocator.Allocate(sa.allocator.zeroNode)
	if err != nil {
		return 0, nil, err
	}

	sa.dirtyNodes[nodeAddress] = struct{}{}
	return nodeAddress, node, nil
}

// Copy allocates new node and copies content from existing one.
func (sa SnapshotAllocator) Copy(data []byte) (NodeAddress, []byte, error) {
	nodeAddress, node, err := sa.allocator.Allocate(data)
	if err != nil {
		return 0, nil, err
	}

	sa.dirtyNodes[nodeAddress] = struct{}{}
	return nodeAddress, node, nil
}

// Deallocate marks node for deallocation.
func (sa SnapshotAllocator) Deallocate(nodeAddress NodeAddress, srcSnapshotID SnapshotID) error {
	if srcSnapshotID == sa.snapshotID {
		sa.DeallocateImmediately(nodeAddress)
		return nil
	}

	listNodeAddress, _ := sa.deallocationLists.Get(srcSnapshotID)
	list := NewList(ListConfig{
		SnapshotID:    sa.snapshotID,
		Item:          listNodeAddress,
		NodeAllocator: sa.listNodeAllocator,
		Allocator:     sa,
	})
	if err := list.Add(nodeAddress); err != nil {
		return err
	}
	if list.config.Item != listNodeAddress {
		if err := sa.deallocationLists.Set(srcSnapshotID, list.config.Item); err != nil {
			return err
		}
	}

	return nil
}

// DeallocateImmediately dealocates node immediately.
func (sa SnapshotAllocator) DeallocateImmediately(nodeAddress NodeAddress) {
	delete(sa.dirtyNodes, nodeAddress)
	sa.allocator.Deallocate(nodeAddress)
}

// NewSpaceNodeAllocator creates new space node allocator.
func NewSpaceNodeAllocator[T comparable](allocator *Allocator) (SpaceNodeAllocator[T], error) {
	headerSize := uint64(unsafe.Sizeof(SpaceNodeHeader{}))
	if headerSize >= allocator.config.NodeSize {
		return SpaceNodeAllocator[T]{}, errors.New("node size is too small")
	}

	stateOffset := headerSize + headerSize%uint64Length
	spaceLeft := allocator.config.NodeSize - stateOffset

	var t T
	itemSize := uint64(unsafe.Sizeof(t))
	itemSize += itemSize % uint64Length

	numOfItems := spaceLeft / (itemSize + 1) // 1 is for slot state
	if numOfItems == 0 {
		return SpaceNodeAllocator[T]{}, errors.New("node size is too small")
	}
	numOfItems, _ = highestPowerOfTwo(numOfItems)
	spaceLeft -= numOfItems
	spaceLeft -= numOfItems % uint64Length

	numOfItems = spaceLeft / itemSize
	numOfItems, bitsPerHop := highestPowerOfTwo(numOfItems)
	if numOfItems == 0 {
		return SpaceNodeAllocator[T]{}, errors.New("node size is too small")
	}

	return SpaceNodeAllocator[T]{
		allocator:   allocator,
		numOfItems:  int(numOfItems),
		stateOffset: stateOffset,
		itemOffset:  allocator.config.NodeSize - spaceLeft,
		indexMask:   numOfItems - 1,
		bitsPerHop:  bitsPerHop,
	}, nil
}

// SpaceNodeAllocator converts nodes from bytes to space objects.
type SpaceNodeAllocator[T comparable] struct {
	allocator *Allocator

	numOfItems  int
	stateOffset uint64
	itemOffset  uint64
	indexMask   uint64
	bitsPerHop  uint64
}

// Get returns object for node.
func (na SpaceNodeAllocator[T]) Get(nodeAddress NodeAddress) ([]byte, SpaceNode[T]) {
	node := na.allocator.Node(nodeAddress)
	return node, na.project(node)
}

// Allocate allocates new object.
func (na SpaceNodeAllocator[T]) Allocate(allocator SnapshotAllocator) (NodeAddress, SpaceNode[T], error) {
	n, node, err := allocator.Allocate()
	if err != nil {
		return 0, SpaceNode[T]{}, err
	}
	return n, na.project(node), nil
}

// Copy allocates copy of existing object.
func (na SpaceNodeAllocator[T]) Copy(allocator SnapshotAllocator, data []byte) (NodeAddress, SpaceNode[T], error) {
	n, node, err := allocator.Copy(data)
	if err != nil {
		return 0, SpaceNode[T]{}, err
	}
	return n, na.project(node), nil
}

// Index returns element index based on hash.
func (na SpaceNodeAllocator[T]) Index(hash Hash) uint64 {
	return uint64(hash) & na.indexMask
}

// Shift shifts bits in hash.
func (na SpaceNodeAllocator[T]) Shift(hash Hash) Hash {
	return hash >> na.bitsPerHop
}

func (na SpaceNodeAllocator[T]) project(node []byte) SpaceNode[T] {
	return SpaceNode[T]{
		Header: photon.FromBytes[SpaceNodeHeader](node),
		States: photon.SliceFromBytes[State](node[na.stateOffset:], na.numOfItems),
		Items:  photon.SliceFromBytes[T](node[na.itemOffset:], na.numOfItems),
	}
}

// NewListNodeAllocator creates new list node allocator.
func NewListNodeAllocator(allocator *Allocator) (ListNodeAllocator, error) {
	headerSize := uint64(unsafe.Sizeof(ListNodeHeader{}))
	if headerSize >= allocator.config.NodeSize {
		return ListNodeAllocator{}, errors.New("node size is too small")
	}

	stateOffset := headerSize + headerSize%uint64Length
	spaceLeft := allocator.config.NodeSize - stateOffset

	numOfItems := spaceLeft / uint64Length
	if numOfItems < 2 {
		return ListNodeAllocator{}, errors.New("node size is too small")
	}

	return ListNodeAllocator{
		allocator:  allocator,
		numOfItems: int(numOfItems),
		itemOffset: allocator.config.NodeSize - spaceLeft,
	}, nil
}

// ListNodeAllocator converts nodes from bytes to list objects.
type ListNodeAllocator struct {
	allocator *Allocator

	numOfItems int
	itemOffset uint64
}

// Get returns object for node.
func (na ListNodeAllocator) Get(nodeAddress NodeAddress) ([]byte, ListNode) {
	node := na.allocator.Node(nodeAddress)
	return node, na.project(node)
}

// Allocate allocates new object.
func (na ListNodeAllocator) Allocate(allocator SnapshotAllocator) (NodeAddress, ListNode, error) {
	n, node, err := allocator.Allocate()
	if err != nil {
		return 0, ListNode{}, err
	}
	return n, na.project(node), nil
}

// Copy allocates copy of existing object.
func (na ListNodeAllocator) Copy(allocator SnapshotAllocator, data []byte) (NodeAddress, ListNode, error) {
	n, node, err := allocator.Copy(data)
	if err != nil {
		return 0, ListNode{}, err
	}
	return n, na.project(node), nil
}

func (na ListNodeAllocator) project(node []byte) ListNode {
	return ListNode{
		Header: photon.FromBytes[ListNodeHeader](node),
		Items:  photon.SliceFromBytes[NodeAddress](node[na.itemOffset:], na.numOfItems),
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
