package space

import (
	"fmt"
	"unsafe"

	"github.com/pkg/errors"

	"github.com/outofforest/photon"
	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/types"
)

// NewPointerNodeAllocator creates new pointer node allocator.
func NewPointerNodeAllocator(state *alloc.State) (*PointerNodeAllocator, error) {
	nodeSize := uintptr(state.NodeSize())

	headerSize := unsafe.Sizeof(PointerNodeHeader{})
	headerSize = (headerSize + types.UInt64Length - 1) / types.UInt64Length * types.UInt64Length // memory alignment
	if headerSize >= nodeSize {
		return nil, errors.New("node size is too small")
	}

	spaceLeft := nodeSize - headerSize

	itemSize := unsafe.Sizeof(types.SpacePointer{})
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

	fmt.Println(numOfItems)

	return &PointerNodeAllocator{
		state:       state,
		numOfItems:  numOfItems,
		itemSize:    itemSize,
		stateOffset: headerSize,
		itemOffset:  headerSize + stateSize,
	}, nil
}

// PointerNodeAllocator converts nodes from bytes to pointer nodes.
type PointerNodeAllocator struct {
	state *alloc.State

	numOfItems  uintptr
	itemSize    uintptr
	stateOffset uintptr
	itemOffset  uintptr
}

// NewNode initializes new node.
func (na *PointerNodeAllocator) NewNode() *PointerNode {
	return &PointerNode{
		numOfItems: na.numOfItems,
		itemSize:   na.itemSize,
	}
}

// Get returns object for node.
func (na *PointerNodeAllocator) Get(nodeAddress types.LogicalAddress, node *PointerNode) {
	na.project(na.state.Node(nodeAddress), node)
}

// Allocate allocates new object.
func (na *PointerNodeAllocator) Allocate(
	pool *alloc.Pool[types.LogicalAddress],
	node *PointerNode,
) (types.LogicalAddress, error) {
	nodeAddress, err := pool.Allocate()
	if err != nil {
		return 0, err
	}

	na.project(na.state.Node(nodeAddress), node)
	return nodeAddress, nil
}

// Shift shifts bits in hash.
func (na *PointerNodeAllocator) Shift(hash types.Hash) types.Hash {
	return hash / types.Hash(na.numOfItems)
}

func (na *PointerNodeAllocator) project(nodeP unsafe.Pointer, node *PointerNode) {
	node.Header = photon.FromPointer[PointerNodeHeader](nodeP)
	node.statesP = unsafe.Add(nodeP, na.stateOffset)
	node.itemsP = unsafe.Add(nodeP, na.itemOffset)
}

// =======================

// NewDataNodeAllocator creates new data node allocator.
func NewDataNodeAllocator[K, V comparable](state *alloc.State) (*DataNodeAllocator[K, V], error) {
	nodeSize := uintptr(state.NodeSize())

	headerSize := unsafe.Sizeof(DataNodeHeader{})
	headerSize = (headerSize + types.UInt64Length - 1) / types.UInt64Length * types.UInt64Length // memory alignment
	if headerSize >= nodeSize {
		return nil, errors.New("node size is too small")
	}

	spaceLeft := nodeSize - headerSize

	var t types.DataItem[K, V]
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

	fmt.Println(numOfItems)

	return &DataNodeAllocator[K, V]{
		state:       state,
		numOfItems:  numOfItems,
		itemSize:    itemSize,
		stateOffset: headerSize,
		itemOffset:  headerSize + stateSize,
	}, nil
}

// DataNodeAllocator converts nodes from bytes to data objects.
type DataNodeAllocator[K, V comparable] struct {
	state *alloc.State

	numOfItems  uintptr
	itemSize    uintptr
	stateOffset uintptr
	itemOffset  uintptr
}

// NewNode initializes new node.
func (na *DataNodeAllocator[K, V]) NewNode() *DataNode[K, V] {
	return &DataNode[K, V]{
		numOfItems: na.numOfItems,
		itemSize:   na.itemSize,
	}
}

// Get returns object for node.
func (na *DataNodeAllocator[K, V]) Get(nodeAddress types.LogicalAddress, node *DataNode[K, V]) {
	na.project(na.state.Node(nodeAddress), node)
}

// Allocate allocates new object.
func (na *DataNodeAllocator[K, V]) Allocate(
	pool *alloc.Pool[types.LogicalAddress],
	node *DataNode[K, V],
) (types.LogicalAddress, error) {
	nodeAddress, err := pool.Allocate()
	if err != nil {
		return 0, err
	}

	na.project(na.state.Node(nodeAddress), node)
	return nodeAddress, nil
}

// Shift shifts bits in hash.
func (na *DataNodeAllocator[K, V]) Shift(hash types.Hash) types.Hash {
	return hash / types.Hash(na.numOfItems)
}

func (na *DataNodeAllocator[K, V]) project(nodeP unsafe.Pointer, node *DataNode[K, V]) {
	node.Header = photon.FromPointer[DataNodeHeader](nodeP)
	node.statesP = unsafe.Add(nodeP, na.stateOffset)
	node.itemsP = unsafe.Add(nodeP, na.itemOffset)
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

// PointerNode represents data stored inside pointer node.
type PointerNode struct {
	Header *PointerNodeHeader

	numOfItems uintptr
	itemSize   uintptr
	statesP    unsafe.Pointer
	itemsP     unsafe.Pointer
}

// ItemByHash returns pointers to the item and its state by hash.
func (sn *PointerNode) ItemByHash(hash types.Hash) (*types.SpacePointer, *types.State) {
	index := uintptr(hash) % sn.numOfItems
	return (*types.SpacePointer)(unsafe.Add(sn.itemsP, sn.itemSize*index)), (*types.State)(unsafe.Add(sn.statesP, index))
}

// Iterator iterates over items.
func (sn *PointerNode) Iterator() func(func(*types.SpacePointer, *types.State) bool) {
	return func(yield func(*types.SpacePointer, *types.State) bool) {
		itemsP := sn.itemsP
		statesP := sn.statesP
		for range sn.numOfItems {
			if !yield((*types.SpacePointer)(itemsP), (*types.State)(statesP)) {
				return
			}
			itemsP = unsafe.Add(itemsP, sn.itemSize)
			statesP = unsafe.Add(statesP, 1)
		}
	}
}

// DataNode represents data stored inside data node.
type DataNode[K, V comparable] struct {
	Header *DataNodeHeader

	numOfItems uintptr
	itemSize   uintptr
	statesP    unsafe.Pointer
	itemsP     unsafe.Pointer
}

// ItemByHash returns pointers to the item and its state by hash.
func (sn *DataNode[K, V]) ItemByHash(hash types.Hash) (*types.DataItem[K, V], *types.State) {
	index := uintptr(hash) % sn.numOfItems
	return (*types.DataItem[K, V])(unsafe.Add(sn.itemsP, sn.itemSize*index)), (*types.State)(unsafe.Add(sn.statesP, index))
}

// Iterator iterates over items.
func (sn *DataNode[K, V]) Iterator() func(func(*types.DataItem[K, V], *types.State) bool) {
	return func(yield func(*types.DataItem[K, V], *types.State) bool) {
		itemsP := sn.itemsP
		statesP := sn.statesP
		for range sn.numOfItems {
			if !yield((*types.DataItem[K, V])(itemsP), (*types.State)(statesP)) {
				return
			}
			itemsP = unsafe.Add(itemsP, sn.itemSize)
			statesP = unsafe.Add(statesP, 1)
		}
	}
}
