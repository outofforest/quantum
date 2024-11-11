package space

import (
	"unsafe"

	"github.com/pkg/errors"

	"github.com/outofforest/quantum/types"
)

const numOfSlots = 4

// NewDataNodeAssistant creates new space data node assistant.
func NewDataNodeAssistant[K, V comparable](nodeSize uint64) (*DataNodeAssistant[K, V], error) {
	headerSize := uint64(unsafe.Sizeof(DataNodeHeader{})+types.UInt64Length-1) / types.UInt64Length * types.UInt64Length
	slotSize := (nodeSize - headerSize) / numOfSlots / types.HashBlockSize * types.HashBlockSize

	itemSize := uint64(unsafe.Sizeof(types.DataItem[K, V]{})+types.UInt64Length-1) /
		types.UInt64Length * types.UInt64Length

	if itemSize > slotSize {
		return nil, errors.Errorf("item size %d is greater than slot size %d", itemSize, slotSize)
	}

	numOfItemsInSlot := slotSize / itemSize

	da := &DataNodeAssistant[K, V]{
		itemSize:         itemSize,
		numOfItems:       numOfItemsInSlot * numOfSlots,
		numOfItemsInSlot: numOfItemsInSlot,
	}

	for si := range uint64(numOfSlots) {
		da.itemOffsets[si] = headerSize + si*slotSize
	}

	return da, nil
}

// DataNodeAssistant converts nodes from bytes to data objects.
type DataNodeAssistant[K, V comparable] struct {
	itemSize         uint64
	numOfItems       uint64
	numOfItemsInSlot uint64
	itemOffsets      [numOfSlots]uint64
}

// NumOfItems returns number of items fitting in one node.
func (na *DataNodeAssistant[K, V]) NumOfItems() uint64 {
	return na.numOfItems
}

// Index returns index from hash.
func (na *DataNodeAssistant[K, V]) Index(hash types.KeyHash) uint64 {
	return uint64(hash) % na.numOfItems
}

// Shift shifts bits in hash.
func (na *DataNodeAssistant[K, V]) Shift(hash types.KeyHash) types.KeyHash {
	return hash / types.KeyHash(na.numOfItems)
}

// ItemOffset returns item's offset relative to the beginning of the node.
func (na *DataNodeAssistant[K, V]) ItemOffset(index uint64) uint64 {
	return na.itemOffsets[index/na.numOfItemsInSlot] + na.itemSize*(index%na.numOfItemsInSlot)
}

// Item maps the memory address given by the node address and offset to an item.
func (na *DataNodeAssistant[K, V]) Item(n unsafe.Pointer, offset uint64) *types.DataItem[K, V] {
	return (*types.DataItem[K, V])(unsafe.Add(n, offset))
}

// Iterator iterates over items.
func (na *DataNodeAssistant[K, V]) Iterator(n unsafe.Pointer) func(func(*types.DataItem[K, V]) bool) {
	return func(yield func(item *types.DataItem[K, V]) bool) {
		for _, offset := range na.itemOffsets {
			itemsP := unsafe.Add(n, offset)
			for range na.numOfItemsInSlot {
				if !yield((*types.DataItem[K, V])(itemsP)) {
					return
				}
				itemsP = unsafe.Add(itemsP, na.itemSize)
			}
		}
	}
}

// DataNodeHeader represents header of data node.
type DataNodeHeader struct {
	Hashes [numOfSlots]types.Hash
}
