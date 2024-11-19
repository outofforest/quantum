package space

import (
	"unsafe"

	"github.com/pkg/errors"

	"github.com/outofforest/quantum/types"
)

// NewDataNodeAssistant creates new space data node assistant.
func NewDataNodeAssistant[K, V comparable]() (*DataNodeAssistant[K, V], error) {
	itemSize := uint64(unsafe.Sizeof(types.DataItem[K, V]{})+types.UInt64Length-1) /
		types.UInt64Length * types.UInt64Length

	numOfItems := types.NodeLength / (itemSize + types.UInt64Length) // Uint64Length is for key hash.

	if numOfItems == 0 {
		return nil, errors.Errorf("item size %d is greater than node size %d",
			(itemSize + types.UInt64Length), types.NodeLength)
	}

	return &DataNodeAssistant[K, V]{
		itemSize:   itemSize,
		itemOffset: numOfItems * types.UInt64Length, // Space reserved for key hashes.
		numOfItems: numOfItems,
	}, nil
}

// DataNodeAssistant converts nodes from bytes to data objects.
type DataNodeAssistant[K, V comparable] struct {
	itemSize   uint64
	itemOffset uint64
	numOfItems uint64
}

// NumOfItems returns number of items fitting in one node.
func (na *DataNodeAssistant[K, V]) NumOfItems() uint64 {
	return na.numOfItems
}

// ItemOffset returns item's offset relative to the beginning of the node.
func (na *DataNodeAssistant[K, V]) ItemOffset(index uint64) uint64 {
	return na.itemOffset + na.itemSize*index
}

// Item maps the memory address given by the node address and offset to an item.
func (na *DataNodeAssistant[K, V]) Item(n unsafe.Pointer, offset uint64) *types.DataItem[K, V] {
	return (*types.DataItem[K, V])(unsafe.Add(n, offset))
}

// KeyHashes returns slice of key hashes stored in the node.
func (na *DataNodeAssistant[K, V]) KeyHashes(n unsafe.Pointer) []types.KeyHash {
	return unsafe.Slice((*types.KeyHash)(n), na.numOfItems)
}

// Iterator iterates over items.
func (na *DataNodeAssistant[K, V]) Iterator(n unsafe.Pointer) func(func(uint64, *types.DataItem[K, V]) bool) {
	return func(yield func(uint64, *types.DataItem[K, V]) bool) {
		keyHashP := n
		itemP := unsafe.Add(n, na.itemOffset)
		for i := range na.numOfItems {
			if *(*types.KeyHash)(keyHashP) != 0 && !yield(i, (*types.DataItem[K, V])(itemP)) {
				return
			}
			keyHashP = unsafe.Add(keyHashP, types.UInt64Length)
			itemP = unsafe.Add(itemP, na.itemSize)
		}
	}
}
