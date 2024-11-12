package space

import (
	"unsafe"

	"github.com/pkg/errors"

	"github.com/outofforest/quantum/types"
)

// NewDataNodeAssistant creates new space data node assistant.
func NewDataNodeAssistant[K, V comparable](nodeSize uint64) (*DataNodeAssistant[K, V], error) {
	itemSize := uint64(unsafe.Sizeof(types.DataItem[K, V]{})+types.UInt64Length-1) /
		types.UInt64Length * types.UInt64Length

	if itemSize > nodeSize {
		return nil, errors.Errorf("item size %d is greater than node size %d", itemSize, nodeSize)
	}

	return &DataNodeAssistant[K, V]{
		itemSize:   itemSize,
		numOfItems: nodeSize / itemSize,
	}, nil
}

// DataNodeAssistant converts nodes from bytes to data objects.
type DataNodeAssistant[K, V comparable] struct {
	itemSize   uint64
	numOfItems uint64
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
	return na.itemSize * index
}

// Item maps the memory address given by the node address and offset to an item.
func (na *DataNodeAssistant[K, V]) Item(n unsafe.Pointer, offset uint64) *types.DataItem[K, V] {
	return (*types.DataItem[K, V])(unsafe.Add(n, offset))
}

// Iterator iterates over items.
func (na *DataNodeAssistant[K, V]) Iterator(n unsafe.Pointer) func(func(*types.DataItem[K, V]) bool) {
	return func(yield func(item *types.DataItem[K, V]) bool) {
		for range na.numOfItems {
			if !yield((*types.DataItem[K, V])(n)) {
				return
			}
			n = unsafe.Add(n, na.itemSize)
		}
	}
}
