package space

import (
	"unsafe"

	"github.com/pkg/errors"

	"github.com/outofforest/quantum/types"
)

// NewPointerNodeAssistant creates new pointer node assistant.
func NewPointerNodeAssistant(nodeSize uint64) (*PointerNodeAssistant, error) {
	itemSize := uint64(unsafe.Sizeof(types.Pointer{})+types.UInt64Length-1) / types.UInt64Length * types.UInt64Length

	numOfItems := nodeSize / itemSize
	if numOfItems == 0 {
		return nil, errors.New("node size is too small")
	}

	return &PointerNodeAssistant{
		numOfItems: numOfItems,
		itemSize:   itemSize,
	}, nil
}

// PointerNodeAssistant converts nodes from bytes to pointer objects.
type PointerNodeAssistant struct {
	numOfItems uint64
	itemSize   uint64
}

// NumOfItems returns number of items fitting in one node.
func (na *PointerNodeAssistant) NumOfItems() uint64 {
	return na.numOfItems
}

// Index returns index from hash.
func (na *PointerNodeAssistant) Index(hash types.KeyHash) uint64 {
	return uint64(hash) % na.numOfItems
}

// Shift shifts bits in hash.
func (na *PointerNodeAssistant) Shift(hash types.KeyHash) types.KeyHash {
	return hash / types.KeyHash(na.numOfItems)
}

// ItemOffset returns item's offset relative to the beginning of the node.
func (na *PointerNodeAssistant) ItemOffset(index uint64) uint64 {
	return na.itemSize * index
}

// Item maps the memory address given by the node address and offset to an item.
func (na *PointerNodeAssistant) Item(n unsafe.Pointer, offset uint64) *types.Pointer {
	return (*types.Pointer)(unsafe.Add(n, offset))
}

// Iterator iterates over items.
func (na *PointerNodeAssistant) Iterator(n unsafe.Pointer) func(func(*types.Pointer) bool) {
	return func(yield func(*types.Pointer) bool) {
		for range na.numOfItems {
			if !yield((*types.Pointer)(n)) {
				return
			}
			n = unsafe.Add(n, na.itemSize)
		}
	}
}
