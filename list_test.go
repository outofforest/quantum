package quantum

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestList(t *testing.T) {
	allocator := NewAllocator(AllocatorConfig{
		TotalSize: 1024 * 1024,
		NodeSize:  512,
	})
	allocator.Allocate()
	nodeAllocator, err := NewListNodeAllocator(allocator)
	require.NoError(t, err)

	list := NewList(ListConfig{
		NodeAllocator: nodeAllocator,
	})

	nItems := 3*nodeAllocator.numOfItems + nodeAllocator.numOfItems/2
	originalItems := []NodeAddress{}

	var item NodeAddress
	for range 10 {
		for range nItems {
			item++
			list.Add(item)
			originalItems = append(originalItems, item)
		}
		list2 := NewList(ListConfig{
			NodeAllocator: nodeAllocator,
		})
		for range nItems {
			item++
			list2.Add(item)
			originalItems = append(originalItems, item)
		}
		list.Attach(list2.config.Item)
	}

	items := make([]NodeAddress, 0, len(originalItems))
	for item := range list.Iterator() {
		items = append(items, item)
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i] < items[j]
	})

	require.Equal(t, originalItems, items)
}
