package test

import (
	"sort"

	"golang.org/x/exp/constraints"

	"github.com/outofforest/quantum/list"
	"github.com/outofforest/quantum/space"
	"github.com/outofforest/quantum/types"
)

// CollectSpaceValues collects values available in space.
func CollectSpaceValues[K comparable, V constraints.Ordered](s *space.Space[K, V]) []V {
	values := []V{}
	for item := range s.Iterator() {
		values = append(values, item.Value)
	}

	sort.Slice(values, func(i, j int) bool {
		return values[i] < values[j]
	})
	return values
}

// CollectSpaceKeys collects keys available in space.
func CollectSpaceKeys[K constraints.Ordered, V comparable](s *space.Space[K, V]) []K {
	keys := []K{}
	for item := range s.Iterator() {
		keys = append(keys, item.Key)
	}

	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	return keys
}

// CollectListItems collects items available in list.
func CollectListItems(l *list.List) []types.NodeAddress {
	items := []types.NodeAddress{}
	for item := range l.Iterator() {
		items = append(items, item)
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i] < items[j]
	})
	return items
}
