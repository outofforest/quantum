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
func CollectListItems(l *list.List) []types.LogicalAddress {
	nodes := []types.LogicalAddress{}
	for pointer := range l.Iterator() {
		nodes = append(nodes, pointer.LogicalAddress)
	}

	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i] < nodes[j]
	})
	return nodes
}

// Error returns an error from the result of function call.
func Error(_ any, err error) error {
	return err
}
