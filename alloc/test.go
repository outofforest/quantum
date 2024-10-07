package alloc

import (
	"sort"

	"github.com/outofforest/quantum/types"
)

// NewTestAllocator creates memory allocator used in tests.
func NewTestAllocator(parentAllocator types.Allocator) *TestAllocator {
	return &TestAllocator{
		parentAllocator:  parentAllocator,
		nodesUsed:        map[types.NodeAddress]struct{}{},
		nodesAllocated:   map[types.NodeAddress]struct{}{},
		nodesDeallocated: map[types.NodeAddress]struct{}{},
	}
}

// TestAllocator is the allocator implementation used in tests.
type TestAllocator struct {
	parentAllocator types.Allocator

	nodesUsed        map[types.NodeAddress]struct{}
	nodesAllocated   map[types.NodeAddress]struct{}
	nodesDeallocated map[types.NodeAddress]struct{}
}

// Node returns node bytes.
func (a *TestAllocator) Node(nodeAddress types.NodeAddress) []byte {
	return a.parentAllocator.Node(nodeAddress)
}

// Allocate allocates node and copies data into it.
func (a *TestAllocator) Allocate(copyFrom []byte) (types.NodeAddress, []byte, error) {
	nodeAddress, node, err := a.parentAllocator.Allocate(copyFrom)
	if err != nil {
		return 0, nil, err
	}
	a.nodesAllocated[nodeAddress] = struct{}{}
	a.nodesUsed[nodeAddress] = struct{}{}
	return nodeAddress, node, nil
}

// Deallocate deallocates node.
func (a *TestAllocator) Deallocate(nodeAddress types.NodeAddress) {
	a.nodesDeallocated[nodeAddress] = struct{}{}
	delete(a.nodesUsed, nodeAddress)
	a.parentAllocator.Deallocate(nodeAddress)
}

// NodeSize returns size of node.
func (a *TestAllocator) NodeSize() uint64 {
	return a.parentAllocator.NodeSize()
}

// Nodes returns touched nodes.
func (a *TestAllocator) Nodes() (
	used []types.NodeAddress,
	allocated []types.NodeAddress,
	deallocated []types.NodeAddress,
) {
	return mapToSlice(a.nodesUsed, false),
		mapToSlice(a.nodesAllocated, true),
		mapToSlice(a.nodesDeallocated, true)
}

func mapToSlice(m map[types.NodeAddress]struct{}, empty bool) []types.NodeAddress {
	s := make([]types.NodeAddress, 0, len(m))
	for k := range m {
		s = append(s, k)
	}

	if empty {
		clear(m)
	}

	sort.Slice(s, func(i, j int) bool {
		return s[i] < s[j]
	})

	return s
}
