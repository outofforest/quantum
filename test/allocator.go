package test

import (
	"sort"

	"github.com/pkg/errors"

	"github.com/outofforest/quantum/types"
)

// AllocatorConfig stores configuration of allocator.
type AllocatorConfig struct {
	TotalSize uint64
	NodeSize  uint64
}

// NewAllocator creates memory allocator used in tests.
func NewAllocator(config AllocatorConfig) *Allocator {
	return &Allocator{
		config:   config,
		data:     make([]byte, config.TotalSize),
		zeroNode: make([]byte, config.NodeSize),

		nodesUsed:        map[types.NodeAddress]struct{}{},
		nodesAllocated:   map[types.NodeAddress]struct{}{},
		nodesDeallocated: map[types.NodeAddress]struct{}{},
	}
}

// Allocator is the allocator implementation used in tests.
type Allocator struct {
	config            AllocatorConfig
	data              []byte
	zeroNode          []byte
	lastAllocatedNode types.NodeAddress

	nodesUsed        map[types.NodeAddress]struct{}
	nodesAllocated   map[types.NodeAddress]struct{}
	nodesDeallocated map[types.NodeAddress]struct{}
}

// Node returns node bytes.
func (a *Allocator) Node(nodeAddress types.NodeAddress) []byte {
	return a.data[uint64(nodeAddress)*a.config.NodeSize : uint64(nodeAddress+1)*a.config.NodeSize]
}

// Allocate allocates node and copies data into it.
func (a *Allocator) Allocate(copyFrom []byte) (types.NodeAddress, []byte, error) {
	a.lastAllocatedNode++
	if uint64(a.lastAllocatedNode+1)*a.config.NodeSize > uint64(len(a.data)) {
		return 0, nil, errors.New("out of space")
	}
	node := a.Node(a.lastAllocatedNode)

	if copyFrom == nil {
		copyFrom = a.zeroNode
	}

	copy(node, copyFrom)

	a.nodesAllocated[a.lastAllocatedNode] = struct{}{}
	a.nodesUsed[a.lastAllocatedNode] = struct{}{}

	return a.lastAllocatedNode, node, nil
}

// Deallocate deallocates node.
func (a *Allocator) Deallocate(nodeAddress types.NodeAddress) {
	a.nodesDeallocated[nodeAddress] = struct{}{}
	delete(a.nodesUsed, nodeAddress)
}

// NodeSize returns size of node.
func (a *Allocator) NodeSize() uint64 {
	return a.config.NodeSize
}

// Nodes returns touched nodes.
func (a *Allocator) Nodes() (
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
