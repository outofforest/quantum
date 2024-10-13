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
	availableNodes := map[types.NodeAddress]struct{}{}
	numOfNodes := config.TotalSize / config.NodeSize
	data := make([]byte, config.NodeSize*numOfNodes)
	nodes := make([][]byte, 0, numOfNodes)
	for i := types.NodeAddress(0x00); i < types.NodeAddress(numOfNodes); i++ {
		nodes = append(nodes, data[uint64(i)*config.NodeSize:uint64(i+1)*config.NodeSize])
		if i > 0x00 {
			availableNodes[i] = struct{}{}
		}
	}

	return &Allocator{
		config: config,
		nodes:  nodes,

		nodesUsed:        map[types.NodeAddress]struct{}{},
		nodesAllocated:   map[types.NodeAddress]struct{}{},
		nodesDeallocated: map[types.NodeAddress]struct{}{},
	}
}

// Allocator is the allocator implementation used in tests.
type Allocator struct {
	config            AllocatorConfig
	nodes             [][]byte
	lastAllocatedNode types.NodeAddress

	nodesUsed        map[types.NodeAddress]struct{}
	nodesAllocated   map[types.NodeAddress]struct{}
	nodesDeallocated map[types.NodeAddress]struct{}
}

// Node returns node bytes.
func (a *Allocator) Node(nodeAddress types.NodeAddress) []byte {
	return a.nodes[nodeAddress]
}

// Allocate allocates node and copies data into it.
func (a *Allocator) Allocate() (types.NodeAddress, []byte, error) {
	a.lastAllocatedNode++
	if uint64(a.lastAllocatedNode) >= uint64(len(a.nodes)) {
		return 0, nil, errors.New("out of space")
	}

	a.nodesAllocated[a.lastAllocatedNode] = struct{}{}
	a.nodesUsed[a.lastAllocatedNode] = struct{}{}

	return a.lastAllocatedNode, a.nodes[a.lastAllocatedNode], nil
}

// Copy allocates new node and moves existing one there.
func (a *Allocator) Copy(nodeAddress types.NodeAddress) (types.NodeAddress, []byte, error) {
	newNodeAddress, newNodeData, err := a.Allocate()
	if err != nil {
		return 0, nil, err
	}

	a.nodes[nodeAddress], a.nodes[newNodeAddress] = newNodeData, a.nodes[nodeAddress]

	return newNodeAddress, a.nodes[newNodeAddress], nil
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
