package test

import (
	"sort"
	"unsafe"

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
	numOfNodes := config.TotalSize/config.NodeSize + 1
	data := make([]byte, config.NodeSize*numOfNodes)
	for i := types.NodeAddress(0x01); i < types.NodeAddress(numOfNodes); i++ {
		availableNodes[i] = struct{}{}
	}

	return &Allocator{
		config:     config,
		data:       data,
		dataP:      unsafe.Pointer(&data[0]),
		numOfNodes: numOfNodes,

		nodesUsed:        map[types.NodeAddress]struct{}{},
		nodesAllocated:   map[types.NodeAddress]struct{}{},
		nodesDeallocated: map[types.NodeAddress]struct{}{},
	}
}

// Allocator is the allocator implementation used in tests.
type Allocator struct {
	config            AllocatorConfig
	data              []byte
	dataP             unsafe.Pointer
	numOfNodes        uint64
	lastAllocatedNode types.NodeAddress

	nodesUsed        map[types.NodeAddress]struct{}
	nodesAllocated   map[types.NodeAddress]struct{}
	nodesDeallocated map[types.NodeAddress]struct{}
}

// Node returns node bytes.
func (a *Allocator) Node(nodeAddress types.NodeAddress) unsafe.Pointer {
	return unsafe.Add(a.dataP, uint64(nodeAddress)*a.config.NodeSize)
}

// Allocate allocates node and copies data into it.
func (a *Allocator) Allocate() (types.NodeAddress, unsafe.Pointer, error) {
	a.lastAllocatedNode++
	if uint64(a.lastAllocatedNode) >= a.numOfNodes {
		return 0, nil, errors.New("out of space")
	}

	a.nodesAllocated[a.lastAllocatedNode] = struct{}{}
	a.nodesUsed[a.lastAllocatedNode] = struct{}{}

	return a.lastAllocatedNode, a.Node(a.lastAllocatedNode), nil
}

// Copy allocates new node and moves existing one there.
func (a *Allocator) Copy(nodeAddress types.NodeAddress) (types.NodeAddress, unsafe.Pointer, error) {
	// FIXME (wojciech): No-copy test
	// newNodeAddress, newNodeData, err := a.Allocate()
	// if err != nil {
	//	return 0, nil, err
	// }

	// a.nodes[nodeAddress], a.nodes[newNodeAddress] = newNodeData, a.nodes[nodeAddress]

	// return newNodeAddress, a.nodes[newNodeAddress], nil

	return nodeAddress, a.Node(nodeAddress), nil
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
