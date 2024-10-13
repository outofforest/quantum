package alloc

import (
	"github.com/pkg/errors"
	"github.com/samber/lo"

	"github.com/outofforest/quantum/list"
	"github.com/outofforest/quantum/types"
)

const nodesPerGroup = 1024

// Config stores configuration of allocator.
type Config struct {
	TotalSize uint64
	NodeSize  uint64
}

// NewAllocator creates memory allocator.
func NewAllocator(config Config) *Allocator {
	numOfGroups := config.TotalSize / config.NodeSize / nodesPerGroup
	numOfNodes := numOfGroups*nodesPerGroup + 1
	data := make([]byte, config.NodeSize*numOfNodes)
	nodes := make([][]byte, 0, numOfNodes)

	availableNodes := make([]types.NodeAddress, 0, numOfNodes-1)
	for i := types.NodeAddress(0); i < types.NodeAddress(numOfNodes); i++ {
		nodes = append(nodes, data[uint64(i)*config.NodeSize:uint64(i+1)*config.NodeSize])
		if i > 0 {
			availableNodes = append(availableNodes, i)
		}
	}

	availableNodesCh := make(chan []types.NodeAddress, numOfGroups)
	for i := uint64(0); i < uint64(len(availableNodes)); i += nodesPerGroup {
		availableNodesCh <- availableNodes[i : i+nodesPerGroup]
	}

	return &Allocator{
		config:                 config,
		nodes:                  nodes,
		availableNodesCh:       availableNodesCh,
		nodesToAllocate:        <-availableNodesCh,
		nodesToDeallocate:      make([]types.NodeAddress, 0, nodesPerGroup),
		nodesToDeallocateStack: make([][]types.NodeAddress, 0, 1),
	}
}

// Allocator is the allocator implementation used in tests.
type Allocator struct {
	config             Config
	nodes              [][]byte
	availableNodesCh   chan []types.NodeAddress
	deallocatedNodesCh chan []types.NodeAddress

	nodesToAllocate        []types.NodeAddress
	nodesToDeallocate      []types.NodeAddress
	nodesToDeallocateStack [][]types.NodeAddress
}

// Node returns node bytes.
func (a *Allocator) Node(nodeAddress types.NodeAddress) []byte {
	return a.nodes[nodeAddress]
}

// Allocate allocates node.
func (a *Allocator) Allocate() (types.NodeAddress, []byte, error) {
	nodeAddress := a.nodesToAllocate[len(a.nodesToAllocate)-1]
	a.nodesToAllocate = a.nodesToAllocate[:len(a.nodesToAllocate)-1]

	if len(a.nodesToAllocate) == 0 {
		if len(a.availableNodesCh) == 0 {
			return 0, nil, errors.New("out of space")
		}
		a.nodesToDeallocateStack = append(a.nodesToDeallocateStack, a.nodesToAllocate)
		a.nodesToAllocate = <-a.availableNodesCh
	}

	return nodeAddress, a.nodes[nodeAddress], nil
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
	if a.deallocatedNodesCh == nil {
		a.deallocatedNodesCh = make(chan []types.NodeAddress, 100)
		for range 5 {
			go func() {
				for nodes := range a.deallocatedNodesCh {
					for _, n := range nodes {
						clear(a.nodes[n])
					}
					a.availableNodesCh <- nodes
				}
			}()
		}
	}

	a.nodesToDeallocate = append(a.nodesToDeallocate, nodeAddress)
	if len(a.nodesToDeallocate) == nodesPerGroup {
		a.deallocatedNodesCh <- a.nodesToDeallocate
		a.nodesToDeallocate = a.nodesToDeallocateStack[len(a.nodesToDeallocateStack)-1]
		a.nodesToDeallocateStack = a.nodesToDeallocateStack[:len(a.nodesToDeallocateStack)-1]
	}
}

// NodeSize returns size of node.
func (a *Allocator) NodeSize() uint64 {
	return a.config.NodeSize
}

// ListToCommit contains cached deallocation list.
type ListToCommit struct {
	List *list.List
	Item *types.NodeAddress
}

// NewSnapshotAllocator returns snapshot-level allocator.
func NewSnapshotAllocator(
	snapshotID types.SnapshotID,
	allocator types.Allocator,
	deallocationListCache map[types.SnapshotID]ListToCommit,
	availableSnapshots map[types.SnapshotID]struct{},
	listNodeAllocator list.NodeAllocator,
) SnapshotAllocator {
	return SnapshotAllocator{
		snapshotID:            snapshotID,
		allocator:             allocator,
		deallocationListCache: deallocationListCache,
		availableSnapshots:    availableSnapshots,
		listNodeAllocator:     listNodeAllocator,
	}
}

// SnapshotAllocator allocates memory on behalf of snapshot.
type SnapshotAllocator struct {
	snapshotID            types.SnapshotID
	allocator             types.Allocator
	deallocationListCache map[types.SnapshotID]ListToCommit
	availableSnapshots    map[types.SnapshotID]struct{}
	listNodeAllocator     list.NodeAllocator
}

// Allocate allocates new node.
func (sa SnapshotAllocator) Allocate() (types.NodeAddress, []byte, error) {
	nodeAddress, node, err := sa.allocator.Allocate()
	if err != nil {
		return 0, nil, err
	}

	return nodeAddress, node, nil
}

// Copy allocates new node and copies content from existing one.
func (sa SnapshotAllocator) Copy(nodeAddress types.NodeAddress) (types.NodeAddress, []byte, error) {
	newNodeAddress, node, err := sa.allocator.Copy(nodeAddress)
	if err != nil {
		return 0, nil, err
	}

	return newNodeAddress, node, nil
}

// Deallocate marks node for deallocation.
func (sa SnapshotAllocator) Deallocate(nodeAddress types.NodeAddress, srcSnapshotID types.SnapshotID) error {
	if srcSnapshotID == sa.snapshotID {
		sa.allocator.Deallocate(nodeAddress)
		return nil
	}

	if _, exists := sa.availableSnapshots[srcSnapshotID]; !exists {
		sa.allocator.Deallocate(nodeAddress)
		return nil
	}

	l, exists := sa.deallocationListCache[srcSnapshotID]
	if !exists {
		l.Item = lo.ToPtr[types.NodeAddress](0)
		l.List = list.New(list.Config{
			SnapshotID:    sa.snapshotID,
			Item:          l.Item,
			NodeAllocator: sa.listNodeAllocator,
			Allocator:     NewImmediateSnapshotAllocator(sa.snapshotID, sa),
		})
		sa.deallocationListCache[srcSnapshotID] = l
	}

	return l.List.Add(nodeAddress)
}

// NewImmediateSnapshotAllocator creates new immediate snapshot deallocator.
func NewImmediateSnapshotAllocator(
	snapshotID types.SnapshotID,
	parentSnapshotAllocator types.SnapshotAllocator,
) ImmediateSnapshotAllocator {
	return ImmediateSnapshotAllocator{
		snapshotID:              snapshotID,
		parentSnapshotAllocator: parentSnapshotAllocator,
	}
}

// ImmediateSnapshotAllocator deallocates nodes immediately instead of adding them to deallocation list.
type ImmediateSnapshotAllocator struct {
	snapshotID              types.SnapshotID
	parentSnapshotAllocator types.SnapshotAllocator
}

// Allocate allocates new node.
func (sa ImmediateSnapshotAllocator) Allocate() (types.NodeAddress, []byte, error) {
	return sa.parentSnapshotAllocator.Allocate()
}

// Copy allocates new node and copies content from existing one.
func (sa ImmediateSnapshotAllocator) Copy(nodeAddress types.NodeAddress) (types.NodeAddress, []byte, error) {
	return sa.parentSnapshotAllocator.Copy(nodeAddress)
}

// Deallocate marks node for deallocation.
func (sa ImmediateSnapshotAllocator) Deallocate(nodeAddress types.NodeAddress, _ types.SnapshotID) error {
	// using sa.snapshotID instead of the snapshotID argument causes immediate deallocation.
	return sa.parentSnapshotAllocator.Deallocate(nodeAddress, sa.snapshotID)
}
