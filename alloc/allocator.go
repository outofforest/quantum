package alloc

import (
	"github.com/pkg/errors"

	"github.com/outofforest/quantum/list"
	"github.com/outofforest/quantum/space"
	"github.com/outofforest/quantum/types"
)

// Config stores configuration of allocator.
type Config struct {
	TotalSize uint64
	NodeSize  uint64
}

// NewAllocator creates memory allocator.
func NewAllocator(config Config) *Allocator {
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
		config:         config,
		nodes:          nodes,
		availableNodes: availableNodes,
	}
}

// Allocator is the allocator implementation used in tests.
type Allocator struct {
	config         Config
	nodes          [][]byte
	availableNodes map[types.NodeAddress]struct{}
}

// Node returns node bytes.
func (a *Allocator) Node(nodeAddress types.NodeAddress) []byte {
	return a.nodes[nodeAddress]
}

// Allocate allocates node.
func (a *Allocator) Allocate() (types.NodeAddress, []byte, error) {
	if len(a.availableNodes) == 0 {
		return 0, nil, errors.New("out of space")
	}
	var nodeAddress types.NodeAddress
	for nodeAddress = range a.availableNodes {
		break
	}

	delete(a.availableNodes, nodeAddress)

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
	a.availableNodes[nodeAddress] = struct{}{}
	clear(a.nodes[nodeAddress])
}

// NodeSize returns size of node.
func (a *Allocator) NodeSize() uint64 {
	return a.config.NodeSize
}

// NewSnapshotAllocator returns snapshot-level allocator.
func NewSnapshotAllocator(
	snapshotID types.SnapshotID,
	allocator types.Allocator,
	snapshots *space.Space[types.SnapshotID, types.SnapshotInfo],
	deallocationLists *space.Space[types.SnapshotID, types.NodeAddress],
	listNodeAllocator list.NodeAllocator,
) SnapshotAllocator {
	return SnapshotAllocator{
		snapshotID:        snapshotID,
		allocator:         allocator,
		snapshots:         snapshots,
		deallocationLists: deallocationLists,
		listNodeAllocator: listNodeAllocator,
	}
}

// SnapshotAllocator allocates memory on behalf of snapshot.
type SnapshotAllocator struct {
	snapshotID        types.SnapshotID
	allocator         types.Allocator
	snapshots         *space.Space[types.SnapshotID, types.SnapshotInfo]
	deallocationLists *space.Space[types.SnapshotID, types.NodeAddress]
	listNodeAllocator list.NodeAllocator
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

	if _, exists := sa.snapshots.Get(srcSnapshotID); !exists {
		sa.allocator.Deallocate(nodeAddress)
		return nil
	}

	listNodeAddress, _ := sa.deallocationLists.Get(srcSnapshotID)
	newListNodeAddress := listNodeAddress
	l := list.New(list.Config{
		SnapshotID:    sa.snapshotID,
		Item:          &newListNodeAddress,
		NodeAllocator: sa.listNodeAllocator,
		Allocator:     NewImmediateSnapshotAllocator(sa.snapshotID, sa),
	})
	if err := l.Add(nodeAddress); err != nil {
		return err
	}
	if newListNodeAddress != listNodeAddress {
		if err := sa.deallocationLists.Set(srcSnapshotID, newListNodeAddress); err != nil {
			return err
		}
	}

	return nil
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
