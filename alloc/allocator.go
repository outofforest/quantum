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
	numOfNodes := config.TotalSize / config.NodeSize
	data := make([]byte, config.TotalSize)
	availableNodes := map[types.NodeAddress][]byte{}
	for i := types.NodeAddress(0x01); i < types.NodeAddress(numOfNodes); i++ {
		availableNodes[i] = data[uint64(i)*config.NodeSize : uint64(i+1)*config.NodeSize]
	}

	return &Allocator{
		config:         config,
		data:           data,
		availableNodes: availableNodes,
	}
}

// Allocator is the allocator implementation used in tests.
type Allocator struct {
	config         Config
	data           []byte
	availableNodes map[types.NodeAddress][]byte
}

// Node returns node bytes.
func (a *Allocator) Node(nodeAddress types.NodeAddress) []byte {
	return a.data[uint64(nodeAddress)*a.config.NodeSize : uint64(nodeAddress+1)*a.config.NodeSize]
}

// Allocate allocates node and copies data into it.
func (a *Allocator) Allocate(copyFrom []byte) (types.NodeAddress, []byte, error) {
	var nodeAddress types.NodeAddress
	var nodeData []byte
	for nodeAddress, nodeData = range a.availableNodes {
		break
	}
	if nodeData == nil {
		return 0, nil, errors.New("out of space")
	}

	delete(a.availableNodes, nodeAddress)

	if copyFrom == nil {
		clear(nodeData)
	} else {
		copy(nodeData, copyFrom)
	}

	return nodeAddress, nodeData, nil
}

// Deallocate deallocates node.
func (a *Allocator) Deallocate(nodeAddress types.NodeAddress) {
	a.availableNodes[nodeAddress] = a.data[uint64(nodeAddress)*a.config.NodeSize : uint64(nodeAddress+1)*a.config.NodeSize]
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
	nodeAddress, node, err := sa.allocator.Allocate(nil)
	if err != nil {
		return 0, nil, err
	}

	return nodeAddress, node, nil
}

// Copy allocates new node and copies content from existing one.
func (sa SnapshotAllocator) Copy(data []byte) (types.NodeAddress, []byte, error) {
	nodeAddress, node, err := sa.allocator.Allocate(data)
	if err != nil {
		return 0, nil, err
	}

	return nodeAddress, node, nil
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
func (sa ImmediateSnapshotAllocator) Copy(data []byte) (types.NodeAddress, []byte, error) {
	return sa.parentSnapshotAllocator.Copy(data)
}

// Deallocate marks node for deallocation.
func (sa ImmediateSnapshotAllocator) Deallocate(nodeAddress types.NodeAddress, _ types.SnapshotID) error {
	// using sa.snapshotID instead of the snapshotID argument causes immediate deallocation.
	return sa.parentSnapshotAllocator.Deallocate(nodeAddress, sa.snapshotID)
}
