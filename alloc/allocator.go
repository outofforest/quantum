package alloc

import (
	"github.com/outofforest/quantum/list"
	"github.com/outofforest/quantum/space"
	"github.com/outofforest/quantum/types"
)

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
		dirtyNodes:        map[types.NodeAddress]struct{}{},
	}
}

// SnapshotAllocator allocates memory on behalf of snapshot.
type SnapshotAllocator struct {
	snapshotID        types.SnapshotID
	allocator         types.Allocator
	snapshots         *space.Space[types.SnapshotID, types.SnapshotInfo]
	deallocationLists *space.Space[types.SnapshotID, types.NodeAddress]
	listNodeAllocator list.NodeAllocator

	dirtyNodes map[types.NodeAddress]struct{}
}

// Allocate allocates new node.
func (sa SnapshotAllocator) Allocate() (types.NodeAddress, []byte, error) {
	nodeAddress, node, err := sa.allocator.Allocate(nil)
	if err != nil {
		return 0, nil, err
	}

	sa.dirtyNodes[nodeAddress] = struct{}{}
	return nodeAddress, node, nil
}

// Copy allocates new node and copies content from existing one.
func (sa SnapshotAllocator) Copy(data []byte) (types.NodeAddress, []byte, error) {
	nodeAddress, node, err := sa.allocator.Allocate(data)
	if err != nil {
		return 0, nil, err
	}

	sa.dirtyNodes[nodeAddress] = struct{}{}
	return nodeAddress, node, nil
}

// Deallocate marks node for deallocation.
func (sa SnapshotAllocator) Deallocate(nodeAddress types.NodeAddress, srcSnapshotID types.SnapshotID) error {
	if srcSnapshotID == sa.snapshotID {
		delete(sa.dirtyNodes, nodeAddress)
		sa.allocator.Deallocate(nodeAddress)
		return nil
	}

	if _, exists := sa.snapshots.Get(srcSnapshotID); !exists {
		delete(sa.dirtyNodes, nodeAddress)
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
