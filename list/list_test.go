package list_test

import (
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/list"
	"github.com/outofforest/quantum/space"
	"github.com/outofforest/quantum/types"
)

func newEnv(requireT *require.Assertions) *env {
	allocator := alloc.NewTestAllocator(alloc.NewAllocator(alloc.Config{
		TotalSize: 1024 * 1024,
		NodeSize:  512,
	}))

	nodeAllocator, err := list.NewNodeAllocator(allocator)
	requireT.NoError(err)

	return &env{
		Allocator:     allocator,
		Item:          lo.ToPtr[types.NodeAddress](0),
		nodeAllocator: nodeAllocator,
	}
}

type env struct {
	Allocator *alloc.TestAllocator
	Item      *types.NodeAddress

	snapshotID    types.SnapshotID
	nodeAllocator list.NodeAllocator
}

func (e *env) NextSnapshot() *list.List {
	snapshotID := e.snapshotID
	e.snapshotID++

	itemCopy := *e.Item
	e.Item = &itemCopy

	return list.New(list.Config{
		SnapshotID:    snapshotID,
		Item:          e.Item,
		NodeAllocator: e.nodeAllocator,
		Allocator: alloc.NewSnapshotAllocator(
			snapshotID,
			e.Allocator,
			&space.Space[types.SnapshotID, types.NodeAddress]{},
			e.nodeAllocator,
		),
	})
}
