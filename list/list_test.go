package list_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/list"
	"github.com/outofforest/quantum/test"
	"github.com/outofforest/quantum/types"
)

func TestAdd(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT)
	e.NextSnapshot()

	items := make([]types.LogicalAddress, 0, 100)
	for i := range types.LogicalAddress(cap(items)) {
		items = append(items, i)
		requireT.NoError(e.List.Add(types.Pointer{
			LogicalAddress: i,
		}))
	}

	requireT.Equal(items, test.CollectListItems(e.List))

	nodesUsed, nodesAllocated, nodesDeallocated := e.Allocator.Nodes()
	requireT.Equal([]types.LogicalAddress{0x01, 0x02, 0x03, 0x04}, nodesUsed)
	requireT.Equal([]types.LogicalAddress{0x01, 0x02, 0x03, 0x04}, nodesAllocated)
	requireT.Empty(nodesDeallocated)
	requireT.Equal([]types.LogicalAddress{0x01, 0x02, 0x03, 0x04}, e.List.Nodes())
}

func TestAttach(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT)
	e.NextSnapshot()

	items := make([]types.LogicalAddress, 0, 250)
	var logicalAddress types.LogicalAddress
	for range 50 {
		l2Address, l2 := e.NewList(requireT)
		for range 50 {
			items = append(items, logicalAddress)
			requireT.NoError(l2.Add(types.Pointer{
				LogicalAddress: logicalAddress,
			}))
			logicalAddress++
		}
		requireT.NoError(e.List.Attach(*l2Address))
	}

	requireT.Equal(items, test.CollectListItems(e.List))

	nodesUsed, nodesAllocated, nodesDeallocated := e.Allocator.Nodes()
	requireT.Equal([]types.LogicalAddress{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12,
		0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20, 0x21, 0x22, 0x23, 0x24,
		0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36,
		0x37, 0x38, 0x39, 0x3a, 0x3b, 0x3c, 0x3d, 0x3e, 0x3f, 0x40, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48,
		0x49, 0x4a, 0x4b, 0x4c, 0x4d, 0x4e, 0x4f, 0x50, 0x51, 0x52, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58, 0x59, 0x5a,
		0x5b, 0x5c, 0x5d, 0x5e, 0x5f, 0x60, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66,
	}, nodesUsed)
	requireT.Equal(nodesUsed, nodesAllocated)
	requireT.Empty(nodesDeallocated)
	requireT.Equal(nodesUsed, e.List.Nodes())
}

func TestAddAttach(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT)
	e.NextSnapshot()

	logicalAddresses := make([]types.LogicalAddress, 0, 10000)
	var logicalAddress types.LogicalAddress
	for range 1000 {
		l2Address, l2 := e.NewList(requireT)
		for range 5 {
			logicalAddresses = append(logicalAddresses, logicalAddress)
			requireT.NoError(e.List.Add(types.Pointer{
				LogicalAddress: logicalAddress,
			}))
			logicalAddress++

			logicalAddresses = append(logicalAddresses, logicalAddress)
			requireT.NoError(l2.Add(types.Pointer{
				LogicalAddress: logicalAddress,
			}))
			logicalAddress++
		}
		requireT.NoError(e.List.Attach(*l2Address))
	}

	requireT.Equal(logicalAddresses, test.CollectListItems(e.List))
}

func TestAttachTwoLevels(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT)
	e.NextSnapshot()

	logicalAddresses := make([]types.LogicalAddress, 0, 10000)
	var logicalAddress types.LogicalAddress
	for range 100 {
		l2Address, l2 := e.NewList(requireT)
		for range 10 {
			l3Address, l3 := e.NewList(requireT)

			logicalAddresses = append(logicalAddresses, logicalAddress)
			requireT.NoError(l3.Add(types.Pointer{
				LogicalAddress: logicalAddress,
			}))
			logicalAddress++

			requireT.NoError(l2.Attach(*l3Address))
		}
		requireT.NoError(e.List.Attach(*l2Address))
	}

	requireT.Equal(logicalAddresses, test.CollectListItems(e.List))
}

func TestTwoSnapshots(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT)

	// Snapshot 0

	e.NextSnapshot()

	logicalAddresses0 := make([]types.LogicalAddress, 0, 100)
	for i := range types.LogicalAddress(cap(logicalAddresses0)) {
		logicalAddresses0 = append(logicalAddresses0, i)
		requireT.NoError(e.List.Add(types.Pointer{
			LogicalAddress: i,
		}))
	}

	requireT.Equal(logicalAddresses0, test.CollectListItems(e.List))

	nodesUsed, nodesAllocated, nodesDeallocated := e.Allocator.Nodes()
	requireT.Equal([]types.LogicalAddress{0x01, 0x02, 0x03, 0x04}, nodesUsed)
	requireT.Equal([]types.LogicalAddress{0x01, 0x02, 0x03, 0x04}, nodesAllocated)
	requireT.Empty(nodesDeallocated)
	requireT.Equal([]types.LogicalAddress{0x01, 0x02, 0x03, 0x04}, e.List.Nodes())

	// Snapshot 1

	logicalAddresses1 := make([]types.LogicalAddress, 0, 2*cap(logicalAddresses0))
	logicalAddresses1 = append(logicalAddresses1, logicalAddresses0...)

	e.NextSnapshot()

	for i := types.LogicalAddress(cap(logicalAddresses0)); i < types.LogicalAddress(cap(logicalAddresses1)); i++ {
		logicalAddresses1 = append(logicalAddresses1, i)
		requireT.NoError(e.List.Add(types.Pointer{
			LogicalAddress: i,
		}))
	}

	nodesUsed, nodesAllocated, nodesDeallocated = e.Allocator.Nodes()
	requireT.Equal([]types.LogicalAddress{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}, nodesUsed)
	requireT.Equal([]types.LogicalAddress{0x05, 0x06, 0x07}, nodesAllocated)
	requireT.Empty(nodesDeallocated)
	requireT.Equal([]types.LogicalAddress{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}, e.List.Nodes())

	requireT.Equal(logicalAddresses1, test.CollectListItems(e.List))
}

func TestDeallocate(t *testing.T) {
	// FIXME (wojciech): skipped until deallocation is done for disk allocator only.
	t.Skip()
	requireT := require.New(t)
	e := newEnv(requireT)
	e.NextSnapshot()

	logicalAddress, _, err := e.Allocator.Allocate()
	requireT.NoError(err)
	requireT.NoError(e.List.Add(types.Pointer{
		LogicalAddress: logicalAddress,
	}))

	for range 10 {
		l2Address, l2 := e.NewList(requireT)
		logicalAddress, _, err := e.Allocator.Allocate()
		requireT.NoError(err)
		requireT.NoError(l2.Add(types.Pointer{
			LogicalAddress: logicalAddress,
		}))
		for range 10 {
			l3Address, l3 := e.NewList(requireT)
			logicalAddress, _, err := e.Allocator.Allocate()
			requireT.NoError(err)

			requireT.NoError(l3.Add(types.Pointer{
				LogicalAddress: logicalAddress,
			}))

			requireT.NoError(l2.Attach(*l3Address))
		}
		requireT.NoError(e.List.Attach(*l2Address))
	}

	nodesUsed1, nodesAllocated1, nodesDeallocated1 := e.Allocator.Nodes()
	requireT.NotEmpty(nodesUsed1)
	requireT.Equal(nodesUsed1, nodesAllocated1)
	requireT.Empty(nodesDeallocated1)

	e.List.Deallocate()

	nodesUsed2, nodesAllocated2, nodesDeallocated2 := e.Allocator.Nodes()
	requireT.Empty(nodesUsed2)
	requireT.Empty(nodesAllocated2)
	requireT.Equal(nodesAllocated1, nodesDeallocated2)
}

func newEnv(requireT *require.Assertions) *env {
	allocator := test.NewAllocator(test.AllocatorConfig{
		TotalSize: 1024 * 1024,
		NodeSize:  512,
	})
	dirtyListNodesCh := make(chan types.DirtyListNode, 100000)

	e := &env{
		Allocator: allocator,
		ListRoot:  &types.Pointer{},
		snapshotAllocator: alloc.NewImmediateSnapshotAllocator(alloc.NewSnapshotAllocator(
			allocator,
			map[types.SnapshotID]alloc.ListToCommit{},
			map[types.SnapshotID]struct{}{},
			dirtyListNodesCh,
		)),
		DirtyListNodesCh: dirtyListNodesCh,
	}

	var err error
	e.List, err = list.New(list.Config{
		ListRoot:          e.ListRoot,
		Allocator:         e.Allocator,
		SnapshotAllocator: e.snapshotAllocator,
		DirtyListNodesCh:  e.DirtyListNodesCh,
	})
	requireT.NoError(err)

	return e
}

type env struct {
	Allocator        *test.Allocator
	List             *list.List
	ListRoot         *types.Pointer
	DirtyListNodesCh chan types.DirtyListNode

	snapshotID        types.SnapshotID
	snapshotAllocator types.SnapshotAllocator
}

func (e *env) NextSnapshot() {
	e.snapshotAllocator.SetSnapshotID(e.snapshotID)
	e.snapshotID++
}

func (e *env) NewList(requireT *require.Assertions) (*types.Pointer, *list.List) {
	listRoot := &types.Pointer{}
	l, err := list.New(list.Config{
		ListRoot:          listRoot,
		Allocator:         e.Allocator,
		SnapshotAllocator: e.snapshotAllocator,
		DirtyListNodesCh:  e.DirtyListNodesCh,
	})
	requireT.NoError(err)
	return listRoot, l
}
