package list_test

import (
	"testing"

	"github.com/samber/lo"
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

	items := make([]types.NodeAddress, 0, 100)
	for i := range types.NodeAddress(cap(items)) {
		items = append(items, i)
		requireT.NoError(e.List.Add(i))
	}

	requireT.Equal(items, test.CollectListItems(e.List))

	nodesUsed, nodesAllocated, nodesDeallocated := e.Allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x01, 0x02}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x01, 0x02}, nodesAllocated)
	requireT.Empty(nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x01, 0x02}, e.List.Nodes())
}

func TestAttach(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT)
	e.NextSnapshot()

	items := make([]types.NodeAddress, 0, 1000)
	var item types.NodeAddress
	for range 100 {
		l2Address, l2 := e.NewList()
		for range 100 {
			items = append(items, item)
			requireT.NoError(l2.Add(item))
			item++
		}
		requireT.NoError(e.List.Attach(*l2Address))
	}

	requireT.Equal(items, test.CollectListItems(e.List))

	nodesUsed, nodesAllocated, nodesDeallocated := e.Allocator.Nodes()
	requireT.Equal([]types.NodeAddress{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf, 0x10, 0x11, 0x12, 0x13,
		0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20, 0x21, 0x22, 0x23, 0x24, 0x25,
		0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37,
		0x38, 0x39, 0x3a, 0x3b, 0x3c, 0x3d, 0x3e, 0x3f, 0x40, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x49,
		0x4a, 0x4b, 0x4c, 0x4d, 0x4e, 0x4f, 0x50, 0x51, 0x52, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58, 0x59, 0x5a, 0x5b,
		0x5c, 0x5d, 0x5e, 0x5f, 0x60, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6a, 0x6b, 0x6c, 0x6d,
		0x6e, 0x6f, 0x70, 0x71, 0x72, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78, 0x79, 0x7a, 0x7b, 0x7c, 0x7d, 0x7e, 0x7f,
		0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8a, 0x8b, 0x8c, 0x8d, 0x8e, 0x8f, 0x90, 0x91,
		0x92, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0x99, 0x9a, 0x9b, 0x9c, 0x9d, 0x9e, 0x9f, 0xa0, 0xa1, 0xa2, 0xa3,
		0xa4, 0xa5, 0xa6, 0xa7, 0xa8, 0xa9, 0xaa, 0xab, 0xac, 0xad, 0xae, 0xaf, 0xb0, 0xb1, 0xb2, 0xb3, 0xb4, 0xb5,
		0xb6, 0xb7, 0xb8, 0xb9, 0xba, 0xbb, 0xbc, 0xbd, 0xbe, 0xbf, 0xc0, 0xc1, 0xc2, 0xc3, 0xc4, 0xc5, 0xc6, 0xc7,
		0xc8, 0xc9, 0xca,
	}, nodesUsed)
	requireT.Equal(nodesUsed, nodesAllocated)
	requireT.Empty(nodesDeallocated)
	requireT.Equal(nodesUsed, e.List.Nodes())
}

func TestAddAttach(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT)
	e.NextSnapshot()

	items := make([]types.NodeAddress, 0, 10000)
	var item types.NodeAddress
	for range 1000 {
		l2Address, l2 := e.NewList()
		for range 5 {
			items = append(items, item)
			requireT.NoError(e.List.Add(item))
			item++

			items = append(items, item)
			requireT.NoError(l2.Add(item))
			item++
		}
		requireT.NoError(e.List.Attach(*l2Address))
	}

	requireT.Equal(items, test.CollectListItems(e.List))
}

func TestAttachTwoLevels(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT)
	e.NextSnapshot()

	items := make([]types.NodeAddress, 0, 10000)
	var item types.NodeAddress
	for range 100 {
		l2Address, l2 := e.NewList()
		for range 10 {
			l3Address, l3 := e.NewList()

			items = append(items, item)
			requireT.NoError(l3.Add(item))
			item++

			requireT.NoError(l2.Attach(*l3Address))
		}
		requireT.NoError(e.List.Attach(*l2Address))
	}

	requireT.Equal(items, test.CollectListItems(e.List))
}

func TestTwoSnapshots(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT)

	// Snapshot 0

	e.NextSnapshot()

	items0 := make([]types.NodeAddress, 0, 100)
	for i := range types.NodeAddress(cap(items0)) {
		items0 = append(items0, i)
		requireT.NoError(e.List.Add(i))
	}

	requireT.Equal(items0, test.CollectListItems(e.List))

	nodesUsed, nodesAllocated, nodesDeallocated := e.Allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x01, 0x02}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x01, 0x02}, nodesAllocated)
	requireT.Empty(nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x01, 0x02}, e.List.Nodes())

	// Snapshot 1

	items1 := make([]types.NodeAddress, 0, 2*cap(items0))
	items1 = append(items1, items0...)

	e.NextSnapshot()

	for i := types.NodeAddress(cap(items0)); i < types.NodeAddress(cap(items1)); i++ {
		items1 = append(items1, i)
		requireT.NoError(e.List.Add(i))
	}

	nodesUsed, nodesAllocated, nodesDeallocated = e.Allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x01, 0x03, 0x04}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x03, 0x04}, nodesAllocated)
	requireT.Equal([]types.NodeAddress{0x02}, nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x01, 0x02, 0x03, 0x04}, e.List.Nodes())

	requireT.Equal(items1, test.CollectListItems(e.List))
}

func TestDeallocate(t *testing.T) {
	// FIXME (wojciech): skipped until deallocation is done for disk allocator only.
	t.Skip()
	requireT := require.New(t)
	e := newEnv(requireT)
	e.NextSnapshot()

	nodeAddress, _, err := e.Allocator.Allocate()
	requireT.NoError(err)
	requireT.NoError(e.List.Add(nodeAddress))

	for range 10 {
		l2Address, l2 := e.NewList()
		nodeAddress, _, err := e.Allocator.Allocate()
		requireT.NoError(err)
		requireT.NoError(l2.Add(nodeAddress))
		for range 10 {
			l3Address, l3 := e.NewList()
			nodeAddress, _, err := e.Allocator.Allocate()
			requireT.NoError(err)

			requireT.NoError(l3.Add(nodeAddress))

			requireT.NoError(l2.Attach(*l3Address))
		}
		requireT.NoError(e.List.Attach(*l2Address))
	}

	nodesUsed1, nodesAllocated1, nodesDeallocated1 := e.Allocator.Nodes()
	requireT.NotEmpty(nodesUsed1)
	requireT.Equal(nodesUsed1, nodesAllocated1)
	requireT.Empty(nodesDeallocated1)

	e.List.Deallocate(e.Allocator)

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

	nodeAllocator, err := list.NewNodeAllocator(allocator)
	requireT.NoError(err)

	e := &env{
		Allocator: allocator,
		Item:      lo.ToPtr[types.NodeAddress](0),
		snapshotAllocator: alloc.NewImmediateSnapshotAllocator(alloc.NewSnapshotAllocator(
			allocator,
			map[types.SnapshotID]alloc.ListToCommit{},
			map[types.SnapshotID]struct{}{},
			nodeAllocator,
		)),
		nodeAllocator: nodeAllocator,
	}

	e.List = list.New(list.Config{
		Item:          e.Item,
		NodeAllocator: e.nodeAllocator,
		Allocator:     e.snapshotAllocator,
	})

	return e
}

type env struct {
	Allocator *test.Allocator
	List      *list.List
	Item      *types.NodeAddress

	snapshotID        types.SnapshotID
	snapshotAllocator types.SnapshotAllocator
	nodeAllocator     *list.NodeAllocator
}

func (e *env) NextSnapshot() {
	e.snapshotAllocator.SetSnapshotID(e.snapshotID)
	e.snapshotID++
}

func (e *env) NewList() (*types.NodeAddress, *list.List) {
	nodeAddress := lo.ToPtr[types.NodeAddress](0)
	return nodeAddress, list.New(list.Config{
		Item:          nodeAddress,
		NodeAllocator: e.nodeAllocator,
		Allocator:     e.snapshotAllocator,
	})
}
