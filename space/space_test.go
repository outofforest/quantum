package space_test

import (
	"sort"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/list"
	"github.com/outofforest/quantum/space"
	"github.com/outofforest/quantum/test"
	"github.com/outofforest/quantum/types"
)

func TestSetOneItem(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT, false)

	s := e.NextSnapshot(requireT)
	requireT.NoError(s.Set(0, 0))

	nodesUsed, nodesAllocated, nodesDeallocated := e.Allocator.Nodes()

	requireT.Equal([]types.NodeAddress{0x01, 0x02}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x01, 0x02}, nodesAllocated)
	requireT.Empty(nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x02}, s.Nodes())
	requireT.Equal([]types.NodeAddress{0x01}, e.Snapshots.Nodes())
	requireT.Empty(e.DeallocationLists.Nodes())

	requireT.Equal([]int{0}, test.CollectSpaceValues(s))
}

func TestSetTwoItems(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT, false)

	s := e.NextSnapshot(requireT)
	requireT.NoError(s.Set(0, 0))
	requireT.NoError(s.Set(1, 1))

	nodesUsed, nodesAllocated, nodesDeallocated := e.Allocator.Nodes()

	requireT.Equal([]types.NodeAddress{0x01, 0x02}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x01, 0x02}, nodesAllocated)
	requireT.Empty(nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x02}, s.Nodes())
	requireT.Equal([]types.NodeAddress{0x01}, e.Snapshots.Nodes())
	requireT.Empty(e.DeallocationLists.Nodes())

	requireT.Equal([]int{0, 1}, test.CollectSpaceValues(s))
}

func TestSetWithPointerNode(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT, false)
	s := e.NextSnapshot(requireT)

	// Insert 0

	requireT.NoError(s.Set(0, 0))
	nodesUsed, nodesAllocated, nodesDeallocated := e.Allocator.Nodes()

	requireT.Equal([]types.NodeAddress{0x01, 0x02}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x01, 0x02}, nodesAllocated)
	requireT.Empty(nodesDeallocated)
	requireT.Empty(e.DeallocationLists.Nodes())

	requireT.Equal([]int{0}, test.CollectSpaceValues(s))

	// Insert 1

	requireT.NoError(s.Set(1, 1))
	nodesUsed, nodesAllocated, nodesDeallocated = e.Allocator.Nodes()

	requireT.Equal([]types.NodeAddress{0x01, 0x02}, nodesUsed)
	requireT.Empty(nodesAllocated)
	requireT.Empty(nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x02}, s.Nodes())
	requireT.Equal([]types.NodeAddress{0x01}, e.Snapshots.Nodes())
	requireT.Empty(e.DeallocationLists.Nodes())

	requireT.Equal([]int{0, 1}, test.CollectSpaceValues(s))

	// Insert 2

	requireT.NoError(s.Set(2, 2))
	nodesUsed, nodesAllocated, nodesDeallocated = e.Allocator.Nodes()

	requireT.Equal([]types.NodeAddress{0x01, 0x02}, nodesUsed)
	requireT.Empty(nodesAllocated)
	requireT.Empty(nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x02}, s.Nodes())
	requireT.Equal([]types.NodeAddress{0x01}, e.Snapshots.Nodes())
	requireT.Empty(e.DeallocationLists.Nodes())

	requireT.Equal([]int{0, 1, 2}, test.CollectSpaceValues(s))

	// Insert 3

	requireT.NoError(s.Set(3, 3))
	nodesUsed, nodesAllocated, nodesDeallocated = e.Allocator.Nodes()

	requireT.Equal([]types.NodeAddress{0x01, 0x02}, nodesUsed)
	requireT.Empty(nodesAllocated)
	requireT.Empty(nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x02}, s.Nodes())
	requireT.Equal([]types.NodeAddress{0x01}, e.Snapshots.Nodes())
	requireT.Empty(e.DeallocationLists.Nodes())

	requireT.Equal([]int{0, 1, 2, 3}, test.CollectSpaceValues(s))

	// Insert 4

	for i := 4; i < 17; i++ {
		requireT.NoError(s.Set(i, i))
	}
	nodesUsed, nodesAllocated, nodesDeallocated = e.Allocator.Nodes()

	requireT.Equal([]types.NodeAddress{
		0x01, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
	}, nodesUsed)
	requireT.Equal([]types.NodeAddress{
		0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
	}, nodesAllocated)
	requireT.Equal([]types.NodeAddress{0x02}, nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f},
		s.Nodes())
	requireT.Equal([]types.NodeAddress{0x01}, e.Snapshots.Nodes())
	requireT.Empty(e.DeallocationLists.Nodes())

	requireT.Equal([]int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, test.CollectSpaceValues(s))
}

func TestSet(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT, false)
	s := e.NextSnapshot(requireT)

	items := make([]int, 0, 1000)
	for i := range cap(items) {
		items = append(items, i)
		requireT.NoError(s.Set(i, i))
	}

	requireT.Equal(items, test.CollectSpaceValues(s))
}

func TestGet(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT, false)
	s := e.NextSnapshot(requireT)

	for i := range 1000 {
		requireT.NoError(s.Set(i, i))
	}
	for i := range 1000 {
		v, exists := s.Get(i)
		requireT.True(exists)
		requireT.Equal(i, v)
	}

	v, exists := s.Get(1001)
	requireT.False(exists)
	requireT.Equal(0, v)
}

func TestDelete(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT, false)
	s := e.NextSnapshot(requireT)

	for i := range 1000 {
		requireT.NoError(s.Set(i, i))
	}

	deleted := make([]int, 0, 100)
	deleted2 := make([]int, 0, 100)
	for i := 100; i < 200; i++ {
		deleted = append(deleted, i)
		requireT.NoError(s.Delete(i))
	}

	// delete non-existing items
	for i := 1000; i < 2000; i++ {
		requireT.NoError(s.Delete(i))
	}

	for i := range 1000 {
		v, exists := s.Get(i)
		if exists {
			requireT.Equal(i, v)
		} else {
			deleted2 = append(deleted2, i)
		}
	}

	requireT.Equal(deleted, deleted2)
}

func TestSetOnNext(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT, false)

	s1 := e.NextSnapshot(requireT)

	for i := range 10 {
		requireT.NoError(s1.Set(i, i))
	}

	s2 := e.NextSnapshot(requireT)

	for i := range 5 {
		requireT.NoError(s2.Set(i, i+10))
	}

	requireT.Equal([]int{5, 6, 7, 8, 9, 10, 11, 12, 13, 14}, test.CollectSpaceValues(s2))
}

func TestReplace(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT, false)

	s1 := e.NextSnapshot(requireT)

	for i := range 10 {
		requireT.NoError(s1.Set(i, i))
	}

	s2 := e.NextSnapshot(requireT)

	for i, j := 0, 10; i < 5; i, j = i+1, j+1 {
		requireT.NoError(s2.Set(i, j))
	}

	for i := range 5 {
		v, exists := s2.Get(i)
		requireT.True(exists)
		requireT.Equal(i+10, v)
	}

	for i := 5; i < 10; i++ {
		v, exists := s2.Get(i)
		requireT.True(exists)
		requireT.Equal(i, v)
	}
}

func TestCopyOnSet(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT, false)

	s0 := e.NextSnapshot(requireT)

	for i := range 5 {
		requireT.NoError(s0.Set(i, i))
	}

	nodesUsed, nodesAllocated, nodesDeallocated := e.Allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x01, 0x02}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x01, 0x02}, nodesAllocated)
	requireT.Empty(nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x02}, s0.Nodes())
	requireT.Equal([]types.NodeAddress{0x01}, e.Snapshots.Nodes())
	requireT.Empty(e.DeallocationLists.Nodes())

	s1 := e.NextSnapshot(requireT)

	for i := range 5 {
		requireT.NoError(s1.Set(i, i+10))
	}

	nodesUsed, nodesAllocated, nodesDeallocated = e.Allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x02, 0x03, 0x04, 0x05, 0x06}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x03, 0x04, 0x05, 0x06}, nodesAllocated)
	requireT.Equal([]types.NodeAddress{0x01}, nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x02}, s0.Nodes())
	requireT.Equal([]types.NodeAddress{0x04}, s1.Nodes())
	requireT.Equal([]types.NodeAddress{0x03}, e.Snapshots.Nodes())

	requireT.Equal([]types.SnapshotID{0x00}, test.CollectSpaceKeys(e.DeallocationLists))
	requireT.Equal([]types.NodeAddress{0x06}, e.DeallocationLists.Nodes())

	dList0 := e.DeallocationList(0x00)
	requireT.Equal([]types.NodeAddress{0x05}, dList0.Nodes())
	requireT.Equal([]types.NodeAddress{0x02}, test.CollectListItems(dList0))

	s2 := e.NextSnapshot(requireT)

	for i := range 5 {
		requireT.NoError(s2.Set(i, i+20))
	}

	nodesUsed, nodesAllocated, nodesDeallocated = e.Allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x02, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x07, 0x08, 0x09, 0x0a}, nodesAllocated)
	requireT.Equal([]types.NodeAddress{0x03}, nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x02}, s0.Nodes())
	requireT.Equal([]types.NodeAddress{0x04}, s1.Nodes())
	requireT.Equal([]types.NodeAddress{0x08}, s2.Nodes())
	requireT.Equal([]types.NodeAddress{0x07}, e.Snapshots.Nodes())

	requireT.Equal([]types.SnapshotID{0x01}, test.CollectSpaceKeys(e.DeallocationLists))
	requireT.Equal([]types.NodeAddress{0x0a}, e.DeallocationLists.Nodes())

	dList1 := e.DeallocationList(0x01)
	requireT.Equal([]types.NodeAddress{0x09}, dList1.Nodes())
	requireT.Equal([]types.NodeAddress{0x04}, test.CollectListItems(dList1))

	// Partial copy

	s3 := e.NextSnapshot(requireT)

	for i := range 2 {
		requireT.NoError(s3.Set(i, i+30))
	}

	nodesUsed, nodesAllocated, nodesDeallocated = e.Allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x02, 0x04, 0x05, 0x06, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x0b, 0x0c, 0x0d, 0x0e}, nodesAllocated)
	requireT.Equal([]types.NodeAddress{0x07}, nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x02}, s0.Nodes())
	requireT.Equal([]types.NodeAddress{0x04}, s1.Nodes())
	requireT.Equal([]types.NodeAddress{0x08}, s2.Nodes())
	requireT.Equal([]types.NodeAddress{0x0c}, s3.Nodes())
	requireT.Equal([]types.NodeAddress{0x0b}, e.Snapshots.Nodes())

	requireT.Equal([]types.SnapshotID{0x02}, test.CollectSpaceKeys(e.DeallocationLists))
	requireT.Equal([]types.NodeAddress{0x0e}, e.DeallocationLists.Nodes())

	dList2 := e.DeallocationList(0x02)
	requireT.Equal([]types.NodeAddress{0x0d}, dList2.Nodes())
	requireT.Equal([]types.NodeAddress{0x08}, test.CollectListItems(dList2))

	// Overwrite everything to create two deallocation lists.

	s4 := e.NextSnapshot(requireT)

	for i := range 5 {
		requireT.NoError(s4.Set(i, i+40))
	}

	nodesUsed, nodesAllocated, nodesDeallocated = e.Allocator.Nodes()
	requireT.Equal([]types.NodeAddress{
		0x02, 0x04, 0x05, 0x06, 0x08, 0x09, 0x0a, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16,
	}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16}, nodesAllocated)
	requireT.Equal([]types.NodeAddress{0x0b}, nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x02}, s0.Nodes())
	requireT.Equal([]types.NodeAddress{0x04}, s1.Nodes())
	requireT.Equal([]types.NodeAddress{0x08}, s2.Nodes())
	requireT.Equal([]types.NodeAddress{0x0c}, s3.Nodes())
	requireT.Equal([]types.NodeAddress{0x14}, s4.Nodes())
	requireT.Equal([]types.NodeAddress{0x0f, 0x10, 0x11, 0x12, 0x13}, e.Snapshots.Nodes())

	requireT.Equal([]types.SnapshotID{0x03}, test.CollectSpaceKeys(e.DeallocationLists))
	requireT.Equal([]types.NodeAddress{0x16}, e.DeallocationLists.Nodes())

	dList3 := e.DeallocationList(0x03)
	requireT.Equal([]types.NodeAddress{0x15}, dList3.Nodes())
	requireT.Equal([]types.NodeAddress{0x0c}, test.CollectListItems(dList3))

	// Check all the values again

	requireT.Equal([]int{40, 41, 42, 43, 44}, test.CollectSpaceValues(s4))
}

func TestCopyOnDelete(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT, false)

	s0 := e.NextSnapshot(requireT)

	for i := range 5 {
		requireT.NoError(s0.Set(i, i))
	}

	nodesUsed, nodesAllocated, nodesDeallocated := e.Allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x01, 0x02}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x01, 0x02}, nodesAllocated)
	requireT.Empty(nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x02}, s0.Nodes())
	requireT.Equal([]types.NodeAddress{0x01}, e.Snapshots.Nodes())
	requireT.Empty(e.DeallocationLists.Nodes())

	s1 := e.NextSnapshot(requireT)

	requireT.NoError(s1.Delete(2))

	nodesUsed, nodesAllocated, nodesDeallocated = e.Allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x02, 0x03, 0x04, 0x05, 0x06}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x03, 0x04, 0x05, 0x06}, nodesAllocated)
	requireT.Equal([]types.NodeAddress{0x01}, nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x02}, s0.Nodes())
	requireT.Equal([]types.NodeAddress{0x04}, s1.Nodes())
	requireT.Equal([]types.NodeAddress{0x03}, e.Snapshots.Nodes())

	requireT.Equal([]types.SnapshotID{0x00}, test.CollectSpaceKeys(e.DeallocationLists))
	requireT.Equal([]types.NodeAddress{0x06}, e.DeallocationLists.Nodes())

	dList0 := e.DeallocationList(0x00)
	requireT.Equal([]types.NodeAddress{0x05}, dList0.Nodes())
	requireT.Equal([]types.NodeAddress{0x02}, test.CollectListItems(dList0))

	v, exists := s1.Get(2)
	requireT.False(exists)
	requireT.Equal(0, v)
	requireT.Equal([]int{0, 1, 3, 4}, test.CollectSpaceValues(s1))

	s2 := e.NextSnapshot(requireT)
	requireT.NoError(s2.Delete(4))

	nodesUsed, nodesAllocated, nodesDeallocated = e.Allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x02, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x07, 0x08, 0x09, 0x0a}, nodesAllocated)
	requireT.Equal([]types.NodeAddress{0x03}, nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x02}, s0.Nodes())
	requireT.Equal([]types.NodeAddress{0x04}, s1.Nodes())
	requireT.Equal([]types.NodeAddress{0x08}, s2.Nodes())
	requireT.Equal([]types.NodeAddress{0x07}, e.Snapshots.Nodes())

	requireT.Equal([]types.SnapshotID{0x01}, test.CollectSpaceKeys(e.DeallocationLists))
	requireT.Equal([]types.NodeAddress{0x0a}, e.DeallocationLists.Nodes())

	dList1 := e.DeallocationList(0x01)
	requireT.Equal([]types.NodeAddress{0x09}, dList1.Nodes())
	requireT.Equal([]types.NodeAddress{0x04}, test.CollectListItems(dList1))

	v, exists = s2.Get(2)
	requireT.False(exists)
	requireT.Equal(0, v)

	v, exists = s2.Get(4)
	requireT.False(exists)
	requireT.Equal(0, v)

	requireT.Equal([]int{0, 1, 3}, test.CollectSpaceValues(s2))
}

func TestDeallocateFromNonExistingSnapshot(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT, false)

	s0 := e.NextSnapshot(requireT)
	requireT.NoError(s0.Set(0, 0))

	nodesUsed, nodesAllocated, nodesDeallocated := e.Allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x01, 0x02}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x01, 0x02}, nodesAllocated)
	requireT.Empty(nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x02}, s0.Nodes())
	requireT.Equal([]types.NodeAddress{0x01}, e.Snapshots.Nodes())

	s1 := e.NextSnapshot(requireT)
	requireT.NoError(e.Snapshots.Delete(0x00))
	requireT.NoError(s1.Set(0, 10))

	nodesUsed, nodesAllocated, nodesDeallocated = e.Allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x03, 0x04}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x03, 0x04}, nodesAllocated)
	requireT.Equal([]types.NodeAddress{0x01, 0x02}, nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x04}, s1.Nodes())
	requireT.Equal([]types.NodeAddress{0x03}, e.Snapshots.Nodes())
}

func TestSetCollisions(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT, false)
	s := e.NextSnapshot(requireT)

	allValues := make([]int, 0, len(collisions)*len(collisions[0]))

	for _, set := range collisions {
		for _, i := range set {
			allValues = append(allValues, i)
			requireT.NoError(s.Set(i, i))
		}
	}

	sort.Ints(allValues)

	nodesUsed, _, _ := e.Allocator.Nodes()
	requireT.Equal([]types.NodeAddress{
		0x01, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12,
	}, nodesUsed)
	requireT.Equal(allValues, test.CollectSpaceValues(s))
}

func TestGetCollisions(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT, false)
	s := e.NextSnapshot(requireT)

	inserted := make([]int, 0, len(collisions)*len(collisions[0]))
	read := make([]int, 0, len(collisions)*len(collisions[0]))

	for _, set := range collisions {
		for _, i := range set {
			inserted = append(inserted, i)
			requireT.NoError(s.Set(i, i))
		}
	}

	for _, set := range collisions {
		for _, i := range set {
			if v, exists := s.Get(i); exists {
				read = append(read, v)
			}
		}
	}

	sort.Ints(inserted)
	sort.Ints(read)

	requireT.Equal(inserted, read)
}

func TestDeallocateAll(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT, true)
	s0 := e.NextSnapshot(requireT)

	for i := range 5 {
		requireT.NoError(s0.Set(i, i))
	}

	requireT.Equal([]types.NodeAddress{0x02}, s0.Nodes())

	s1 := e.NextSnapshot(requireT)
	requireT.NoError(s1.Set(0, 10))
	s1Nodes := s1.Nodes()
	requireT.Equal([]types.NodeAddress{0x04}, s1Nodes)

	s2 := e.NextSnapshot(requireT)
	requireT.Equal(s1Nodes, s2.Nodes())
	e.Allocator.Nodes() // to clean collected data

	requireT.NoError(s2.DeallocateAll())
	_, _, deallocatedNodes := e.Allocator.Nodes()
	requireT.Equal(s1Nodes, deallocatedNodes)
}

func newEnv(requireT *require.Assertions, immediateAallocator bool) *env {
	allocator := test.NewAllocator(test.AllocatorConfig{
		TotalSize: 1024 * 1024,
		NodeSize:  512,
	})

	pointerNodeAllocator, err := space.NewNodeAllocator[types.Pointer](allocator)
	requireT.NoError(err)

	dataNodeAllocator, err := space.NewNodeAllocator[types.DataItem[int, int]](allocator)
	requireT.NoError(err)

	snapshotInfoNodeAllocator, err := space.NewNodeAllocator[types.DataItem[types.SnapshotID, types.SnapshotInfo]](
		allocator,
	)
	requireT.NoError(err)

	snapshotToNodeNodeAllocator, err := space.NewNodeAllocator[types.DataItem[types.SnapshotID, types.NodeAddress]](
		allocator,
	)
	requireT.NoError(err)

	listNodeAllocator, err := list.NewNodeAllocator(allocator)
	requireT.NoError(err)

	return &env{
		Allocator:           allocator,
		immediateAallocator: immediateAallocator,
		snapshotRoot: types.ParentInfo{
			State:   lo.ToPtr(types.StateFree),
			Pointer: &types.Pointer{},
		},
		spaceRoot: types.ParentInfo{
			State:   lo.ToPtr(types.StateFree),
			Pointer: &types.Pointer{},
		},
		deallocationRoot: types.ParentInfo{
			State:   lo.ToPtr(types.StateFree),
			Pointer: &types.Pointer{},
		},
		snapshotHashMod:             lo.ToPtr[uint64](0),
		spaceHashMod:                lo.ToPtr[uint64](0),
		pointerNodeAllocator:        pointerNodeAllocator,
		dataNodeAllocator:           dataNodeAllocator,
		snapshotInfoNodeAllocator:   snapshotInfoNodeAllocator,
		snapshotToNodeNodeAllocator: snapshotToNodeNodeAllocator,
		listNodeAllocator:           listNodeAllocator,
	}
}

type env struct {
	Allocator         *test.Allocator
	Snapshots         *space.Space[types.SnapshotID, types.SnapshotInfo]
	DeallocationLists *space.Space[types.SnapshotID, types.NodeAddress]

	immediateAallocator         bool
	snapshotID                  types.SnapshotID
	snapshotAllocator           types.SnapshotAllocator
	snapshotRoot                types.ParentInfo
	spaceRoot                   types.ParentInfo
	deallocationRoot            types.ParentInfo
	snapshotHashMod             *uint64
	spaceHashMod                *uint64
	pointerNodeAllocator        space.NodeAllocator[types.Pointer]
	dataNodeAllocator           space.NodeAllocator[types.DataItem[int, int]]
	snapshotInfoNodeAllocator   space.NodeAllocator[types.DataItem[types.SnapshotID, types.SnapshotInfo]]
	snapshotToNodeNodeAllocator space.NodeAllocator[types.DataItem[types.SnapshotID, types.NodeAddress]]
	listNodeAllocator           list.NodeAllocator
}

func (e *env) NextSnapshot(requireT *require.Assertions) *space.Space[int, int] {
	snapshotID := e.snapshotID
	e.snapshotID++

	stateCopy := *e.spaceRoot.State
	pointerCopy := *e.spaceRoot.Pointer

	e.spaceRoot = types.ParentInfo{
		State:   &stateCopy,
		Pointer: &pointerCopy,
	}

	e.deallocationRoot = types.ParentInfo{
		State:   lo.ToPtr(types.StateFree),
		Pointer: &types.Pointer{},
	}

	e.Snapshots = &space.Space[types.SnapshotID, types.SnapshotInfo]{}
	e.DeallocationLists = &space.Space[types.SnapshotID, types.NodeAddress]{}
	e.snapshotAllocator = alloc.NewSnapshotAllocator(
		snapshotID,
		e.Allocator,
		e.Snapshots,
		e.DeallocationLists,
		e.listNodeAllocator,
	)
	if e.immediateAallocator {
		e.snapshotAllocator = alloc.NewImmediateSnapshotAllocator(snapshotID, e.snapshotAllocator)
	}
	*e.Snapshots = *space.New[types.SnapshotID, types.SnapshotInfo](space.Config[types.SnapshotID, types.SnapshotInfo]{
		SnapshotID:           snapshotID,
		HashMod:              e.snapshotHashMod,
		SpaceRoot:            e.snapshotRoot,
		PointerNodeAllocator: e.pointerNodeAllocator,
		DataNodeAllocator:    e.snapshotInfoNodeAllocator,
		Allocator:            alloc.NewImmediateSnapshotAllocator(snapshotID, e.snapshotAllocator),
	})
	*e.DeallocationLists = *space.New[types.SnapshotID, types.NodeAddress](
		space.Config[types.SnapshotID, types.NodeAddress]{
			SnapshotID:           snapshotID,
			HashMod:              lo.ToPtr[uint64](0),
			SpaceRoot:            e.deallocationRoot,
			PointerNodeAllocator: e.pointerNodeAllocator,
			DataNodeAllocator:    e.snapshotToNodeNodeAllocator,
			Allocator:            alloc.NewImmediateSnapshotAllocator(snapshotID, e.snapshotAllocator),
		},
	)

	requireT.NoError(e.Snapshots.Set(snapshotID, types.SnapshotInfo{}))

	return space.New[int, int](space.Config[int, int]{
		SnapshotID:           snapshotID,
		HashMod:              e.spaceHashMod,
		SpaceRoot:            e.spaceRoot,
		PointerNodeAllocator: e.pointerNodeAllocator,
		DataNodeAllocator:    e.dataNodeAllocator,
		Allocator:            e.snapshotAllocator,
	})
}

func (e *env) DeallocationList(snapshotID types.SnapshotID) *list.List {
	nodeAddress, exists := e.DeallocationLists.Get(snapshotID)
	if !exists {
		return nil
	}

	return list.New(list.Config{
		SnapshotID:    snapshotID,
		Item:          &nodeAddress,
		NodeAllocator: e.listNodeAllocator,
		Allocator:     e.snapshotAllocator,
	})
}

var collisions = [][]int{
	{15691551, 62234586, 76498628, 79645586},
	{6417226, 8828927, 78061179, 87384387},
	{9379853, 15271236, 26924827, 39742852},
	{71180670, 73568605, 96077640, 100118418},
	{11317952, 69053141, 82160848, 112455075},
	{33680651, 34881710, 52672514, 56033413},
	{635351, 7564491, 43998577, 77923294},
	{15069177, 60348274, 84185567, 116299206},
	{43622549, 93531002, 108158183, 115087013},
	{32134280, 33645087, 37005304, 83416269},
}
