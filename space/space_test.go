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
	e := newEnv(requireT)

	s := e.NextSnapshot()
	requireT.NoError(test.Error(s.Get(0).Set(0)))

	nodesUsed, nodesAllocated, nodesDeallocated := e.Allocator.Nodes()

	requireT.Equal([]types.NodeAddress{0x01}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x01}, nodesAllocated)
	requireT.Empty(nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x01}, s.Nodes())

	requireT.Equal([]int{0}, test.CollectSpaceValues(s))
}

func TestSetTwoItems(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT)

	s := e.NextSnapshot()
	requireT.NoError(test.Error(s.Get(0).Set(0)))
	requireT.NoError(test.Error(s.Get(1).Set(1)))

	nodesUsed, nodesAllocated, nodesDeallocated := e.Allocator.Nodes()

	requireT.Equal([]types.NodeAddress{0x01}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x01}, nodesAllocated)
	requireT.Empty(nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x01}, s.Nodes())

	requireT.Equal([]int{0, 1}, test.CollectSpaceValues(s))
}

func TestSetWithPointerNode(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT)
	s := e.NextSnapshot()

	// Insert 0

	requireT.NoError(test.Error(s.Get(0).Set(0)))
	nodesUsed, nodesAllocated, nodesDeallocated := e.Allocator.Nodes()

	requireT.Equal([]types.NodeAddress{0x01}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x01}, nodesAllocated)
	requireT.Empty(nodesDeallocated)

	requireT.Equal([]int{0}, test.CollectSpaceValues(s))

	// Insert 1

	requireT.NoError(test.Error(s.Get(1).Set(1)))
	nodesUsed, nodesAllocated, nodesDeallocated = e.Allocator.Nodes()

	requireT.Equal([]types.NodeAddress{0x01}, nodesUsed)
	requireT.Empty(nodesAllocated)
	requireT.Empty(nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x01}, s.Nodes())

	requireT.Equal([]int{0, 1}, test.CollectSpaceValues(s))

	// Insert 2

	requireT.NoError(test.Error(s.Get(2).Set(2)))
	nodesUsed, nodesAllocated, nodesDeallocated = e.Allocator.Nodes()

	requireT.Equal([]types.NodeAddress{0x01}, nodesUsed)
	requireT.Empty(nodesAllocated)
	requireT.Empty(nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x01}, s.Nodes())

	requireT.Equal([]int{0, 1, 2}, test.CollectSpaceValues(s))

	// Insert 3

	requireT.NoError(test.Error(s.Get(3).Set(3)))
	nodesUsed, nodesAllocated, nodesDeallocated = e.Allocator.Nodes()

	requireT.Equal([]types.NodeAddress{0x01}, nodesUsed)
	requireT.Empty(nodesAllocated)
	requireT.Empty(nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x01}, s.Nodes())

	requireT.Equal([]int{0, 1, 2, 3}, test.CollectSpaceValues(s))

	// Insert 4

	for i := 4; i < 17; i++ {
		requireT.NoError(test.Error(s.Get(i).Set(i)))
	}
	nodesUsed, nodesAllocated, nodesDeallocated = e.Allocator.Nodes()

	requireT.Equal([]types.NodeAddress{
		0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
	}, nodesUsed)
	requireT.Equal([]types.NodeAddress{
		0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
	}, nodesAllocated)
	requireT.Equal([]types.NodeAddress{0x01}, nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e},
		s.Nodes())

	requireT.Equal([]int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, test.CollectSpaceValues(s))
}

func TestSet(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT)
	s := e.NextSnapshot()

	items := make([]int, 0, 1000)
	for i := range cap(items) {
		items = append(items, i)
		requireT.NoError(test.Error(s.Get(i).Set(i)))
	}

	requireT.Equal(items, test.CollectSpaceValues(s))
}

func TestGet(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT)
	s := e.NextSnapshot()

	for i := range 1000 {
		requireT.NoError(test.Error(s.Get(i).Set(i)))
	}
	for i := range 1000 {
		v := s.Get(i)
		requireT.True(v.Exists())
		requireT.Equal(i, v.Value())
	}

	v := s.Get(1001)
	requireT.False(v.Exists())
	requireT.Equal(0, v.Value())
}

func TestDelete(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT)
	s := e.NextSnapshot()

	for i := range 1000 {
		requireT.NoError(test.Error(s.Get(i).Set(i)))
	}

	deleted := make([]int, 0, 100)
	deleted2 := make([]int, 0, 100)
	for i := 100; i < 200; i++ {
		deleted = append(deleted, i)
		requireT.NoError(s.Get(i).Delete())
	}

	// delete non-existing items
	for i := 1000; i < 2000; i++ {
		requireT.NoError(s.Get(i).Delete())
	}

	for i := range 1000 {
		v := s.Get(i)
		if v.Exists() {
			requireT.Equal(i, v.Value())
		} else {
			deleted2 = append(deleted2, i)
		}
	}

	requireT.Equal(deleted, deleted2)
}

func TestSetOnNext(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT)

	s1 := e.NextSnapshot()

	for i := range 10 {
		requireT.NoError(test.Error(s1.Get(i).Set(i)))
	}

	s2 := e.NextSnapshot()

	for i := range 5 {
		requireT.NoError(test.Error(s2.Get(i).Set(i + 10)))
	}

	requireT.Equal([]int{5, 6, 7, 8, 9, 10, 11, 12, 13, 14}, test.CollectSpaceValues(s2))
}

func TestReplace(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT)

	s1 := e.NextSnapshot()

	for i := range 10 {
		requireT.NoError(test.Error(s1.Get(i).Set(i)))
	}

	s2 := e.NextSnapshot()

	for i, j := 0, 10; i < 5; i, j = i+1, j+1 {
		requireT.NoError(test.Error(s2.Get(i).Set(j)))
	}

	for i := range 5 {
		v := s2.Get(i)
		requireT.True(v.Exists())
		requireT.Equal(i+10, v.Value())
	}

	for i := 5; i < 10; i++ {
		v := s2.Get(i)
		requireT.True(v.Exists())
		requireT.Equal(i, v.Value())
	}
}

func TestCopyOnSet(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT)

	s0 := e.NextSnapshot()

	for i := range 5 {
		requireT.NoError(test.Error(s0.Get(i).Set(i)))
	}

	nodesUsed, nodesAllocated, nodesDeallocated := e.Allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x01}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x01}, nodesAllocated)
	requireT.Empty(nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x01}, s0.Nodes())

	s1 := e.NextSnapshot()

	for i := range 5 {
		requireT.NoError(test.Error(s1.Get(i).Set(i + 10)))
	}

	nodesUsed, nodesAllocated, nodesDeallocated = e.Allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x02}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x02}, nodesAllocated)
	requireT.Equal([]types.NodeAddress{0x01}, nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x02}, s1.Nodes())

	s2 := e.NextSnapshot()

	for i := range 5 {
		requireT.NoError(test.Error(s2.Get(i).Set(i + 20)))
	}

	nodesUsed, nodesAllocated, nodesDeallocated = e.Allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x03}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x03}, nodesAllocated)
	requireT.Equal([]types.NodeAddress{0x02}, nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x03}, s2.Nodes())

	// Partial copy

	s3 := e.NextSnapshot()

	for i := range 2 {
		requireT.NoError(test.Error(s3.Get(i).Set(i + 30)))
	}

	nodesUsed, nodesAllocated, nodesDeallocated = e.Allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x04}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x04}, nodesAllocated)
	requireT.Equal([]types.NodeAddress{0x03}, nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x04}, s3.Nodes())

	// Overwrite everything to create two deallocation lists.

	s4 := e.NextSnapshot()

	for i := range 5 {
		requireT.NoError(test.Error(s4.Get(i).Set(i + 40)))
	}

	nodesUsed, nodesAllocated, nodesDeallocated = e.Allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x05}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x05}, nodesAllocated)
	requireT.Equal([]types.NodeAddress{0x04}, nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x05}, s4.Nodes())

	// Check all the values again

	requireT.Equal([]int{40, 41, 42, 43, 44}, test.CollectSpaceValues(s4))
}

func TestCopyOnDelete(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT)

	s0 := e.NextSnapshot()

	for i := range 5 {
		requireT.NoError(test.Error(s0.Get(i).Set(i)))
	}

	nodesUsed, nodesAllocated, nodesDeallocated := e.Allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x01}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x01}, nodesAllocated)
	requireT.Empty(nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x01}, s0.Nodes())

	s1 := e.NextSnapshot()

	requireT.NoError(s1.Get(2).Delete())

	nodesUsed, nodesAllocated, nodesDeallocated = e.Allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x02}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x02}, nodesAllocated)
	requireT.Equal([]types.NodeAddress{0x01}, nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x02}, s1.Nodes())

	v := s1.Get(2)
	requireT.False(v.Exists())
	requireT.Equal(0, v.Value())
	requireT.Equal([]int{0, 1, 3, 4}, test.CollectSpaceValues(s1))

	s2 := e.NextSnapshot()
	requireT.NoError(s2.Get(4).Delete())

	nodesUsed, nodesAllocated, nodesDeallocated = e.Allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x03}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x03}, nodesAllocated)
	requireT.Equal([]types.NodeAddress{0x02}, nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x03}, s2.Nodes())

	v = s2.Get(2)
	requireT.False(v.Exists())
	requireT.Equal(0, v.Value())

	v = s2.Get(4)
	requireT.False(v.Exists())
	requireT.Equal(0, v.Value())

	requireT.Equal([]int{0, 1, 3}, test.CollectSpaceValues(s2))
}

func TestSetCollisions(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT)
	s := e.NextSnapshot()

	allValues := make([]int, 0, len(collisions)*len(collisions[0]))

	for _, set := range collisions {
		for _, i := range set {
			allValues = append(allValues, i)
			requireT.NoError(test.Error(s.Get(i).Set(i)))
		}
	}

	sort.Ints(allValues)

	nodesUsed, _, _ := e.Allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09}, nodesUsed)
	requireT.Equal(allValues, test.CollectSpaceValues(s))
}

func TestGetCollisions(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT)
	s := e.NextSnapshot()

	inserted := make([]int, 0, len(collisions)*len(collisions[0]))
	read := make([]int, 0, len(collisions)*len(collisions[0]))

	for _, set := range collisions {
		for _, i := range set {
			inserted = append(inserted, i)
			requireT.NoError(test.Error(s.Get(i).Set(i)))
		}
	}

	for _, set := range collisions {
		for _, i := range set {
			if v := s.Get(i); v.Exists() {
				read = append(read, v.Value())
			}
		}
	}

	sort.Ints(inserted)
	sort.Ints(read)

	requireT.Equal(inserted, read)
}

func TestDeallocateAll(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT)
	s0 := e.NextSnapshot()

	for i := range 5 {
		requireT.NoError(test.Error(s0.Get(i).Set(i)))
	}

	requireT.Equal([]types.NodeAddress{0x01}, s0.Nodes())

	s1 := e.NextSnapshot()
	requireT.NoError(test.Error(s1.Get(0).Set(10)))
	s1Nodes := s1.Nodes()
	requireT.Equal([]types.NodeAddress{0x02}, s1Nodes)

	s2 := e.NextSnapshot()
	requireT.Equal(s1Nodes, s2.Nodes())
	e.Allocator.Nodes() // to clean collected data

	requireT.NoError(s2.DeallocateAll())
	_, _, deallocatedNodes := e.Allocator.Nodes()
	requireT.Equal(s1Nodes, deallocatedNodes)
}

func newEnv(requireT *require.Assertions) *env {
	allocator := test.NewAllocator(test.AllocatorConfig{
		TotalSize: 1024 * 1024,
		NodeSize:  512,
	})

	pointerNodeAllocator, err := space.NewNodeAllocator[types.Pointer](allocator)
	requireT.NoError(err)

	dataNodeAllocator, err := space.NewNodeAllocator[types.DataItem[int, int]](allocator)
	requireT.NoError(err)

	return &env{
		Allocator: allocator,
		spaceRoot: types.ParentInfo{
			State:   lo.ToPtr(types.StateFree),
			Pointer: &types.Pointer{},
		},
		spaceHashMod:         lo.ToPtr[uint64](0),
		pointerNodeAllocator: pointerNodeAllocator,
		dataNodeAllocator:    dataNodeAllocator,
	}
}

type env struct {
	Allocator *test.Allocator

	snapshotID           types.SnapshotID
	spaceRoot            types.ParentInfo
	spaceHashMod         *uint64
	pointerNodeAllocator space.NodeAllocator[types.Pointer]
	dataNodeAllocator    space.NodeAllocator[types.DataItem[int, int]]
}

func (e *env) NextSnapshot() *space.Space[int, int] {
	snapshotID := e.snapshotID
	e.snapshotID++

	return space.New[int, int](space.Config[int, int]{
		SnapshotID:           snapshotID,
		HashMod:              e.spaceHashMod,
		SpaceRoot:            e.spaceRoot,
		PointerNodeAllocator: e.pointerNodeAllocator,
		DataNodeAllocator:    e.dataNodeAllocator,
		Allocator: alloc.NewImmediateSnapshotAllocator(snapshotID, alloc.NewSnapshotAllocator(
			snapshotID,
			e.Allocator,
			map[types.SnapshotID]alloc.ListToCommit{},
			map[types.SnapshotID]struct{}{},
			list.NodeAllocator{},
		)),
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
