package space_test

import (
	"sort"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/list"
	"github.com/outofforest/quantum/space"
	"github.com/outofforest/quantum/types"
)

func TestSetOneItem(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT)

	s := e.NextSnapshot()
	requireT.NoError(s.Set(0, 0))

	nodesAccessed, nodesAllocated, nodesDeallocated := e.Allocator.Nodes()

	requireT.Empty(nodesAccessed)
	requireT.Equal([]types.NodeAddress{0x01}, nodesAllocated)
	requireT.Empty(nodesDeallocated)

	requireT.Equal([]int{0}, collect(s))
}

func TestSetTwoItems(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT)

	s := e.NextSnapshot()
	requireT.NoError(s.Set(0, 0))
	requireT.NoError(s.Set(1, 1))

	nodesAccessed, nodesAllocated, nodesDeallocated := e.Allocator.Nodes()

	requireT.Equal([]types.NodeAddress{0x01}, nodesAccessed)
	requireT.Equal([]types.NodeAddress{0x01}, nodesAllocated)
	requireT.Empty(nodesDeallocated)

	requireT.Equal([]int{0, 1}, collect(s))
}

func TestSetWithPointerNode(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT)
	s := e.NextSnapshot()

	// Insert 0

	requireT.NoError(s.Set(0, 0))
	nodesAccessed, nodesAllocated, nodesDeallocated := e.Allocator.Nodes()

	requireT.Empty(nodesAccessed)
	requireT.Equal([]types.NodeAddress{0x01}, nodesAllocated)
	requireT.Empty(nodesDeallocated)

	requireT.Equal([]int{0}, collect(s))

	// Insert 1

	requireT.NoError(s.Set(1, 1))
	nodesAccessed, nodesAllocated, nodesDeallocated = e.Allocator.Nodes()

	requireT.Equal([]types.NodeAddress{0x01}, nodesAccessed)
	requireT.Empty(nodesAllocated)
	requireT.Empty(nodesDeallocated)

	requireT.Equal([]int{0, 1}, collect(s))

	// Insert 2

	requireT.NoError(s.Set(2, 2))
	nodesAccessed, nodesAllocated, nodesDeallocated = e.Allocator.Nodes()

	requireT.Equal([]types.NodeAddress{0x01}, nodesAccessed)
	requireT.Empty(nodesAllocated)
	requireT.Empty(nodesDeallocated)

	requireT.Equal([]int{0, 1, 2}, collect(s))

	// Insert 3

	requireT.NoError(s.Set(3, 3))
	nodesAccessed, nodesAllocated, nodesDeallocated = e.Allocator.Nodes()

	requireT.Equal([]types.NodeAddress{0x01}, nodesAccessed)
	requireT.Empty(nodesAllocated)
	requireT.Empty(nodesDeallocated)

	requireT.Equal([]int{0, 1, 2, 3}, collect(s))

	// Insert 4

	requireT.NoError(s.Set(4, 4))
	nodesAccessed, nodesAllocated, nodesDeallocated = e.Allocator.Nodes()

	requireT.Equal([]types.NodeAddress{0x01, 0x02}, nodesAccessed)
	requireT.Equal([]types.NodeAddress{0x02, 0x03, 0x04, 0x05, 0x06, 0x07}, nodesAllocated)
	requireT.Equal([]types.NodeAddress{0x01}, nodesDeallocated)

	requireT.Equal([]int{0, 1, 2, 3, 4}, collect(s))
}

func TestSet(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT)
	s := e.NextSnapshot()

	items := make([]int, 0, 1000)
	for i := range cap(items) {
		items = append(items, i)
		requireT.NoError(s.Set(i, i))
	}

	requireT.Equal(items, collect(s))
}

func TestGet(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT)
	s := e.NextSnapshot()

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
	e := newEnv(requireT)
	s := e.NextSnapshot()

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
	e := newEnv(requireT)

	s1 := e.NextSnapshot()

	for i := range 10 {
		requireT.NoError(s1.Set(i, i))
	}

	s2 := e.NextSnapshot()

	for i := range 5 {
		requireT.NoError(s2.Set(i, i+10))
	}

	requireT.Equal([]int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, collect(s1))
	requireT.Equal([]int{5, 6, 7, 8, 9, 10, 11, 12, 13, 14}, collect(s2))
}

func TestReplace(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT)

	s1 := e.NextSnapshot()

	for i := range 10 {
		requireT.NoError(s1.Set(i, i))
	}

	s2 := e.NextSnapshot()

	for i, j := 0, 10; i < 5; i, j = i+1, j+1 {
		requireT.NoError(s2.Set(i, j))
	}

	for i := range 10 {
		v, exists := s1.Get(i)
		requireT.True(exists)
		requireT.Equal(i, v)
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
	e := newEnv(requireT)

	s1 := e.NextSnapshot()

	for i := range 5 {
		requireT.NoError(s1.Set(i, i))
	}

	nodesAccessed1, nodesAllocated1, nodesDeallocated1 := e.Allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x01, 0x02}, nodesAccessed1)
	requireT.Equal([]types.NodeAddress{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}, nodesAllocated1)
	requireT.Equal([]types.NodeAddress{0x01}, nodesDeallocated1)

	s2 := e.NextSnapshot()

	for i := range 5 {
		requireT.NoError(s2.Set(i, i+10))
	}

	nodesAccessed2, nodesAllocated2, nodesDeallocated2 := e.Allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a}, nodesAccessed2)
	requireT.Equal([]types.NodeAddress{0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f}, nodesAllocated2)
	requireT.Empty(nodesDeallocated2)

	s3 := e.NextSnapshot()

	for i := range 5 {
		requireT.NoError(s3.Set(i, i+20))
	}

	nodesAccessed3, nodesAllocated3, nodesDeallocated3 := e.Allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x08, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12}, nodesAccessed3)
	requireT.Equal([]types.NodeAddress{0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17}, nodesAllocated3)
	requireT.Empty(nodesDeallocated3)
}

func TestCopyOnDelete(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT)

	s1 := e.NextSnapshot()

	for i := range 5 {
		requireT.NoError(s1.Set(i, i))
	}

	nodesAccessed1, nodesAllocated1, nodesDeallocated1 := e.Allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x01, 0x02}, nodesAccessed1)
	requireT.Equal([]types.NodeAddress{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}, nodesAllocated1)
	requireT.Equal([]types.NodeAddress{0x01}, nodesDeallocated1)

	s2 := e.NextSnapshot()
	requireT.NoError(s2.Delete(2))

	nodesAccessed2, nodesAllocated2, nodesDeallocated2 := e.Allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x02, 0x03, 0x09, 0x0a}, nodesAccessed2)
	requireT.Equal([]types.NodeAddress{0x08, 0x09, 0x0a, 0x0b}, nodesAllocated2)
	requireT.Empty(nodesDeallocated2)

	s3 := e.NextSnapshot()
	requireT.NoError(s3.Delete(4))

	nodesAccessed3, nodesAllocated3, nodesDeallocated3 := e.Allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x07, 0x08, 0x0e}, nodesAccessed3)
	requireT.Equal([]types.NodeAddress{0x0c, 0x0d, 0x0e, 0x0f, 0x10}, nodesAllocated3)
	requireT.Empty(nodesDeallocated3)
}

func TestSetCollisions(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT)
	s := e.NextSnapshot()

	allValues := make([]int, 0, len(collisions)*len(collisions[0]))

	for _, set := range collisions {
		for _, i := range set {
			allValues = append(allValues, i)
			requireT.NoError(s.Set(i, i))
		}
	}

	sort.Ints(allValues)

	requireT.Equal(allValues, collect(s))
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

func newEnv(requireT *require.Assertions) *env {
	allocator := alloc.NewTestAllocator(alloc.NewAllocator(alloc.Config{
		TotalSize: 1024 * 1024,
		NodeSize:  512,
	}))

	pointerNodeAllocator, err := space.NewNodeAllocator[types.NodeAddress](allocator)
	requireT.NoError(err)

	dataNodeAllocator, err := space.NewNodeAllocator[types.DataItem[int, int]](allocator)
	requireT.NoError(err)

	snapshotToNodeNodeAllocator, err := space.NewNodeAllocator[types.DataItem[types.SnapshotID, types.NodeAddress]](
		allocator,
	)
	requireT.NoError(err)

	listNodeAllocator, err := list.NewNodeAllocator(allocator)
	requireT.NoError(err)

	return &env{
		Allocator: allocator,
		SpaceRoot: types.ParentInfo{
			State: lo.ToPtr(types.StateFree),
			Item:  lo.ToPtr[types.NodeAddress](0),
		},
		DeallocationRoot: types.ParentInfo{
			State: lo.ToPtr(types.StateFree),
			Item:  lo.ToPtr[types.NodeAddress](0),
		},
		hashMod:                     lo.ToPtr[uint64](0),
		pointerNodeAllocator:        pointerNodeAllocator,
		dataNodeAllocator:           dataNodeAllocator,
		snapshotToNodeNodeAllocator: snapshotToNodeNodeAllocator,
		listNodeAllocator:           listNodeAllocator,
	}
}

type env struct {
	Allocator        *alloc.TestAllocator
	SpaceRoot        types.ParentInfo
	DeallocationRoot types.ParentInfo

	snapshotID                  types.SnapshotID
	hashMod                     *uint64
	pointerNodeAllocator        space.NodeAllocator[types.NodeAddress]
	dataNodeAllocator           space.NodeAllocator[types.DataItem[int, int]]
	snapshotToNodeNodeAllocator space.NodeAllocator[types.DataItem[types.SnapshotID, types.NodeAddress]]
	listNodeAllocator           list.NodeAllocator
}

func (e *env) NextSnapshot() *space.Space[int, int] {
	snapshotID := e.snapshotID
	e.snapshotID++

	stateCopy := *e.SpaceRoot.State
	itemCopy := *e.SpaceRoot.Item

	e.SpaceRoot = types.ParentInfo{
		State: &stateCopy,
		Item:  &itemCopy,
	}

	e.DeallocationRoot = types.ParentInfo{
		State: lo.ToPtr(types.StateFree),
		Item:  lo.ToPtr[types.NodeAddress](0),
	}

	deallocationLists := &space.Space[types.SnapshotID, types.NodeAddress]{}
	snapshotAllocator := alloc.NewSnapshotAllocator(
		snapshotID,
		e.Allocator,
		deallocationLists,
		e.listNodeAllocator,
	)
	*deallocationLists = *space.New[types.SnapshotID, types.NodeAddress](space.Config[types.SnapshotID, types.NodeAddress]{
		SnapshotID:           snapshotID,
		HashMod:              lo.ToPtr[uint64](0),
		SpaceRoot:            e.DeallocationRoot,
		PointerNodeAllocator: e.pointerNodeAllocator,
		DataNodeAllocator:    e.snapshotToNodeNodeAllocator,
		Allocator:            snapshotAllocator,
	})

	return space.New[int, int](space.Config[int, int]{
		SnapshotID:           snapshotID,
		HashMod:              e.hashMod,
		SpaceRoot:            e.SpaceRoot,
		PointerNodeAllocator: e.pointerNodeAllocator,
		DataNodeAllocator:    e.dataNodeAllocator,
		Allocator:            snapshotAllocator,
	})
}

func collect(s *space.Space[int, int]) []int {
	values := []int{}
	for item := range s.Iterator() {
		values = append(values, item.Value)
	}

	sort.Ints(values)
	return values
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
