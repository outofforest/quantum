package space_test

import (
	"sort"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/constraints"

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

	nodesAllocated, nodesDeallocated := e.Allocator.Nodes()

	requireT.Equal([]types.NodeAddress{0x01}, nodesAllocated)
	requireT.Empty(nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x01}, s.Nodes())
	requireT.Empty(e.DeallocationLists.Nodes())

	requireT.Equal([]int{0}, collectSpaceValues(s))
}

func TestSetTwoItems(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT)

	s := e.NextSnapshot()
	requireT.NoError(s.Set(0, 0))
	requireT.NoError(s.Set(1, 1))

	nodesAllocated, nodesDeallocated := e.Allocator.Nodes()

	requireT.Equal([]types.NodeAddress{0x01}, nodesAllocated)
	requireT.Empty(nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x01}, s.Nodes())
	requireT.Empty(e.DeallocationLists.Nodes())

	requireT.Equal([]int{0, 1}, collectSpaceValues(s))
}

func TestSetWithPointerNode(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT)
	s := e.NextSnapshot()

	// Insert 0

	requireT.NoError(s.Set(0, 0))
	nodesAllocated, nodesDeallocated := e.Allocator.Nodes()

	requireT.Equal([]types.NodeAddress{0x01}, nodesAllocated)
	requireT.Empty(nodesDeallocated)
	requireT.Empty(e.DeallocationLists.Nodes())

	requireT.Equal([]int{0}, collectSpaceValues(s))

	// Insert 1

	requireT.NoError(s.Set(1, 1))
	nodesAllocated, nodesDeallocated = e.Allocator.Nodes()

	requireT.Empty(nodesAllocated)
	requireT.Empty(nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x01}, s.Nodes())
	requireT.Empty(e.DeallocationLists.Nodes())

	requireT.Equal([]int{0, 1}, collectSpaceValues(s))

	// Insert 2

	requireT.NoError(s.Set(2, 2))
	nodesAllocated, nodesDeallocated = e.Allocator.Nodes()

	requireT.Empty(nodesAllocated)
	requireT.Empty(nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x01}, s.Nodes())
	requireT.Empty(e.DeallocationLists.Nodes())

	requireT.Equal([]int{0, 1, 2}, collectSpaceValues(s))

	// Insert 3

	requireT.NoError(s.Set(3, 3))
	nodesAllocated, nodesDeallocated = e.Allocator.Nodes()

	requireT.Empty(nodesAllocated)
	requireT.Empty(nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x01}, s.Nodes())
	requireT.Empty(e.DeallocationLists.Nodes())

	requireT.Equal([]int{0, 1, 2, 3}, collectSpaceValues(s))

	// Insert 4

	requireT.NoError(s.Set(4, 4))
	nodesAllocated, nodesDeallocated = e.Allocator.Nodes()

	requireT.Equal([]types.NodeAddress{0x02, 0x03, 0x04, 0x05, 0x06, 0x07}, nodesAllocated)
	requireT.Equal([]types.NodeAddress{0x01}, nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x02, 0x03, 0x04, 0x05, 0x06, 0x07}, s.Nodes())
	requireT.Empty(e.DeallocationLists.Nodes())

	requireT.Equal([]int{0, 1, 2, 3, 4}, collectSpaceValues(s))
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

	requireT.Equal(items, collectSpaceValues(s))
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

	requireT.Equal([]int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, collectSpaceValues(s1))
	requireT.Equal([]int{5, 6, 7, 8, 9, 10, 11, 12, 13, 14}, collectSpaceValues(s2))
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

	s0 := e.NextSnapshot()

	for i := range 5 {
		requireT.NoError(s0.Set(i, i))
	}

	nodesAllocated, nodesDeallocated := e.Allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}, nodesAllocated)
	requireT.Equal([]types.NodeAddress{0x01}, nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x02, 0x03, 0x04, 0x05, 0x06, 0x07}, s0.Nodes())
	requireT.Empty(e.DeallocationLists.Nodes())

	s1 := e.NextSnapshot()

	for i := range 5 {
		requireT.NoError(s1.Set(i, i+10))
	}

	nodesAllocated, nodesDeallocated = e.Allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f}, nodesAllocated)
	requireT.Empty(nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x02, 0x03, 0x04, 0x05, 0x06, 0x07}, s0.Nodes())
	requireT.Equal([]types.NodeAddress{0x08, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f}, s1.Nodes())

	requireT.Equal([]types.SnapshotID{0x00}, collectSpaceKeys(e.DeallocationLists))
	requireT.Equal([]types.NodeAddress{0x0a}, e.DeallocationLists.Nodes())

	dList0 := e.DeallocationList(0x00)
	requireT.Equal([]types.NodeAddress{0x09}, dList0.Nodes())
	requireT.Equal([]types.NodeAddress{0x02, 0x03, 0x04, 0x05, 0x06, 0x07}, collectListItems(dList0))

	s2 := e.NextSnapshot()

	for i := range 5 {
		requireT.NoError(s2.Set(i, i+20))
	}

	nodesAllocated, nodesDeallocated = e.Allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17}, nodesAllocated)
	requireT.Empty(nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x02, 0x03, 0x04, 0x05, 0x06, 0x07}, s0.Nodes())
	requireT.Equal([]types.NodeAddress{0x08, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f}, s1.Nodes())
	requireT.Equal([]types.NodeAddress{0x10, 0x13, 0x14, 0x15, 0x16, 0x17}, s2.Nodes())

	requireT.Equal([]types.SnapshotID{0x01}, collectSpaceKeys(e.DeallocationLists))
	requireT.Equal([]types.NodeAddress{0x12}, e.DeallocationLists.Nodes())

	dList1 := e.DeallocationList(0x01)
	requireT.Equal([]types.NodeAddress{0x11}, dList1.Nodes())
	requireT.Equal([]types.NodeAddress{0x08, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f}, collectListItems(dList1))

	// Partial copy

	s3 := e.NextSnapshot()

	for i := range 2 {
		requireT.NoError(s3.Set(i, i+30))
	}

	nodesAllocated, nodesDeallocated = e.Allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x18, 0x19, 0x1a, 0x1b, 0x1c}, nodesAllocated)
	requireT.Empty(nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x02, 0x03, 0x04, 0x05, 0x06, 0x07}, s0.Nodes())
	requireT.Equal([]types.NodeAddress{0x08, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f}, s1.Nodes())
	requireT.Equal([]types.NodeAddress{0x10, 0x13, 0x14, 0x15, 0x16, 0x17}, s2.Nodes())
	requireT.Equal([]types.NodeAddress{0x15, 0x16, 0x17, 0x18, 0x1b, 0x1c}, s3.Nodes())

	requireT.Equal([]types.SnapshotID{0x02}, collectSpaceKeys(e.DeallocationLists))
	requireT.Equal([]types.NodeAddress{0x1a}, e.DeallocationLists.Nodes())

	dList2 := e.DeallocationList(0x02)
	requireT.Equal([]types.NodeAddress{0x19}, dList2.Nodes())
	requireT.Equal([]types.NodeAddress{0x10, 0x13, 0x14}, collectListItems(dList2))

	// Overwrite everything to create two deallocation lists.

	s4 := e.NextSnapshot()

	for i := range 5 {
		requireT.NoError(s4.Set(i, i+40))
	}

	nodesAllocated, nodesDeallocated = e.Allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x1d, 0x1e, 0x1f, 0x20, 0x21, 0x22, 0x23, 0x24, 0x25}, nodesAllocated)
	requireT.Empty(nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x02, 0x03, 0x04, 0x05, 0x06, 0x07}, s0.Nodes())
	requireT.Equal([]types.NodeAddress{0x08, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f}, s1.Nodes())
	requireT.Equal([]types.NodeAddress{0x10, 0x13, 0x14, 0x15, 0x16, 0x17}, s2.Nodes())
	requireT.Equal([]types.NodeAddress{0x15, 0x16, 0x17, 0x18, 0x1b, 0x1c}, s3.Nodes())
	requireT.Equal([]types.NodeAddress{0x1d, 0x20, 0x21, 0x22, 0x24, 0x25}, s4.Nodes())

	requireT.Equal([]types.SnapshotID{0x02, 0x03}, collectSpaceKeys(e.DeallocationLists))
	requireT.Equal([]types.NodeAddress{0x1f}, e.DeallocationLists.Nodes())

	dList2 = e.DeallocationList(0x02)
	requireT.Equal([]types.NodeAddress{0x23}, dList2.Nodes())
	requireT.Equal([]types.NodeAddress{0x15, 0x16, 0x17}, collectListItems(dList2))

	dList3 := e.DeallocationList(0x03)
	requireT.Equal([]types.NodeAddress{0x1e}, dList3.Nodes())
	requireT.Equal([]types.NodeAddress{0x18, 0x1b, 0x1c}, collectListItems(dList3))

	// Check all the values again

	requireT.Equal([]int{0, 1, 2, 3, 4}, collectSpaceValues(s0))
	requireT.Equal([]int{10, 11, 12, 13, 14}, collectSpaceValues(s1))
	requireT.Equal([]int{20, 21, 22, 23, 24}, collectSpaceValues(s2))
	requireT.Equal([]int{22, 23, 24, 30, 31}, collectSpaceValues(s3))
	requireT.Equal([]int{40, 41, 42, 43, 44}, collectSpaceValues(s4))
}

func TestCopyOnDelete(t *testing.T) {
	requireT := require.New(t)
	e := newEnv(requireT)

	s0 := e.NextSnapshot()

	for i := range 5 {
		requireT.NoError(s0.Set(i, i))
	}

	nodesAllocated, nodesDeallocated := e.Allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}, nodesAllocated)
	requireT.Equal([]types.NodeAddress{0x01}, nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x02, 0x03, 0x04, 0x05, 0x06, 0x07}, s0.Nodes())
	requireT.Empty(e.DeallocationLists.Nodes())

	s1 := e.NextSnapshot()

	requireT.NoError(s1.Delete(2))

	nodesAllocated, nodesDeallocated = e.Allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x08, 0x09, 0x0a, 0x0b}, nodesAllocated)
	requireT.Empty(nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x02, 0x03, 0x04, 0x05, 0x06, 0x07}, s0.Nodes())
	requireT.Equal([]types.NodeAddress{0x04, 0x05, 0x06, 0x07, 0x08, 0x0b}, s1.Nodes())

	requireT.Equal([]types.SnapshotID{0x00}, collectSpaceKeys(e.DeallocationLists))
	requireT.Equal([]types.NodeAddress{0x0a}, e.DeallocationLists.Nodes())

	dList0 := e.DeallocationList(0x00)
	requireT.Equal([]types.NodeAddress{0x09}, dList0.Nodes())
	requireT.Equal([]types.NodeAddress{0x02, 0x03}, collectListItems(dList0))

	v, exists := s1.Get(2)
	requireT.False(exists)
	requireT.Equal(0, v)
	requireT.Equal([]int{0, 1, 3, 4}, collectSpaceValues(s1))

	s2 := e.NextSnapshot()
	requireT.NoError(s2.Delete(4))

	nodesAllocated, nodesDeallocated = e.Allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x0c, 0x0d, 0x0e, 0x0f, 0x10}, nodesAllocated)
	requireT.Empty(nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x02, 0x03, 0x04, 0x05, 0x06, 0x07}, s0.Nodes())
	requireT.Equal([]types.NodeAddress{0x04, 0x05, 0x06, 0x07, 0x08, 0x0b}, s1.Nodes())
	requireT.Equal([]types.NodeAddress{0x04, 0x05, 0x06, 0x0b, 0x0c, 0x0f}, s2.Nodes())

	requireT.Equal([]types.SnapshotID{0x00, 0x01}, collectSpaceKeys(e.DeallocationLists))
	requireT.Equal([]types.NodeAddress{0x0e}, e.DeallocationLists.Nodes())

	dList0 = e.DeallocationList(0x00)
	requireT.Equal([]types.NodeAddress{0x10}, dList0.Nodes())
	requireT.Equal([]types.NodeAddress{0x07}, collectListItems(dList0))

	dList1 := e.DeallocationList(0x01)
	requireT.Equal([]types.NodeAddress{0x0d}, dList1.Nodes())
	requireT.Equal([]types.NodeAddress{0x08}, collectListItems(dList1))

	v, exists = s2.Get(2)
	requireT.False(exists)
	requireT.Equal(0, v)

	v, exists = s2.Get(4)
	requireT.False(exists)
	requireT.Equal(0, v)

	requireT.Equal([]int{0, 1, 2, 3, 4}, collectSpaceValues(s0))
	requireT.Equal([]int{0, 1, 3, 4}, collectSpaceValues(s1))
	requireT.Equal([]int{0, 1, 3}, collectSpaceValues(s2))
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

	requireT.Equal(allValues, collectSpaceValues(s))
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
		spaceRoot: types.ParentInfo{
			State: lo.ToPtr(types.StateFree),
			Item:  lo.ToPtr[types.NodeAddress](0),
		},
		deallocationRoot: types.ParentInfo{
			State: lo.ToPtr(types.StateFree),
			Item:  lo.ToPtr[types.NodeAddress](0),
		},
		spaceHashMod:                lo.ToPtr[uint64](0),
		pointerNodeAllocator:        pointerNodeAllocator,
		dataNodeAllocator:           dataNodeAllocator,
		snapshotToNodeNodeAllocator: snapshotToNodeNodeAllocator,
		listNodeAllocator:           listNodeAllocator,
	}
}

type env struct {
	Allocator         *alloc.TestAllocator
	DeallocationLists *space.Space[types.SnapshotID, types.NodeAddress]

	snapshotID                  types.SnapshotID
	snapshotAllocator           alloc.SnapshotAllocator
	spaceRoot                   types.ParentInfo
	deallocationRoot            types.ParentInfo
	spaceHashMod                *uint64
	pointerNodeAllocator        space.NodeAllocator[types.NodeAddress]
	dataNodeAllocator           space.NodeAllocator[types.DataItem[int, int]]
	snapshotToNodeNodeAllocator space.NodeAllocator[types.DataItem[types.SnapshotID, types.NodeAddress]]
	listNodeAllocator           list.NodeAllocator
}

func (e *env) NextSnapshot() *space.Space[int, int] {
	snapshotID := e.snapshotID
	e.snapshotID++

	stateCopy := *e.spaceRoot.State
	itemCopy := *e.spaceRoot.Item

	e.spaceRoot = types.ParentInfo{
		State: &stateCopy,
		Item:  &itemCopy,
	}

	e.deallocationRoot = types.ParentInfo{
		State: lo.ToPtr(types.StateFree),
		Item:  lo.ToPtr[types.NodeAddress](0),
	}

	e.DeallocationLists = &space.Space[types.SnapshotID, types.NodeAddress]{}
	e.snapshotAllocator = alloc.NewSnapshotAllocator(
		snapshotID,
		e.Allocator,
		e.DeallocationLists,
		e.listNodeAllocator,
	)
	*e.DeallocationLists = *space.New[types.SnapshotID, types.NodeAddress](
		space.Config[types.SnapshotID, types.NodeAddress]{
			SnapshotID:           snapshotID,
			HashMod:              lo.ToPtr[uint64](0),
			SpaceRoot:            e.deallocationRoot,
			PointerNodeAllocator: e.pointerNodeAllocator,
			DataNodeAllocator:    e.snapshotToNodeNodeAllocator,
			Allocator:            e.snapshotAllocator,
		},
	)

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

func collectSpaceValues[K comparable, V constraints.Ordered](s *space.Space[K, V]) []V {
	values := []V{}
	for item := range s.Iterator() {
		values = append(values, item.Value)
	}

	sort.Slice(values, func(i, j int) bool {
		return values[i] < values[j]
	})
	return values
}

func collectSpaceKeys[K constraints.Ordered, V comparable](s *space.Space[K, V]) []K {
	keys := []K{}
	for item := range s.Iterator() {
		keys = append(keys, item.Key)
	}

	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	return keys
}

func collectListItems(l *list.List) []types.NodeAddress {
	items := []types.NodeAddress{}
	for item := range l.Iterator() {
		items = append(items, item)
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i] < items[j]
	})
	return items
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
