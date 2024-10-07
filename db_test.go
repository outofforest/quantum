package quantum

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/constraints"

	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/list"
	"github.com/outofforest/quantum/space"
	"github.com/outofforest/quantum/types"
)

const (
	space0 types.SpaceID = 0x00
	space1 types.SpaceID = 0x01
	space2 types.SpaceID = 0x02
)

func TestCommitNewSnapshots(t *testing.T) {
	requireT := require.New(t)
	db, allocator := newDB(requireT)

	// Initial state

	snapshot := db.nextSnapshot
	requireT.Equal(types.SingularityNode{
		Version:         0x00,
		FirstSnapshotID: 0x00,
		LastSnapshotID:  0x00,
		SnapshotRoot: types.SpaceInfo{
			State:   types.StateFree,
			Node:    0x00,
			HashMod: 0x00,
		},
	}, *snapshot.SingularityNode)
	requireT.Equal([]types.SnapshotID{}, collectSpaceKeys(snapshot.Snapshots))

	// Snapshot 0

	snapshot = db.nextSnapshot
	requireT.NoError(db.Commit())

	nodesUsed, nodesAllocated, nodesDeallocated := allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x01}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x01}, nodesAllocated)
	requireT.Empty(nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x01}, snapshot.Snapshots.Nodes())

	requireT.Equal(types.SingularityNode{
		Version:         0x00,
		FirstSnapshotID: 0x00,
		LastSnapshotID:  0x00,
		SnapshotRoot: types.SpaceInfo{
			State:   types.StateData,
			Node:    0x01,
			HashMod: 0x00,
		},
	}, *snapshot.SingularityNode)
	requireT.Equal([]types.SnapshotID{0x00}, collectSpaceKeys(snapshot.Snapshots))
	snapshotInfo, exists := snapshot.Snapshots.Get(0x00)
	requireT.True(exists)
	requireT.Equal(types.SnapshotInfo{
		PreviousSnapshotID: 0x00,
		NextSnapshotID:     0x01,
		DeallocationRoot: types.SpaceInfo{
			State:   types.StateFree,
			Node:    0x00,
			HashMod: 0x00,
		},
		SpaceRoot: types.SpaceInfo{
			State:   types.StateFree,
			Node:    0x00,
			HashMod: 0x00,
		},
	}, snapshotInfo)

	// Snapshot 1

	s, err := GetSpace[int, int](space0, db)
	requireT.NoError(err)
	requireT.NoError(s.Set(0, 0))
	requireT.NoError(s.Set(1, 1))

	snapshot = db.nextSnapshot
	requireT.NoError(db.Commit())

	nodesUsed, nodesAllocated, nodesDeallocated = allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x02, 0x03, 0x04}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x02, 0x03, 0x04}, nodesAllocated)
	requireT.Equal([]types.NodeAddress{0x01}, nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x04}, snapshot.Snapshots.Nodes())
	requireT.Equal([]types.NodeAddress{0x03}, snapshot.Spaces.Nodes())
	requireT.Equal([]types.NodeAddress{0x02}, s.Nodes())

	requireT.Equal(types.SingularityNode{
		Version:         0x00,
		FirstSnapshotID: 0x00,
		LastSnapshotID:  0x01,
		SnapshotRoot: types.SpaceInfo{
			State:   types.StateData,
			Node:    0x04,
			HashMod: 0x00,
		},
	}, *snapshot.SingularityNode)
	requireT.Equal([]types.SnapshotID{0x00, 0x01}, collectSpaceKeys(snapshot.Snapshots))
	requireT.Equal([]types.SpaceID{space0}, collectSpaceKeys(snapshot.Spaces))

	snapshotInfo, exists = snapshot.Snapshots.Get(0x00)
	requireT.True(exists)
	requireT.Equal(types.SnapshotInfo{
		PreviousSnapshotID: 0x00,
		NextSnapshotID:     0x01,
		DeallocationRoot: types.SpaceInfo{
			State:   types.StateFree,
			Node:    0x00,
			HashMod: 0x00,
		},
		SpaceRoot: types.SpaceInfo{
			State:   types.StateFree,
			Node:    0x00,
			HashMod: 0x00,
		},
	}, snapshotInfo)

	snapshotInfo, exists = snapshot.Snapshots.Get(0x01)
	requireT.True(exists)
	requireT.Equal(types.SnapshotInfo{
		PreviousSnapshotID: 0x00,
		NextSnapshotID:     0x02,
		DeallocationRoot: types.SpaceInfo{
			State:   types.StateFree,
			Node:    0x00,
			HashMod: 0x00,
		},
		SpaceRoot: types.SpaceInfo{
			State:   types.StateData,
			Node:    0x03,
			HashMod: 0x00,
		},
	}, snapshotInfo)

	spaceInfo, exists := snapshot.Spaces.Get(space0)
	requireT.True(exists)
	requireT.Equal(types.SpaceInfo{
		State:   types.StateData,
		Node:    0x02,
		HashMod: 0x00,
	}, spaceInfo)

	// Snapshot 2

	s, err = GetSpace[int, int](space0, db)
	requireT.NoError(err)
	requireT.NoError(s.Set(0, 10))
	requireT.NoError(s.Set(1, 11))

	snapshot = db.nextSnapshot
	requireT.NoError(db.Commit())

	nodesUsed, nodesAllocated, nodesDeallocated = allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x02, 0x03, 0x05, 0x06, 0x07, 0x08, 0x09}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x05, 0x06, 0x07, 0x08, 0x09}, nodesAllocated)
	requireT.Equal([]types.NodeAddress{0x04}, nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x09}, snapshot.Snapshots.Nodes())
	requireT.Equal([]types.NodeAddress{0x08}, snapshot.Spaces.Nodes())
	requireT.Equal([]types.NodeAddress{0x05}, s.Nodes())

	requireT.Equal(types.SingularityNode{
		Version:         0x00,
		FirstSnapshotID: 0x00,
		LastSnapshotID:  0x02,
		SnapshotRoot: types.SpaceInfo{
			State:   types.StateData,
			Node:    0x09,
			HashMod: 0x00,
		},
	}, *snapshot.SingularityNode)
	requireT.Equal([]types.SnapshotID{0x00, 0x01, 0x02}, collectSpaceKeys(snapshot.Snapshots))
	requireT.Equal([]types.SpaceID{space0}, collectSpaceKeys(snapshot.Spaces))

	snapshotInfo, exists = snapshot.Snapshots.Get(0x00)
	requireT.True(exists)
	requireT.Equal(types.SnapshotInfo{
		PreviousSnapshotID: 0x00,
		NextSnapshotID:     0x01,
		DeallocationRoot: types.SpaceInfo{
			State:   types.StateFree,
			Node:    0x00,
			HashMod: 0x00,
		},
		SpaceRoot: types.SpaceInfo{
			State:   types.StateFree,
			Node:    0x00,
			HashMod: 0x00,
		},
	}, snapshotInfo)

	snapshotInfo, exists = snapshot.Snapshots.Get(0x01)
	requireT.True(exists)
	requireT.Equal(types.SnapshotInfo{
		PreviousSnapshotID: 0x00,
		NextSnapshotID:     0x02,
		DeallocationRoot: types.SpaceInfo{
			State:   types.StateFree,
			Node:    0x00,
			HashMod: 0x00,
		},
		SpaceRoot: types.SpaceInfo{
			State:   types.StateData,
			Node:    0x03,
			HashMod: 0x00,
		},
	}, snapshotInfo)

	snapshotInfo, exists = snapshot.Snapshots.Get(0x02)
	requireT.True(exists)
	requireT.Equal(types.SnapshotInfo{
		PreviousSnapshotID: 0x01,
		NextSnapshotID:     0x03,
		DeallocationRoot: types.SpaceInfo{
			State:   types.StateData,
			Node:    0x07,
			HashMod: 0x00,
		},
		SpaceRoot: types.SpaceInfo{
			State:   types.StateData,
			Node:    0x08,
			HashMod: 0x00,
		},
	}, snapshotInfo)

	spaceInfo, exists = snapshot.Spaces.Get(space0)
	requireT.True(exists)
	requireT.Equal(types.SpaceInfo{
		State:   types.StateData,
		Node:    0x05,
		HashMod: 0x00,
	}, spaceInfo)
	requireT.Equal([]types.NodeAddress{0x07}, snapshot.DeallocationLists.Nodes())
	requireT.Equal([]types.SnapshotID{0x01}, collectSpaceKeys(snapshot.DeallocationLists))

	dList1Address, exists := snapshot.DeallocationLists.Get(0x01)
	requireT.True(exists)
	requireT.Equal(types.NodeAddress(0x06), dList1Address)

	dList1 := newList(dList1Address, db)
	requireT.Equal([]types.NodeAddress{0x02, 0x03}, collectListItems(dList1))
}

func TestSpaces(t *testing.T) {
	requireT := require.New(t)
	db, _ := newDB(requireT)

	// Snapshot 0

	s00, err := GetSpace[int, int](space0, db)
	requireT.NoError(err)

	requireT.NoError(s00.Set(0, 0x000))
	requireT.NoError(s00.Set(1, 0x001))

	v0, exists := s00.Get(0)
	requireT.True(exists)
	requireT.Equal(0x000, v0)

	v1, exists := s00.Get(1)
	requireT.True(exists)
	requireT.Equal(0x001, v1)

	requireT.NoError(db.Commit())

	// Snapshot 1

	s10, err := GetSpace[int, int](space0, db)
	requireT.NoError(err)

	s11, err := GetSpace[int, int](space1, db)
	requireT.NoError(err)

	v0, exists = s10.Get(0)
	requireT.True(exists)
	requireT.Equal(0x000, v0)

	v1, exists = s10.Get(1)
	requireT.True(exists)
	requireT.Equal(0x001, v1)

	requireT.NoError(s10.Set(0, 0x100))
	requireT.NoError(s10.Set(1, 0x101))

	requireT.NoError(s11.Set(0, 0x110))
	requireT.NoError(s11.Set(1, 0x111))

	v0, exists = s00.Get(0)
	requireT.True(exists)
	requireT.Equal(0x000, v0)

	v1, exists = s00.Get(1)
	requireT.True(exists)
	requireT.Equal(0x001, v1)

	v0, exists = s10.Get(0)
	requireT.True(exists)
	requireT.Equal(0x100, v0)

	v1, exists = s10.Get(1)
	requireT.True(exists)
	requireT.Equal(0x101, v1)

	v0, exists = s11.Get(0)
	requireT.True(exists)
	requireT.Equal(0x110, v0)

	v1, exists = s11.Get(1)
	requireT.True(exists)
	requireT.Equal(0x111, v1)

	requireT.NoError(db.Commit())

	// Snapshot 2

	s20, err := GetSpace[int, int](space0, db)
	requireT.NoError(err)

	s21, err := GetSpace[int, int](space1, db)
	requireT.NoError(err)

	s22, err := GetSpace[int, int](space2, db)
	requireT.NoError(err)

	v0, exists = s20.Get(0)
	requireT.True(exists)
	requireT.Equal(0x100, v0)

	v1, exists = s20.Get(1)
	requireT.True(exists)
	requireT.Equal(0x101, v1)

	v0, exists = s21.Get(0)
	requireT.True(exists)
	requireT.Equal(0x110, v0)

	v1, exists = s21.Get(1)
	requireT.True(exists)
	requireT.Equal(0x111, v1)

	requireT.NoError(s20.Set(0, 0x200))
	requireT.NoError(s20.Set(1, 0x201))

	requireT.NoError(s21.Set(0, 0x210))
	requireT.NoError(s21.Set(1, 0x211))

	requireT.NoError(s22.Set(0, 0x220))
	requireT.NoError(s22.Set(1, 0x221))

	v0, exists = s00.Get(0)
	requireT.True(exists)
	requireT.Equal(0x000, v0)

	v1, exists = s00.Get(1)
	requireT.True(exists)
	requireT.Equal(0x001, v1)

	v0, exists = s10.Get(0)
	requireT.True(exists)
	requireT.Equal(0x100, v0)

	v1, exists = s10.Get(1)
	requireT.True(exists)
	requireT.Equal(0x101, v1)

	v0, exists = s11.Get(0)
	requireT.True(exists)
	requireT.Equal(0x110, v0)

	v1, exists = s11.Get(1)
	requireT.True(exists)
	requireT.Equal(0x111, v1)

	v0, exists = s20.Get(0)
	requireT.True(exists)
	requireT.Equal(0x200, v0)

	v1, exists = s20.Get(1)
	requireT.True(exists)
	requireT.Equal(0x201, v1)

	v0, exists = s21.Get(0)
	requireT.True(exists)
	requireT.Equal(0x210, v0)

	v1, exists = s21.Get(1)
	requireT.True(exists)
	requireT.Equal(0x211, v1)

	v0, exists = s22.Get(0)
	requireT.True(exists)
	requireT.Equal(0x220, v0)

	v1, exists = s22.Get(1)
	requireT.True(exists)
	requireT.Equal(0x221, v1)

	requireT.NoError(db.Commit())
}

func newDB(requireT *require.Assertions) (*DB, *alloc.TestAllocator) {
	allocator := alloc.NewTestAllocator(alloc.NewAllocator(alloc.Config{
		TotalSize: 1024 * 1024,
		NodeSize:  512,
	}))
	db, err := New(Config{
		Allocator: allocator,
	})
	requireT.NoError(err)
	return db, allocator
}

func newList(nodeAddress types.NodeAddress, db *DB) *list.List {
	snapshot := db.nextSnapshot
	return list.New(list.Config{
		SnapshotID:    snapshot.SnapshotID,
		Item:          &nodeAddress,
		NodeAllocator: db.listNodeAllocator,
		Allocator:     snapshot.Allocator,
	})
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
