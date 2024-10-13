package quantum

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/quantum/list"
	"github.com/outofforest/quantum/space"
	"github.com/outofforest/quantum/test"
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

	requireT.Equal(types.SingularityNode{
		Version:         0x00,
		FirstSnapshotID: 0x00,
		LastSnapshotID:  0x00,
		SnapshotRoot: types.SpaceInfo{
			State: types.StateFree,
			Pointer: types.Pointer{
				Version:    0x00,
				SnapshotID: 0x00,
				Address:    0x00,
			},
			HashMod: 0x00,
		},
	}, *db.nextSnapshot.SingularityNode)
	requireT.Equal([]types.SnapshotID{}, test.CollectSpaceKeys(db.nextSnapshot.Snapshots))
	requireT.Empty(db.nextSnapshot.DeallocationLists.Nodes())

	// Snapshot 0

	s0, err := GetSpace[int, int](space0, db)
	requireT.NoError(err)
	requireT.NoError(db.Commit())
	snapshot0 := newSnapshot(requireT, 0x00, db)

	nodesUsed, nodesAllocated, nodesDeallocated := allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x01}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x01}, nodesAllocated)
	requireT.Empty(nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x01}, db.nextSnapshot.Snapshots.Nodes())
	requireT.Empty(snapshot0.Spaces.Nodes())
	requireT.Empty(s0.Nodes())
	requireT.Empty(snapshot0.DeallocationLists.Nodes())

	requireT.Equal(types.SingularityNode{
		Version:         0x00,
		FirstSnapshotID: 0x00,
		LastSnapshotID:  0x01,
		SnapshotRoot: types.SpaceInfo{
			State: types.StateData,
			Pointer: types.Pointer{
				Version:    0x00,
				SnapshotID: 0x00,
				Address:    0x01,
			},
			HashMod: 0x00,
		},
	}, *db.nextSnapshot.SingularityNode)
	requireT.Equal([]types.SnapshotID{0x00}, test.CollectSpaceKeys(db.nextSnapshot.Snapshots))

	snapshotInfo, exists := db.nextSnapshot.Snapshots.Get(0x00)
	requireT.True(exists)
	requireT.Equal(types.SnapshotInfo{
		PreviousSnapshotID: 0x00,
		NextSnapshotID:     0x01,
		DeallocationRoot: types.SpaceInfo{
			State: types.StateFree,
			Pointer: types.Pointer{
				Version:    0x00,
				SnapshotID: 0x00,
				Address:    0x00,
			},
			HashMod: 0x00,
		},
		SpaceRoot: types.SpaceInfo{
			State: types.StateFree,
			Pointer: types.Pointer{
				Version:    0x00,
				SnapshotID: 0x00,
				Address:    0x00,
			},
			HashMod: 0x00,
		},
	}, snapshotInfo)

	// Snapshot 1

	s1, err := GetSpace[int, int](space0, db)
	requireT.NoError(err)
	requireT.NoError(s1.Set(0, 0))
	requireT.NoError(s1.Set(1, 1))
	requireT.NoError(s1.Set(2, 2))
	requireT.NoError(s1.Set(3, 3))
	requireT.NoError(s1.Set(4, 4))

	requireT.NoError(db.Commit())
	snapshot1 := newSnapshot(requireT, 0x01, db)

	nodesUsed, nodesAllocated, nodesDeallocated = allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x02, 0x03, 0x04}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x02, 0x03, 0x04}, nodesAllocated)
	requireT.Equal([]types.NodeAddress{0x01}, nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x04}, db.nextSnapshot.Snapshots.Nodes())
	requireT.Empty(snapshot0.Spaces.Nodes())
	requireT.Equal([]types.NodeAddress{0x03}, snapshot1.Spaces.Nodes())
	requireT.Empty(s0.Nodes())
	requireT.Equal([]types.NodeAddress{0x02}, s1.Nodes())
	requireT.Empty(snapshot0.DeallocationLists.Nodes())
	requireT.Empty(snapshot1.DeallocationLists.Nodes())

	requireT.Equal(types.SingularityNode{
		Version:         0x00,
		FirstSnapshotID: 0x00,
		LastSnapshotID:  0x02,
		SnapshotRoot: types.SpaceInfo{
			State: types.StateData,
			Pointer: types.Pointer{
				Version:    0x00,
				SnapshotID: 0x01,
				Address:    0x04,
			},
			HashMod: 0x00,
		},
	}, *db.nextSnapshot.SingularityNode)
	requireT.Equal([]types.SnapshotID{0x00, 0x01}, test.CollectSpaceKeys(db.nextSnapshot.Snapshots))
	requireT.Equal([]types.SpaceID{space0}, test.CollectSpaceKeys(snapshot1.Spaces))

	snapshotInfo, exists = db.nextSnapshot.Snapshots.Get(0x00)
	requireT.True(exists)
	requireT.Equal(types.SnapshotInfo{
		PreviousSnapshotID: 0x00,
		NextSnapshotID:     0x01,
		DeallocationRoot: types.SpaceInfo{
			State: types.StateFree,
			Pointer: types.Pointer{
				Version:    0x00,
				SnapshotID: 0x00,
				Address:    0x00,
			},
			HashMod: 0x00,
		},
		SpaceRoot: types.SpaceInfo{
			State: types.StateFree,
			Pointer: types.Pointer{
				Version:    0x00,
				SnapshotID: 0x00,
				Address:    0x00,
			},
			HashMod: 0x00,
		},
	}, snapshotInfo)

	snapshotInfo, exists = db.nextSnapshot.Snapshots.Get(0x01)
	requireT.True(exists)
	requireT.Equal(types.SnapshotInfo{
		PreviousSnapshotID: 0x00,
		NextSnapshotID:     0x02,
		DeallocationRoot: types.SpaceInfo{
			State: types.StateFree,
			Pointer: types.Pointer{
				Version:    0x00,
				SnapshotID: 0x00,
				Address:    0x00,
			},
			HashMod: 0x00,
		},
		SpaceRoot: types.SpaceInfo{
			State: types.StateData,
			Pointer: types.Pointer{
				Version:    0x00,
				SnapshotID: 0x01,
				Address:    0x03,
			},
			HashMod: 0x00,
		},
	}, snapshotInfo)

	spaceInfo, exists := snapshot1.Spaces.Get(space0)
	requireT.True(exists)
	requireT.Equal(types.SpaceInfo{
		State: types.StateData,
		Pointer: types.Pointer{
			Version:    0x00,
			SnapshotID: 0x01,
			Address:    0x02,
		},
		HashMod: 0x00,
	}, spaceInfo)

	// Snapshot 2

	s2, err := GetSpace[int, int](space0, db)
	requireT.NoError(err)
	requireT.NoError(s2.Set(0, 10))

	requireT.NoError(db.Commit())
	snapshot2 := newSnapshot(requireT, 0x02, db)

	nodesUsed, nodesAllocated, nodesDeallocated = allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x02, 0x03, 0x05, 0x06, 0x07, 0x08, 0x09}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x05, 0x06, 0x07, 0x08, 0x09}, nodesAllocated)
	requireT.Equal([]types.NodeAddress{0x04}, nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x09}, db.nextSnapshot.Snapshots.Nodes())
	requireT.Empty(snapshot0.Spaces.Nodes())
	requireT.Equal([]types.NodeAddress{0x03}, snapshot1.Spaces.Nodes())
	requireT.Equal([]types.NodeAddress{0x08}, snapshot2.Spaces.Nodes())
	requireT.Empty(s0.Nodes())
	requireT.Equal([]types.NodeAddress{0x02}, s1.Nodes())
	requireT.Equal([]types.NodeAddress{0x05}, s2.Nodes())
	requireT.Empty(snapshot0.DeallocationLists.Nodes())
	requireT.Empty(snapshot1.DeallocationLists.Nodes())
	requireT.Equal([]types.NodeAddress{0x07}, snapshot2.DeallocationLists.Nodes())
	requireT.Equal([]types.SnapshotID{0x01}, test.CollectSpaceKeys(snapshot2.DeallocationLists))
	dlist21Address, exists := snapshot2.DeallocationLists.Get(0x01)
	requireT.True(exists)
	dList21 := newList(dlist21Address, db)
	requireT.Equal([]types.NodeAddress{0x06}, dList21.Nodes())

	requireT.Equal(types.SingularityNode{
		Version:         0x00,
		FirstSnapshotID: 0x00,
		LastSnapshotID:  0x03,
		SnapshotRoot: types.SpaceInfo{
			State: types.StateData,
			Pointer: types.Pointer{
				Version:    0x00,
				SnapshotID: 0x02,
				Address:    0x09,
			},
			HashMod: 0x00,
		},
	}, *db.nextSnapshot.SingularityNode)
	requireT.Equal([]types.SnapshotID{0x00, 0x01, 0x02}, test.CollectSpaceKeys(db.nextSnapshot.Snapshots))
	requireT.Equal([]types.SpaceID{space0}, test.CollectSpaceKeys(snapshot2.Spaces))

	snapshotInfo, exists = db.nextSnapshot.Snapshots.Get(0x00)
	requireT.True(exists)
	requireT.Equal(types.SnapshotInfo{
		PreviousSnapshotID: 0x00,
		NextSnapshotID:     0x01,
		DeallocationRoot: types.SpaceInfo{
			State: types.StateFree,
			Pointer: types.Pointer{
				Version:    0x00,
				SnapshotID: 0x00,
				Address:    0x00,
			},
			HashMod: 0x00,
		},
		SpaceRoot: types.SpaceInfo{
			State: types.StateFree,
			Pointer: types.Pointer{
				Version:    0x00,
				SnapshotID: 0x00,
				Address:    0x00,
			},
			HashMod: 0x00,
		},
	}, snapshotInfo)

	snapshotInfo, exists = db.nextSnapshot.Snapshots.Get(0x01)
	requireT.True(exists)
	requireT.Equal(types.SnapshotInfo{
		PreviousSnapshotID: 0x00,
		NextSnapshotID:     0x02,
		DeallocationRoot: types.SpaceInfo{
			State: types.StateFree,
			Pointer: types.Pointer{
				Version:    0x00,
				SnapshotID: 0x00,
				Address:    0x00,
			},
			HashMod: 0x00,
		},
		SpaceRoot: types.SpaceInfo{
			State: types.StateData,
			Pointer: types.Pointer{
				Version:    0x00,
				SnapshotID: 0x01,
				Address:    0x03,
			},
			HashMod: 0x00,
		},
	}, snapshotInfo)

	snapshotInfo, exists = db.nextSnapshot.Snapshots.Get(0x02)
	requireT.True(exists)
	requireT.Equal(types.SnapshotInfo{
		PreviousSnapshotID: 0x01,
		NextSnapshotID:     0x03,
		DeallocationRoot: types.SpaceInfo{
			State: types.StateData,
			Pointer: types.Pointer{
				Version:    0x00,
				SnapshotID: 0x02,
				Address:    0x07,
			},
			HashMod: 0x00,
		},
		SpaceRoot: types.SpaceInfo{
			State: types.StateData,
			Pointer: types.Pointer{
				Version:    0x00,
				SnapshotID: 0x02,
				Address:    0x08,
			},
			HashMod: 0x00,
		},
	}, snapshotInfo)

	spaceInfo, exists = snapshot2.Spaces.Get(space0)
	requireT.True(exists)
	requireT.Equal(types.SpaceInfo{
		State: types.StateData,
		Pointer: types.Pointer{
			Version:    0x00,
			SnapshotID: 0x02,
			Address:    0x05,
		},
		HashMod: 0x00,
	}, spaceInfo)

	// Snapshot 3

	s3, err := GetSpace[int, int](space0, db)
	requireT.NoError(err)
	requireT.NoError(s3.Set(0, 20))
	requireT.NoError(s3.Set(4, 24))

	requireT.NoError(db.Commit())
	snapshot3 := newSnapshot(requireT, 0x03, db)

	nodesUsed, nodesAllocated, nodesDeallocated = allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x02, 0x03, 0x05, 0x06, 0x07, 0x08, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x0a, 0x0b, 0x0c, 0x0d, 0x0e}, nodesAllocated)
	requireT.Equal([]types.NodeAddress{0x09}, nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x0e}, db.nextSnapshot.Snapshots.Nodes())
	requireT.Empty(snapshot0.Spaces.Nodes())
	requireT.Equal([]types.NodeAddress{0x03}, snapshot1.Spaces.Nodes())
	requireT.Equal([]types.NodeAddress{0x08}, snapshot2.Spaces.Nodes())
	requireT.Equal([]types.NodeAddress{0x0d}, snapshot3.Spaces.Nodes())
	requireT.Empty(s0.Nodes())
	requireT.Equal([]types.NodeAddress{0x02}, s1.Nodes())
	requireT.Equal([]types.NodeAddress{0x05}, s2.Nodes())
	requireT.Equal([]types.NodeAddress{0x0a}, s3.Nodes())
	requireT.Empty(snapshot0.DeallocationLists.Nodes())
	requireT.Empty(snapshot1.DeallocationLists.Nodes())
	requireT.Equal([]types.NodeAddress{0x07}, snapshot2.DeallocationLists.Nodes())
	requireT.Equal([]types.NodeAddress{0x0c}, snapshot3.DeallocationLists.Nodes())
	requireT.Equal([]types.SnapshotID{0x01}, test.CollectSpaceKeys(snapshot2.DeallocationLists))
	requireT.Equal([]types.SnapshotID{0x02}, test.CollectSpaceKeys(snapshot3.DeallocationLists))
	dlist21Address, exists = snapshot2.DeallocationLists.Get(0x01)
	requireT.True(exists)
	dList21 = newList(dlist21Address, db)
	dlist32Address, exists := snapshot3.DeallocationLists.Get(0x02)
	requireT.True(exists)
	dList32 := newList(dlist32Address, db)
	requireT.Equal([]types.NodeAddress{0x06}, dList21.Nodes())
	requireT.Equal([]types.NodeAddress{0x0b}, dList32.Nodes())

	requireT.Equal(types.SingularityNode{
		Version:         0x00,
		FirstSnapshotID: 0x00,
		LastSnapshotID:  0x04,
		SnapshotRoot: types.SpaceInfo{
			State: types.StateData,
			Pointer: types.Pointer{
				Version:    0x00,
				SnapshotID: 0x03,
				Address:    0x0e,
			},
			HashMod: 0x00,
		},
	}, *db.nextSnapshot.SingularityNode)
	requireT.Equal([]types.SnapshotID{0x00, 0x01, 0x02, 0x03}, test.CollectSpaceKeys(db.nextSnapshot.Snapshots))
	requireT.Equal([]types.SpaceID{space0}, test.CollectSpaceKeys(snapshot3.Spaces))

	snapshotInfo, exists = db.nextSnapshot.Snapshots.Get(0x00)
	requireT.True(exists)
	requireT.Equal(types.SnapshotInfo{
		PreviousSnapshotID: 0x00,
		NextSnapshotID:     0x01,
		DeallocationRoot: types.SpaceInfo{
			State: types.StateFree,
			Pointer: types.Pointer{
				Version:    0x00,
				SnapshotID: 0x00,
				Address:    0x00,
			},
			HashMod: 0x00,
		},
		SpaceRoot: types.SpaceInfo{
			State: types.StateFree,
			Pointer: types.Pointer{
				Version:    0x00,
				SnapshotID: 0x00,
				Address:    0x00,
			},
			HashMod: 0x00,
		},
	}, snapshotInfo)

	snapshotInfo, exists = db.nextSnapshot.Snapshots.Get(0x01)
	requireT.True(exists)
	requireT.Equal(types.SnapshotInfo{
		PreviousSnapshotID: 0x00,
		NextSnapshotID:     0x02,
		DeallocationRoot: types.SpaceInfo{
			State: types.StateFree,
			Pointer: types.Pointer{
				Version:    0x00,
				SnapshotID: 0x00,
				Address:    0x00,
			},
			HashMod: 0x00,
		},
		SpaceRoot: types.SpaceInfo{
			State: types.StateData,
			Pointer: types.Pointer{
				Version:    0x00,
				SnapshotID: 0x01,
				Address:    0x03,
			},
			HashMod: 0x00,
		},
	}, snapshotInfo)

	snapshotInfo, exists = db.nextSnapshot.Snapshots.Get(0x02)
	requireT.True(exists)
	requireT.Equal(types.SnapshotInfo{
		PreviousSnapshotID: 0x01,
		NextSnapshotID:     0x03,
		DeallocationRoot: types.SpaceInfo{
			State: types.StateData,
			Pointer: types.Pointer{
				Version:    0x00,
				SnapshotID: 0x02,
				Address:    0x07,
			},
			HashMod: 0x00,
		},
		SpaceRoot: types.SpaceInfo{
			State: types.StateData,
			Pointer: types.Pointer{
				Version:    0x00,
				SnapshotID: 0x02,
				Address:    0x08,
			},
			HashMod: 0x00,
		},
	}, snapshotInfo)

	snapshotInfo, exists = db.nextSnapshot.Snapshots.Get(0x03)
	requireT.True(exists)
	requireT.Equal(types.SnapshotInfo{
		PreviousSnapshotID: 0x02,
		NextSnapshotID:     0x04,
		DeallocationRoot: types.SpaceInfo{
			State: types.StateData,
			Pointer: types.Pointer{
				Version:    0x00,
				SnapshotID: 0x03,
				Address:    0x0c,
			},
			HashMod: 0x00,
		},
		SpaceRoot: types.SpaceInfo{
			State: types.StateData,
			Pointer: types.Pointer{
				Version:    0x00,
				SnapshotID: 0x03,
				Address:    0x0d,
			},
			HashMod: 0x00,
		},
	}, snapshotInfo)

	spaceInfo, exists = snapshot3.Spaces.Get(space0)
	requireT.True(exists)
	requireT.Equal(types.SpaceInfo{
		State: types.StateData,
		Pointer: types.Pointer{
			Version:    0x00,
			SnapshotID: 0x03,
			Address:    0x0a,
		},
		HashMod: 0x00,
	}, spaceInfo)
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

func TestDoubledSpace(t *testing.T) {
	requireT := require.New(t)
	db, _ := newDB(requireT)

	sA, err := GetSpace[int, int](space0, db)
	requireT.NoError(err)
	requireT.NoError(sA.Set(1, 1))

	v, exists := sA.Get(1)
	requireT.True(exists)
	requireT.Equal(1, v)

	v, exists = sA.Get(2)
	requireT.False(exists)
	requireT.Equal(0, v)

	sB, err := GetSpace[int, int](space0, db)
	requireT.NoError(err)

	v, exists = sB.Get(1)
	requireT.True(exists)
	requireT.Equal(1, v)

	requireT.NoError(sB.Set(2, 2))

	v, exists = sB.Get(2)
	requireT.True(exists)
	requireT.Equal(2, v)

	v, exists = sA.Get(2)
	requireT.True(exists)
	requireT.Equal(2, v)

	requireT.NoError(sA.Set(2, 12))

	v, exists = sB.Get(2)
	requireT.True(exists)
	requireT.Equal(12, v)
}

func TestDeleteTheOnlySnapshot(t *testing.T) {
	requireT := require.New(t)
	db, allocator := newDB(requireT)

	// Prepare snapshots

	// Snapshot 0

	s, err := GetSpace[int, int](space0, db)
	requireT.NoError(err)

	requireT.NoError(s.Set(0, 0))
	requireT.NoError(s.Set(1, 1))
	requireT.NoError(s.Set(2, 2))
	requireT.NoError(s.Set(3, 3))
	requireT.NoError(s.Set(4, 4))
	requireT.NoError(db.Commit())

	snapshot0 := newSnapshot(requireT, 0x00, db)

	requireT.Equal([]int{0, 1, 2, 3, 4}, test.CollectSpaceValues(s))

	nodesUsed, _, _ := allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x01, 0x02, 0x03}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x01}, s.Nodes())
	requireT.Equal([]types.NodeAddress{0x02}, snapshot0.Spaces.Nodes())
	requireT.Equal([]types.NodeAddress{0x03}, db.nextSnapshot.Snapshots.Nodes())
	requireT.Equal([]types.SnapshotID{0x00}, test.CollectSpaceKeys(db.nextSnapshot.Snapshots))
	requireT.Empty(snapshot0.DeallocationLists.Nodes())

	// Cause some deallocations.

	s, err = GetSpace[int, int](space0, db)
	requireT.NoError(err)
	requireT.NoError(s.Set(0, 10))
	requireT.Equal([]types.SnapshotID{0x00}, test.CollectSpaceKeys(db.nextSnapshot.DeallocationLists))

	// Delete snapshot 0

	requireT.NoError(db.DeleteSnapshot(0x00))
	requireT.Empty(test.CollectSpaceKeys(db.nextSnapshot.DeallocationLists))
	requireT.NoError(db.Commit())

	snapshot1 := newSnapshot(requireT, 0x01, db)

	// Verify the current state.

	requireT.Equal(types.SnapshotID(0x01), snapshot1.SnapshotInfo.PreviousSnapshotID)
	requireT.Equal(types.SnapshotID(0x02), snapshot1.SnapshotInfo.NextSnapshotID)
	requireT.Equal(types.SnapshotID(0x01), db.nextSnapshot.SingularityNode.FirstSnapshotID)
	requireT.Equal(types.SnapshotID(0x02), db.nextSnapshot.SingularityNode.LastSnapshotID)

	s, err = GetSpace[int, int](space0, db)
	requireT.NoError(err)

	nodesUsed, nodesAllocated, nodesDeallocated := allocator.Nodes()

	requireT.Equal([]types.NodeAddress{0x04, 0x06, 0x07, 0x08}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x04, 0x05, 0x06, 0x07, 0x08}, nodesAllocated)
	requireT.Equal([]types.NodeAddress{0x01, 0x02, 0x03, 0x05}, nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x07}, db.nextSnapshot.Snapshots.Nodes())
	requireT.Equal([]types.NodeAddress{0x08}, snapshot1.Spaces.Nodes())
	requireT.Equal([]types.NodeAddress{0x04}, s.Nodes())
	requireT.Equal([]types.NodeAddress{0x06}, snapshot1.DeallocationLists.Nodes())
	requireT.Empty(test.CollectSpaceKeys(snapshot1.DeallocationLists))
	requireT.Equal([]types.SnapshotID{0x01}, test.CollectSpaceKeys(db.nextSnapshot.Snapshots))

	// Cause some deallocations.

	s, err = GetSpace[int, int](space0, db)
	requireT.NoError(err)
	requireT.NoError(s.Set(0, 20))
	requireT.Equal([]types.SnapshotID{0x01}, test.CollectSpaceKeys(db.nextSnapshot.DeallocationLists))

	// Delete the only existing snapshot 1 to verify that procedure works with non-zero snapshot.

	requireT.NoError(db.DeleteSnapshot(0x01))
	requireT.NoError(db.Commit())

	snapshot2 := newSnapshot(requireT, 0x02, db)

	// Verify the current state.

	requireT.Equal(types.SnapshotID(0x02), snapshot2.SnapshotInfo.PreviousSnapshotID)
	requireT.Equal(types.SnapshotID(0x03), snapshot2.SnapshotInfo.NextSnapshotID)
	requireT.Equal(types.SnapshotID(0x02), db.nextSnapshot.SingularityNode.FirstSnapshotID)
	requireT.Equal(types.SnapshotID(0x03), db.nextSnapshot.SingularityNode.LastSnapshotID)

	s, err = GetSpace[int, int](space0, db)
	requireT.NoError(err)

	nodesUsed, nodesAllocated, nodesDeallocated = allocator.Nodes()

	requireT.Equal([]types.NodeAddress{0x09, 0x0b, 0x0c, 0x0d}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x09, 0x0a, 0x0b, 0x0c, 0x0d}, nodesAllocated)
	requireT.Equal([]types.NodeAddress{0x04, 0x06, 0x07, 0x08, 0x0a}, nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x0c}, db.nextSnapshot.Snapshots.Nodes())
	requireT.Equal([]types.NodeAddress{0x0d}, snapshot2.Spaces.Nodes())
	requireT.Equal([]types.NodeAddress{0x09}, s.Nodes())
	requireT.Equal([]types.NodeAddress{0x0b}, snapshot2.DeallocationLists.Nodes())
	requireT.Empty(test.CollectSpaceKeys(snapshot2.DeallocationLists))
	requireT.Equal([]types.SnapshotID{0x02}, test.CollectSpaceKeys(db.nextSnapshot.Snapshots))
}

func TestDeleteFirstSnapshot(t *testing.T) {
	requireT := require.New(t)
	db, allocator := newDB(requireT)

	// Snapshot 0

	s0, err := GetSpace[int, int](space0, db)
	requireT.NoError(err)

	requireT.NoError(s0.Set(0, 0))
	requireT.NoError(s0.Set(1, 1))
	requireT.NoError(s0.Set(2, 2))
	requireT.NoError(s0.Set(3, 3))
	requireT.NoError(s0.Set(4, 4))
	requireT.NoError(db.Commit())

	// Snapshot 1

	s1, err := GetSpace[int, int](space0, db)
	requireT.NoError(err)

	requireT.NoError(s1.Set(0, 10))
	requireT.NoError(db.Commit())

	// Snapshot 2

	s2, err := GetSpace[int, int](space0, db)
	requireT.NoError(err)

	requireT.NoError(s2.Set(0, 20))
	requireT.NoError(s2.Set(1, 21))
	requireT.NoError(db.Commit())

	// Check the initial state

	snapshot0 := newSnapshot(requireT, 0x00, db)
	snapshot1 := newSnapshot(requireT, 0x01, db)
	snapshot2 := newSnapshot(requireT, 0x02, db)

	requireT.Equal(types.SnapshotID(0x00), snapshot0.SnapshotInfo.PreviousSnapshotID)
	requireT.Equal(types.SnapshotID(0x01), snapshot0.SnapshotInfo.NextSnapshotID)
	requireT.Equal(types.SnapshotID(0x00), snapshot1.SnapshotInfo.PreviousSnapshotID)
	requireT.Equal(types.SnapshotID(0x02), snapshot1.SnapshotInfo.NextSnapshotID)
	requireT.Equal(types.SnapshotID(0x01), snapshot2.SnapshotInfo.PreviousSnapshotID)
	requireT.Equal(types.SnapshotID(0x03), snapshot2.SnapshotInfo.NextSnapshotID)
	requireT.Equal(types.SnapshotID(0x00), db.nextSnapshot.SingularityNode.FirstSnapshotID)
	requireT.Equal(types.SnapshotID(0x03), db.nextSnapshot.SingularityNode.LastSnapshotID)

	s20 := newSpace[int, int](requireT, space0, snapshot2.Spaces, db)

	requireT.Equal([]int{2, 3, 4, 20, 21}, test.CollectSpaceValues(s20))

	nodesUsed, _, _ := allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x01, 0x02, 0x04, 0x05, 0x06, 0x07, 0x09, 0x0a, 0x0b, 0x0c, 0x0d}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x09}, s20.Nodes())
	requireT.Equal([]types.NodeAddress{0x02}, snapshot0.Spaces.Nodes())
	requireT.Equal([]types.NodeAddress{0x07}, snapshot1.Spaces.Nodes())
	requireT.Equal([]types.NodeAddress{0x0c}, snapshot2.Spaces.Nodes())
	requireT.Equal([]types.NodeAddress{0x0d}, db.nextSnapshot.Snapshots.Nodes())
	requireT.Equal([]types.SnapshotID{0x00, 0x01, 0x02}, test.CollectSpaceKeys(db.nextSnapshot.Snapshots))
	requireT.Empty(snapshot0.DeallocationLists.Nodes())
	requireT.Equal([]types.NodeAddress{0x06}, snapshot1.DeallocationLists.Nodes())
	requireT.Equal([]types.NodeAddress{0x0b}, snapshot2.DeallocationLists.Nodes())
	requireT.Equal([]types.SnapshotID{0x00}, test.CollectSpaceKeys(snapshot1.DeallocationLists))
	requireT.Equal([]types.SnapshotID{0x01}, test.CollectSpaceKeys(snapshot2.DeallocationLists))
	dList10Address, exists := snapshot1.DeallocationLists.Get(0x00)
	requireT.True(exists)
	dList21Address, exists := snapshot2.DeallocationLists.Get(0x01)
	requireT.True(exists)

	dList10 := newList(dList10Address, db)
	dList21 := newList(dList21Address, db)

	requireT.Equal([]types.NodeAddress{0x05}, dList10.Nodes())
	requireT.Equal([]types.NodeAddress{0x0a}, dList21.Nodes())
	requireT.Equal([]types.NodeAddress{0x01, 0x02}, test.CollectListItems(dList10))
	requireT.Equal([]types.NodeAddress{0x04, 0x07}, test.CollectListItems(dList21))

	// Delete snapshot 0

	requireT.NoError(db.DeleteSnapshot(0x00))
	requireT.NoError(db.Commit())

	snapshot1 = newSnapshot(requireT, 0x01, db)
	snapshot2 = newSnapshot(requireT, 0x02, db)
	snapshot3 := newSnapshot(requireT, 0x03, db)

	requireT.Equal(types.SnapshotID(0x01), snapshot1.SnapshotInfo.PreviousSnapshotID)
	requireT.Equal(types.SnapshotID(0x02), snapshot1.SnapshotInfo.NextSnapshotID)
	requireT.Equal(types.SnapshotID(0x01), snapshot2.SnapshotInfo.PreviousSnapshotID)
	requireT.Equal(types.SnapshotID(0x03), snapshot2.SnapshotInfo.NextSnapshotID)
	requireT.Equal(types.SnapshotID(0x02), snapshot3.SnapshotInfo.PreviousSnapshotID)
	requireT.Equal(types.SnapshotID(0x04), snapshot3.SnapshotInfo.NextSnapshotID)
	requireT.Equal(types.SnapshotID(0x01), db.nextSnapshot.SingularityNode.FirstSnapshotID)
	requireT.Equal(types.SnapshotID(0x04), db.nextSnapshot.SingularityNode.LastSnapshotID)

	s30 := newSpace[int, int](requireT, space0, snapshot3.Spaces, db)

	requireT.Equal([]int{2, 3, 4, 20, 21}, test.CollectSpaceValues(s30))

	nodesUsed, _, _ = allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x04, 0x07, 0x09, 0x0a, 0x0b, 0x0c, 0x0e, 0x0f}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x09}, s30.Nodes()) // Same as s20
	requireT.Equal([]types.NodeAddress{0x07}, snapshot1.Spaces.Nodes())
	requireT.Equal([]types.NodeAddress{0x0c}, snapshot2.Spaces.Nodes())
	requireT.Equal([]types.NodeAddress{0x0c}, snapshot3.Spaces.Nodes()) // Same as snapshot 2
	requireT.Equal([]types.NodeAddress{0x0f}, db.nextSnapshot.Snapshots.Nodes())
	requireT.Equal([]types.SnapshotID{0x01, 0x02, 0x03}, test.CollectSpaceKeys(db.nextSnapshot.Snapshots))
	requireT.Equal([]types.NodeAddress{0x0e}, snapshot1.DeallocationLists.Nodes())
	requireT.Equal([]types.NodeAddress{0x0b}, snapshot2.DeallocationLists.Nodes())
	requireT.Empty(snapshot3.DeallocationLists.Nodes())
	requireT.Empty(test.CollectSpaceKeys(snapshot1.DeallocationLists))
	requireT.Equal([]types.SnapshotID{0x01}, test.CollectSpaceKeys(snapshot2.DeallocationLists))
	dList21Address, exists = snapshot2.DeallocationLists.Get(0x01)
	requireT.True(exists)

	dList21 = newList(dList21Address, db)

	requireT.Equal([]types.NodeAddress{0x0a}, dList21.Nodes())
	requireT.Equal([]types.NodeAddress{0x04, 0x07}, test.CollectListItems(dList21))

	// Delete snapshot 1

	requireT.NoError(db.DeleteSnapshot(0x01))
	requireT.NoError(db.Commit())

	snapshot2 = newSnapshot(requireT, 0x02, db)
	snapshot3 = newSnapshot(requireT, 0x03, db)
	snapshot4 := newSnapshot(requireT, 0x04, db)

	requireT.Equal(types.SnapshotID(0x02), snapshot2.SnapshotInfo.PreviousSnapshotID)
	requireT.Equal(types.SnapshotID(0x03), snapshot2.SnapshotInfo.NextSnapshotID)
	requireT.Equal(types.SnapshotID(0x02), snapshot3.SnapshotInfo.PreviousSnapshotID)
	requireT.Equal(types.SnapshotID(0x04), snapshot3.SnapshotInfo.NextSnapshotID)
	requireT.Equal(types.SnapshotID(0x03), snapshot4.SnapshotInfo.PreviousSnapshotID)
	requireT.Equal(types.SnapshotID(0x05), snapshot4.SnapshotInfo.NextSnapshotID)
	requireT.Equal(types.SnapshotID(0x02), db.nextSnapshot.SingularityNode.FirstSnapshotID)
	requireT.Equal(types.SnapshotID(0x05), db.nextSnapshot.SingularityNode.LastSnapshotID)

	s40 := newSpace[int, int](requireT, space0, snapshot4.Spaces, db)

	requireT.Equal([]int{2, 3, 4, 20, 21}, test.CollectSpaceValues(s40))

	nodesUsed, _, _ = allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x09, 0x0c, 0x10, 0x11}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x09}, s40.Nodes())
	requireT.Equal([]types.NodeAddress{0x0c}, snapshot2.Spaces.Nodes())
	requireT.Equal([]types.NodeAddress{0x0c}, snapshot3.Spaces.Nodes()) // Same as snapshot 2
	requireT.Equal([]types.NodeAddress{0x0c}, snapshot4.Spaces.Nodes()) // Same as snapshot 2
	requireT.Equal([]types.NodeAddress{0x11}, db.nextSnapshot.Snapshots.Nodes())
	requireT.Equal([]types.SnapshotID{0x02, 0x03, 0x04}, test.CollectSpaceKeys(db.nextSnapshot.Snapshots))
	requireT.Equal([]types.NodeAddress{0x10}, snapshot2.DeallocationLists.Nodes())
	requireT.Empty(snapshot3.DeallocationLists.Nodes())
	requireT.Empty(snapshot4.DeallocationLists.Nodes())
	requireT.Empty(test.CollectSpaceKeys(snapshot2.DeallocationLists))
}

func TestDeleteLastSnapshot(t *testing.T) {
	requireT := require.New(t)
	db, allocator := newDB(requireT)

	// Snapshot 0

	s0, err := GetSpace[int, int](space0, db)
	requireT.NoError(err)

	requireT.NoError(s0.Set(0, 0))
	requireT.NoError(s0.Set(1, 1))
	requireT.NoError(s0.Set(2, 2))
	requireT.NoError(s0.Set(3, 3))
	requireT.NoError(s0.Set(4, 4))
	requireT.NoError(db.Commit())

	// Snapshot 1

	s1, err := GetSpace[int, int](space0, db)
	requireT.NoError(err)

	requireT.NoError(s1.Set(0, 10))
	requireT.NoError(db.Commit())

	// Snapshot 2

	s2, err := GetSpace[int, int](space0, db)
	requireT.NoError(err)

	requireT.NoError(s2.Set(0, 20))
	requireT.NoError(s2.Set(1, 21))
	requireT.NoError(db.Commit())

	// Check the initial state

	snapshot0 := newSnapshot(requireT, 0x00, db)
	snapshot1 := newSnapshot(requireT, 0x01, db)
	snapshot2 := newSnapshot(requireT, 0x02, db)

	requireT.Equal(types.SnapshotID(0x00), snapshot0.SnapshotInfo.PreviousSnapshotID)
	requireT.Equal(types.SnapshotID(0x01), snapshot0.SnapshotInfo.NextSnapshotID)
	requireT.Equal(types.SnapshotID(0x00), snapshot1.SnapshotInfo.PreviousSnapshotID)
	requireT.Equal(types.SnapshotID(0x02), snapshot1.SnapshotInfo.NextSnapshotID)
	requireT.Equal(types.SnapshotID(0x01), snapshot2.SnapshotInfo.PreviousSnapshotID)
	requireT.Equal(types.SnapshotID(0x03), snapshot2.SnapshotInfo.NextSnapshotID)
	requireT.Equal(types.SnapshotID(0x00), db.nextSnapshot.SingularityNode.FirstSnapshotID)
	requireT.Equal(types.SnapshotID(0x03), db.nextSnapshot.SingularityNode.LastSnapshotID)

	s20 := newSpace[int, int](requireT, space0, snapshot2.Spaces, db)

	requireT.Equal([]int{2, 3, 4, 20, 21}, test.CollectSpaceValues(s20))

	nodesUsed, _, _ := allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x01, 0x02, 0x04, 0x05, 0x06, 0x07, 0x09, 0x0a, 0x0b, 0x0c, 0x0d}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x09}, s20.Nodes())
	requireT.Equal([]types.NodeAddress{0x02}, snapshot0.Spaces.Nodes())
	requireT.Equal([]types.NodeAddress{0x07}, snapshot1.Spaces.Nodes())
	requireT.Equal([]types.NodeAddress{0x0c}, snapshot2.Spaces.Nodes())
	requireT.Equal([]types.NodeAddress{0x0d}, db.nextSnapshot.Snapshots.Nodes())
	requireT.Equal([]types.SnapshotID{0x00, 0x01, 0x02}, test.CollectSpaceKeys(db.nextSnapshot.Snapshots))
	requireT.Empty(snapshot0.DeallocationLists.Nodes())
	requireT.Equal([]types.NodeAddress{0x06}, snapshot1.DeallocationLists.Nodes())
	requireT.Equal([]types.NodeAddress{0x0b}, snapshot2.DeallocationLists.Nodes())
	requireT.Equal([]types.SnapshotID{0x00}, test.CollectSpaceKeys(snapshot1.DeallocationLists))
	requireT.Equal([]types.SnapshotID{0x01}, test.CollectSpaceKeys(snapshot2.DeallocationLists))
	dList21Address, exists := snapshot2.DeallocationLists.Get(0x01)
	requireT.True(exists)

	dList21 := newList(dList21Address, db)

	requireT.Equal([]types.NodeAddress{0x0a}, dList21.Nodes())
	requireT.Equal([]types.NodeAddress{0x04, 0x07}, test.CollectListItems(dList21))

	// Override data set in snapshot 2

	s3, err := GetSpace[int, int](space0, db)
	requireT.NoError(err)
	requireT.NoError(s3.Set(0, 30))
	requireT.NoError(s3.Set(1, 31))
	requireT.NoError(s3.Set(2, 32))

	// Delete snapshot 2

	requireT.NoError(db.DeleteSnapshot(0x02))

	snapshot0 = newSnapshot(requireT, 0x00, db)
	snapshot1 = newSnapshot(requireT, 0x01, db)

	requireT.Equal(types.SnapshotID(0x00), snapshot0.SnapshotInfo.PreviousSnapshotID)
	requireT.Equal(types.SnapshotID(0x01), snapshot0.SnapshotInfo.NextSnapshotID)
	requireT.Equal(types.SnapshotID(0x00), snapshot1.SnapshotInfo.PreviousSnapshotID)
	requireT.Equal(types.SnapshotID(0x03), snapshot1.SnapshotInfo.NextSnapshotID)
	requireT.Equal(types.SnapshotID(0x01), db.nextSnapshot.SnapshotInfo.PreviousSnapshotID)
	requireT.Equal(types.SnapshotID(0x04), db.nextSnapshot.SnapshotInfo.NextSnapshotID)
	requireT.Equal(types.SnapshotID(0x00), db.nextSnapshot.SingularityNode.FirstSnapshotID)
	requireT.Equal(types.SnapshotID(0x03), db.nextSnapshot.SingularityNode.LastSnapshotID)

	s30, err := GetSpace[int, int](space0, db)
	requireT.NoError(err)

	requireT.Equal([]int{3, 4, 30, 31, 32}, test.CollectSpaceValues(s30))

	nodesUsed, _, _ = allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x01, 0x02, 0x04, 0x05, 0x06, 0x07, 0x0a, 0x0c, 0x0e, 0x10, 0x11}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x0e}, s30.Nodes())
	requireT.Equal([]types.NodeAddress{0x02}, snapshot0.Spaces.Nodes())
	requireT.Equal([]types.NodeAddress{0x07}, snapshot1.Spaces.Nodes())
	requireT.Equal([]types.NodeAddress{0x0c}, db.nextSnapshot.Spaces.Nodes())
	requireT.Equal([]types.NodeAddress{0x11}, db.nextSnapshot.Snapshots.Nodes())
	requireT.Equal([]types.SnapshotID{0x00, 0x01}, test.CollectSpaceKeys(db.nextSnapshot.Snapshots))
	requireT.Empty(snapshot0.DeallocationLists.Nodes())
	requireT.Equal([]types.NodeAddress{0x06}, snapshot1.DeallocationLists.Nodes())
	requireT.Equal([]types.NodeAddress{0x10}, db.nextSnapshot.DeallocationLists.Nodes())
	requireT.Equal([]types.SnapshotID{0x00}, test.CollectSpaceKeys(snapshot1.DeallocationLists))
	requireT.Equal([]types.SnapshotID{0x01}, test.CollectSpaceKeys(db.nextSnapshot.DeallocationLists))
	dList31Address, exists := db.nextSnapshot.DeallocationLists.Get(0x01)
	requireT.True(exists)

	dList31 := newList(dList31Address, db)

	requireT.Equal([]types.NodeAddress{0x0a}, dList31.Nodes())
	requireT.Equal([]types.NodeAddress{0x04, 0x07}, test.CollectListItems(dList31))

	// Delete snapshot 1

	requireT.NoError(db.DeleteSnapshot(0x01))

	snapshot0 = newSnapshot(requireT, 0x00, db)

	requireT.Equal(types.SnapshotID(0x00), snapshot0.SnapshotInfo.PreviousSnapshotID)
	requireT.Equal(types.SnapshotID(0x03), snapshot0.SnapshotInfo.NextSnapshotID)
	requireT.Equal(types.SnapshotID(0x00), db.nextSnapshot.SnapshotInfo.PreviousSnapshotID)
	requireT.Equal(types.SnapshotID(0x04), db.nextSnapshot.SnapshotInfo.NextSnapshotID)
	requireT.Equal(types.SnapshotID(0x00), db.nextSnapshot.SingularityNode.FirstSnapshotID)
	requireT.Equal(types.SnapshotID(0x03), db.nextSnapshot.SingularityNode.LastSnapshotID)

	requireT.Equal([]int{3, 4, 30, 31, 32}, test.CollectSpaceValues(s30))

	nodesUsed, _, _ = allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x01, 0x02, 0x05, 0x0c, 0x0e, 0x10, 0x11}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x0e}, s30.Nodes())
	requireT.Equal([]types.NodeAddress{0x02}, snapshot0.Spaces.Nodes())
	requireT.Equal([]types.NodeAddress{0x0c}, db.nextSnapshot.Spaces.Nodes())
	requireT.Equal([]types.NodeAddress{0x11}, db.nextSnapshot.Snapshots.Nodes())
	requireT.Equal([]types.SnapshotID{0x00}, test.CollectSpaceKeys(db.nextSnapshot.Snapshots))
	requireT.Empty(snapshot0.DeallocationLists.Nodes())
	requireT.Equal([]types.NodeAddress{0x10}, db.nextSnapshot.DeallocationLists.Nodes())
	requireT.Equal([]types.SnapshotID{0x00}, test.CollectSpaceKeys(db.nextSnapshot.DeallocationLists))
	dList30Address, exists := db.nextSnapshot.DeallocationLists.Get(0x00)
	requireT.True(exists)

	dList30 := newList(dList30Address, db)

	requireT.Equal([]types.NodeAddress{0x05}, dList30.Nodes())
	requireT.Equal([]types.NodeAddress{0x01, 0x02}, test.CollectListItems(dList30))
}

func TestDeleteTwoMiddleSnapshots(t *testing.T) {
	requireT := require.New(t)
	db, allocator := newDB(requireT)

	// Prepare snapshots

	// Snapshot 0

	s0, err := GetSpace[int, int](space0, db)
	requireT.NoError(err)

	requireT.NoError(s0.Set(0, 0))
	requireT.NoError(s0.Set(1, 1))
	requireT.NoError(s0.Set(2, 2))
	requireT.NoError(s0.Set(3, 3))
	requireT.NoError(s0.Set(4, 4))
	requireT.NoError(db.Commit())

	// Snapshot 1

	s1, err := GetSpace[int, int](space0, db)
	requireT.NoError(err)

	requireT.NoError(s1.Set(0, 10))
	requireT.NoError(db.Commit())

	// Snapshot 2

	s2, err := GetSpace[int, int](space0, db)
	requireT.NoError(err)

	requireT.NoError(s2.Set(0, 20))
	requireT.NoError(s2.Set(1, 21))
	requireT.NoError(db.Commit())

	// Snapshot 3

	s3, err := GetSpace[int, int](space0, db)
	requireT.NoError(err)

	requireT.NoError(s3.Set(0, 30))
	requireT.NoError(s3.Set(1, 31))
	requireT.NoError(s3.Set(2, 32))
	requireT.NoError(db.Commit())

	requireT.Equal([]int{3, 4, 30, 31, 32}, test.CollectSpaceValues(s3))

	snapshot0 := newSnapshot(requireT, 0x00, db)
	snapshot1 := newSnapshot(requireT, 0x01, db)
	snapshot2 := newSnapshot(requireT, 0x02, db)
	snapshot3 := newSnapshot(requireT, 0x03, db)

	requireT.Equal(types.SnapshotID(0x00), snapshot0.SnapshotInfo.PreviousSnapshotID)
	requireT.Equal(types.SnapshotID(0x01), snapshot0.SnapshotInfo.NextSnapshotID)
	requireT.Equal(types.SnapshotID(0x00), snapshot1.SnapshotInfo.PreviousSnapshotID)
	requireT.Equal(types.SnapshotID(0x02), snapshot1.SnapshotInfo.NextSnapshotID)
	requireT.Equal(types.SnapshotID(0x01), snapshot2.SnapshotInfo.PreviousSnapshotID)
	requireT.Equal(types.SnapshotID(0x03), snapshot2.SnapshotInfo.NextSnapshotID)
	requireT.Equal(types.SnapshotID(0x02), snapshot3.SnapshotInfo.PreviousSnapshotID)
	requireT.Equal(types.SnapshotID(0x04), snapshot3.SnapshotInfo.NextSnapshotID)
	requireT.Equal(types.SnapshotID(0x00), db.nextSnapshot.SingularityNode.FirstSnapshotID)
	requireT.Equal(types.SnapshotID(0x04), db.nextSnapshot.SingularityNode.LastSnapshotID)

	nodesUsed, _, _ := allocator.Nodes()
	requireT.Equal([]types.NodeAddress{
		0x01, 0x02, 0x04, 0x05, 0x06, 0x07, 0x09, 0x0a, 0x0b, 0x0c, 0x0e, 0x0f, 0x10, 0x11, 0x12,
	}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x01}, s0.Nodes())
	requireT.Equal([]types.NodeAddress{0x04}, s1.Nodes())
	requireT.Equal([]types.NodeAddress{0x09}, s2.Nodes())
	requireT.Equal([]types.NodeAddress{0x0e}, s3.Nodes())
	requireT.Equal([]types.NodeAddress{0x02}, snapshot0.Spaces.Nodes())
	requireT.Equal([]types.NodeAddress{0x07}, snapshot1.Spaces.Nodes())
	requireT.Equal([]types.NodeAddress{0x0c}, snapshot2.Spaces.Nodes())
	requireT.Equal([]types.NodeAddress{0x11}, snapshot3.Spaces.Nodes())
	requireT.Equal([]types.NodeAddress{0x12}, db.nextSnapshot.Snapshots.Nodes())
	requireT.Equal([]types.SnapshotID{0x00, 0x01, 0x02, 0x03}, test.CollectSpaceKeys(db.nextSnapshot.Snapshots))
	requireT.Empty(snapshot0.DeallocationLists.Nodes())
	requireT.Equal([]types.NodeAddress{0x06}, snapshot1.DeallocationLists.Nodes())
	requireT.Equal([]types.NodeAddress{0x0b}, snapshot2.DeallocationLists.Nodes())
	requireT.Equal([]types.NodeAddress{0x10}, snapshot3.DeallocationLists.Nodes())
	requireT.Equal([]types.SnapshotID{0x00}, test.CollectSpaceKeys(snapshot1.DeallocationLists))
	requireT.Equal([]types.SnapshotID{0x01}, test.CollectSpaceKeys(snapshot2.DeallocationLists))
	requireT.Equal([]types.SnapshotID{0x02}, test.CollectSpaceKeys(snapshot3.DeallocationLists))
	dList10Address, exists := snapshot1.DeallocationLists.Get(0x00)
	requireT.True(exists)
	dList21Address, exists := snapshot2.DeallocationLists.Get(0x01)
	requireT.True(exists)
	dList32Address, exists := snapshot3.DeallocationLists.Get(0x02)
	requireT.True(exists)

	dList10 := newList(dList10Address, db)
	dList21 := newList(dList21Address, db)
	dList32 := newList(dList32Address, db)

	requireT.Equal([]types.NodeAddress{0x05}, dList10.Nodes())
	requireT.Equal([]types.NodeAddress{0x0a}, dList21.Nodes())
	requireT.Equal([]types.NodeAddress{0x0f}, dList32.Nodes())
	requireT.Equal([]types.NodeAddress{0x01, 0x02}, test.CollectListItems(dList10))
	requireT.Equal([]types.NodeAddress{0x04, 0x07}, test.CollectListItems(dList21))
	requireT.Equal([]types.NodeAddress{0x09, 0x0c}, test.CollectListItems(dList32))

	// Delete snapshot 2
	// - snapshot 2 deallocation list should be deallocated from snapshot 3
	// - snapshot 0 and 1 deallocation lists from snapshot 2 should be attached to snapshot 3
	// - deallocation list nodes of snapshot 2 should be deallocated

	requireT.NoError(db.DeleteSnapshot(0x02))

	// Verify next state.

	requireT.Equal([]types.SnapshotID{0x00, 0x01, 0x03}, test.CollectSpaceKeys(db.nextSnapshot.Snapshots))

	// Verify that expected nodes were allocated and deallocated.

	snapshot0 = newSnapshot(requireT, 0x00, db)
	snapshot1 = newSnapshot(requireT, 0x01, db)
	snapshot3 = newSnapshot(requireT, 0x03, db)

	requireT.Equal(types.SnapshotID(0x00), snapshot0.SnapshotInfo.PreviousSnapshotID)
	requireT.Equal(types.SnapshotID(0x01), snapshot0.SnapshotInfo.NextSnapshotID)
	requireT.Equal(types.SnapshotID(0x00), snapshot1.SnapshotInfo.PreviousSnapshotID)
	requireT.Equal(types.SnapshotID(0x03), snapshot1.SnapshotInfo.NextSnapshotID)
	requireT.Equal(types.SnapshotID(0x01), snapshot3.SnapshotInfo.PreviousSnapshotID)
	requireT.Equal(types.SnapshotID(0x04), snapshot3.SnapshotInfo.NextSnapshotID)
	requireT.Equal(types.SnapshotID(0x00), db.nextSnapshot.SingularityNode.FirstSnapshotID)
	requireT.Equal(types.SnapshotID(0x04), db.nextSnapshot.SingularityNode.LastSnapshotID)

	s30 := newSpace[int, int](requireT, space0, snapshot3.Spaces, db)
	nodesUsed, nodesAllocated, nodesDeallocated := allocator.Nodes()

	requireT.Equal([]types.NodeAddress{0x01, 0x02, 0x04, 0x05, 0x06, 0x07, 0x0a, 0x0e, 0x11, 0x13, 0x14}, nodesUsed)
	requireT.Equal([]types.NodeAddress{0x13, 0x14}, nodesAllocated)
	requireT.Equal([]types.NodeAddress{0x09, 0x0b, 0x0c, 0x0f, 0x10, 0x12}, nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x14}, db.nextSnapshot.Snapshots.Nodes())
	requireT.Equal([]types.NodeAddress{0x02}, snapshot0.Spaces.Nodes())
	requireT.Equal([]types.NodeAddress{0x07}, snapshot1.Spaces.Nodes())
	requireT.Equal([]types.NodeAddress{0x11}, snapshot3.Spaces.Nodes())
	requireT.Equal([]types.NodeAddress{0x0e}, s30.Nodes())
	requireT.Empty(snapshot0.DeallocationLists.Nodes())
	requireT.Equal([]types.NodeAddress{0x06}, snapshot1.DeallocationLists.Nodes())
	requireT.Equal([]types.NodeAddress{0x13}, snapshot3.DeallocationLists.Nodes())
	requireT.Equal([]types.SnapshotID{0x00}, test.CollectSpaceKeys(snapshot1.DeallocationLists))
	requireT.Equal([]types.SnapshotID{0x01}, test.CollectSpaceKeys(snapshot3.DeallocationLists))
	dList31Address, exists := snapshot3.DeallocationLists.Get(0x01)
	requireT.True(exists)
	dList31 := newList(dList31Address, db)
	requireT.Equal([]types.NodeAddress{0x0a}, dList31.Nodes())
	requireT.Equal([]types.NodeAddress{0x04, 0x07}, test.CollectListItems(dList31))

	// Delete snapshot 1
	// - snapshot 1 deallocation list should be deallocated from snapshot 3
	// - snapshot 0 deallocation list from snapshot 1 should be attached to snapshot 3
	// - deallocation list nodes of snapshot 1 should be deallocated

	requireT.NoError(db.DeleteSnapshot(0x01))
	requireT.NoError(db.Commit())

	// Verify next state.

	requireT.Equal([]types.SnapshotID{0x00, 0x03, 0x04}, test.CollectSpaceKeys(db.nextSnapshot.Snapshots))

	// Verify that expected nodes were allocated and deallocated.

	snapshot0 = newSnapshot(requireT, 0x00, db)
	snapshot3 = newSnapshot(requireT, 0x03, db)
	snapshot4 := newSnapshot(requireT, 0x04, db)

	requireT.Equal(types.SnapshotID(0x00), snapshot0.SnapshotInfo.PreviousSnapshotID)
	requireT.Equal(types.SnapshotID(0x03), snapshot0.SnapshotInfo.NextSnapshotID)
	requireT.Equal(types.SnapshotID(0x00), snapshot3.SnapshotInfo.PreviousSnapshotID)
	requireT.Equal(types.SnapshotID(0x04), snapshot3.SnapshotInfo.NextSnapshotID)
	requireT.Equal(types.SnapshotID(0x03), snapshot4.SnapshotInfo.PreviousSnapshotID)
	requireT.Equal(types.SnapshotID(0x05), snapshot4.SnapshotInfo.NextSnapshotID)
	requireT.Equal(types.SnapshotID(0x00), db.nextSnapshot.SingularityNode.FirstSnapshotID)
	requireT.Equal(types.SnapshotID(0x05), db.nextSnapshot.SingularityNode.LastSnapshotID)

	s30 = newSpace[int, int](requireT, space0, snapshot3.Spaces, db)
	s40 := newSpace[int, int](requireT, space0, snapshot4.Spaces, db)

	nodesUsed, nodesAllocated, nodesDeallocated = allocator.Nodes()
	requireT.Equal([]types.NodeAddress{0x01, 0x02, 0x05, 0x0e, 0x11, 0x13, 0x14}, nodesUsed)
	requireT.Empty(nodesAllocated)
	requireT.Equal([]types.NodeAddress{0x04, 0x06, 0x07, 0x0a}, nodesDeallocated)
	requireT.Equal([]types.NodeAddress{0x14}, db.nextSnapshot.Snapshots.Nodes())
	requireT.Equal([]types.NodeAddress{0x02}, snapshot0.Spaces.Nodes())
	requireT.Equal([]types.NodeAddress{0x11}, snapshot3.Spaces.Nodes())
	// 0x1f is same as for snapshot 3 because spaces were not updated
	requireT.Equal([]types.NodeAddress{0x11}, snapshot4.Spaces.Nodes())
	requireT.Equal([]types.NodeAddress{0x0e}, s30.Nodes())
	// Same as for snapshot 3 because spaces were not updated
	requireT.Equal([]types.NodeAddress{0x0e}, s40.Nodes())
	requireT.Empty(snapshot0.DeallocationLists.Nodes())
	requireT.Equal([]types.NodeAddress{0x13}, snapshot3.DeallocationLists.Nodes())
	requireT.Empty(snapshot4.DeallocationLists.Nodes())
	requireT.Equal([]types.SnapshotID{0x00}, test.CollectSpaceKeys(snapshot3.DeallocationLists))
	dList30Address, exists := snapshot3.DeallocationLists.Get(0x00)
	requireT.True(exists)
	dList30 := newList(dList30Address, db)
	requireT.Equal([]types.NodeAddress{0x05}, dList30.Nodes())
	requireT.Equal([]types.NodeAddress{0x01, 0x02}, test.CollectListItems(dList30))
}

func newDB(requireT *require.Assertions) (*DB, *test.Allocator) {
	allocator := test.NewAllocator(test.AllocatorConfig{
		TotalSize: 1024 * 1024,
		NodeSize:  512,
	})
	db, err := New(Config{
		Allocator: allocator,
	})
	requireT.NoError(err)
	return db, allocator
}

type snapshot struct {
	SnapshotInfo      types.SnapshotInfo
	Spaces            *space.Space[types.SpaceID, types.SpaceInfo]
	DeallocationLists *space.Space[types.SnapshotID, types.NodeAddress]
}

func newSnapshot(
	requireT *require.Assertions,
	snapshotID types.SnapshotID,
	db *DB,
) snapshot {
	snapshotInfo, exists := db.nextSnapshot.Snapshots.Get(snapshotID)
	requireT.True(exists)

	dataNodeAllocator, err := space.NewNodeAllocator[types.DataItem[types.SpaceID, types.SpaceInfo]](db.config.Allocator)
	requireT.NoError(err)

	return snapshot{
		SnapshotInfo: snapshotInfo,
		Spaces: space.New[types.SpaceID, types.SpaceInfo](
			space.Config[types.SpaceID, types.SpaceInfo]{
				SnapshotID: snapshotID,
				HashMod:    &snapshotInfo.SpaceRoot.HashMod,
				SpaceRoot: types.ParentInfo{
					State:   &snapshotInfo.SpaceRoot.State,
					Pointer: &snapshotInfo.SpaceRoot.Pointer,
				},
				PointerNodeAllocator: db.pointerNodeAllocator,
				DataNodeAllocator:    dataNodeAllocator,
			},
		),
		DeallocationLists: space.New[types.SnapshotID, types.NodeAddress](
			space.Config[types.SnapshotID, types.NodeAddress]{
				SnapshotID: snapshotID,
				HashMod:    &snapshotInfo.DeallocationRoot.HashMod,
				SpaceRoot: types.ParentInfo{
					State:   &snapshotInfo.DeallocationRoot.State,
					Pointer: &snapshotInfo.DeallocationRoot.Pointer,
				},
				PointerNodeAllocator: db.pointerNodeAllocator,
				DataNodeAllocator:    db.snapshotToNodeNodeAllocator,
			},
		),
	}
}

func newSpace[K, V comparable](
	requireT *require.Assertions,
	spaceID types.SpaceID,
	spaces *space.Space[types.SpaceID, types.SpaceInfo],
	db *DB,
) *space.Space[K, V] {
	spaceInfo, exists := spaces.Get(spaceID)
	requireT.True(exists)

	dataNodeAllocator, err := space.NewNodeAllocator[types.DataItem[K, V]](db.config.Allocator)
	requireT.NoError(err)

	return space.New[K, V](space.Config[K, V]{
		HashMod: &spaceInfo.HashMod,
		SpaceRoot: types.ParentInfo{
			State:   &spaceInfo.State,
			Pointer: &spaceInfo.Pointer,
		},
		PointerNodeAllocator: db.pointerNodeAllocator,
		DataNodeAllocator:    dataNodeAllocator,
	})
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
