package quantum

import (
	"sort"

	"github.com/pkg/errors"
	"github.com/samber/lo"

	"github.com/outofforest/photon"
	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/list"
	"github.com/outofforest/quantum/space"
	"github.com/outofforest/quantum/types"
)

// Config stores snapshot configuration.
type Config struct {
	Allocator types.Allocator
}

// SpaceToCommit represents requested space which might require to be committed.
type SpaceToCommit struct {
	HashMod      *uint64
	PInfo        types.ParentInfo
	OriginalItem types.NodeAddress
}

// Snapshot represents snapshot.
type Snapshot struct {
	SnapshotID      types.SnapshotID
	SingularityNode *types.SingularityNode
	SnapshotInfo    *types.SnapshotInfo
	Snapshots       *space.Space[types.SnapshotID, types.SnapshotInfo]
	Spaces          *space.Space[types.SpaceID, types.SpaceInfo]
	SpacesToCommit  map[types.SpaceID]SpaceToCommit
	Allocator       types.SnapshotAllocator
}

// New creates new database.
func New(config Config) (*DB, error) {
	pointerNodeAllocator, err := space.NewNodeAllocator[types.NodeAddress](config.Allocator)
	if err != nil {
		return nil, err
	}

	snapshotInfoNodeAllocator, err := space.NewNodeAllocator[types.DataItem[types.SnapshotID, types.SnapshotInfo]](
		config.Allocator,
	)
	if err != nil {
		return nil, err
	}

	snapshotToNodeNodeAllocator, err := space.NewNodeAllocator[types.DataItem[types.SnapshotID, types.NodeAddress]](
		config.Allocator,
	)
	if err != nil {
		return nil, err
	}

	spaceInfoNodeAllocator, err := space.NewNodeAllocator[types.DataItem[types.SpaceID, types.SpaceInfo]](config.Allocator)
	if err != nil {
		return nil, err
	}

	listNodeAllocator, err := list.NewNodeAllocator(config.Allocator)
	if err != nil {
		return nil, err
	}

	s := &DB{
		config:                      config,
		pointerNodeAllocator:        pointerNodeAllocator,
		snapshotInfoNodeAllocator:   snapshotInfoNodeAllocator,
		spaceInfoNodeAllocator:      spaceInfoNodeAllocator,
		snapshotToNodeNodeAllocator: snapshotToNodeNodeAllocator,
		listNodeAllocator:           listNodeAllocator,
	}
	if err := s.prepareNextSnapshot(*photon.FromBytes[types.SingularityNode](config.Allocator.Node(0))); err != nil {
		return nil, err
	}
	return s, nil
}

// DB represents the database.
type DB struct {
	config       Config
	nextSnapshot Snapshot

	pointerNodeAllocator        space.NodeAllocator[types.NodeAddress]
	snapshotInfoNodeAllocator   space.NodeAllocator[types.DataItem[types.SnapshotID, types.SnapshotInfo]]
	spaceInfoNodeAllocator      space.NodeAllocator[types.DataItem[types.SpaceID, types.SpaceInfo]]
	snapshotToNodeNodeAllocator space.NodeAllocator[types.DataItem[types.SnapshotID, types.NodeAddress]]
	listNodeAllocator           list.NodeAllocator
}

// DeleteSnapshot deletes snapshot.
func (db *DB) DeleteSnapshot(snapshotID types.SnapshotID) error {
	// FIXME (wojciech): Deallocation of last snapshot

	snapshotInfo, exists := db.nextSnapshot.Snapshots.Get(snapshotID)
	if !exists {
		return errors.Errorf("snapshot %d does not exist", snapshotID)
	}

	//nolint:nestif
	if snapshotInfo.NextSnapshotID <= db.nextSnapshot.SingularityNode.LastSnapshotID {
		nextSnapshotInfo, exists := db.nextSnapshot.Snapshots.Get(snapshotInfo.NextSnapshotID)
		if !exists {
			return errors.Errorf("snapshot %d does not exist", snapshotID)
		}

		deallocationLists := space.New[types.SnapshotID, types.NodeAddress](space.Config[types.SnapshotID, types.NodeAddress]{
			SpaceRoot: types.ParentInfo{
				State: lo.ToPtr(snapshotInfo.DeallocationRoot.State),
				Item:  lo.ToPtr(snapshotInfo.DeallocationRoot.Node),
			},
			PointerNodeAllocator: db.pointerNodeAllocator,
			DataNodeAllocator:    db.snapshotToNodeNodeAllocator,
		})

		nextDeallocationLists := space.New[types.SnapshotID, types.NodeAddress](
			space.Config[types.SnapshotID, types.NodeAddress]{
				SnapshotID: snapshotInfo.NextSnapshotID,
				HashMod:    &nextSnapshotInfo.DeallocationRoot.HashMod,
				SpaceRoot: types.ParentInfo{
					State: &nextSnapshotInfo.DeallocationRoot.State,
					Item:  &nextSnapshotInfo.DeallocationRoot.Node,
				},
				PointerNodeAllocator: db.pointerNodeAllocator,
				DataNodeAllocator:    db.snapshotToNodeNodeAllocator,
				Allocator:            db.nextSnapshot.Allocator,
			},
		)

		var startSnapshotID types.SnapshotID
		if snapshotID == db.nextSnapshot.SingularityNode.FirstSnapshotID {
			startSnapshotID = snapshotID
		} else {
			startSnapshotID = snapshotInfo.PreviousSnapshotID + 1
		}

		for sID := startSnapshotID; sID <= snapshotID; sID++ {
			listNodeAddress, exists := nextDeallocationLists.Get(sID)
			if !exists {
				continue
			}

			list := list.New(list.Config{
				Item:          &listNodeAddress,
				NodeAllocator: db.listNodeAllocator,
			})
			list.Deallocate(db.config.Allocator)
			if err := nextDeallocationLists.Delete(sID); err != nil {
				return err
			}
		}

		// FIXME (wojciech): Iterate over space instead
		for sID := db.nextSnapshot.SingularityNode.FirstSnapshotID; sID < snapshotID; sID++ {
			listNodeAddress, exists := deallocationLists.Get(sID)
			if !exists {
				continue
			}

			nextListNodeAddress, _ := nextDeallocationLists.Get(sID)
			newNextListNodeAddress := nextListNodeAddress
			nextList := list.New(list.Config{
				Item: &newNextListNodeAddress,
			})
			if err := nextList.Attach(listNodeAddress); err != nil {
				return err
			}
			if newNextListNodeAddress != nextListNodeAddress {
				if err := nextDeallocationLists.Set(sID, newNextListNodeAddress); err != nil {
					return err
				}
			}
		}

		nextSnapshotInfo.PreviousSnapshotID = snapshotInfo.PreviousSnapshotID
		if err := db.nextSnapshot.Snapshots.Set(snapshotInfo.NextSnapshotID, nextSnapshotInfo); err != nil {
			return err
		}
	}

	if snapshotInfo.PreviousSnapshotID > db.nextSnapshot.SingularityNode.FirstSnapshotID {
		previousSnapshotInfo, exists := db.nextSnapshot.Snapshots.Get(snapshotInfo.PreviousSnapshotID)
		if !exists {
			return errors.Errorf("snapshot %d does not exist", snapshotID)
		}
		previousSnapshotInfo.NextSnapshotID = snapshotInfo.NextSnapshotID
		if err := db.nextSnapshot.Snapshots.Set(snapshotInfo.PreviousSnapshotID, previousSnapshotInfo); err != nil {
			return err
		}
	}

	if snapshotID == db.nextSnapshot.SingularityNode.FirstSnapshotID {
		db.nextSnapshot.SingularityNode.FirstSnapshotID = snapshotInfo.NextSnapshotID
	}

	// FIXME (wojciech): Deallocate nodes used by deleted snapshot (DeallocationRoot).

	return nil
}

// Commit commits current snapshot and returns next one.
func (db *DB) Commit() error {
	spaces := make([]types.SpaceID, 0, len(db.nextSnapshot.SpacesToCommit))
	for spaceID := range db.nextSnapshot.SpacesToCommit {
		spaces = append(spaces, spaceID)
	}
	sort.Slice(spaces, func(i, j int) bool { return spaces[i] < spaces[j] })

	for _, spaceID := range spaces {
		spaceToCommit := db.nextSnapshot.SpacesToCommit[spaceID]
		if *spaceToCommit.PInfo.State == types.StateFree || *spaceToCommit.PInfo.Item == spaceToCommit.OriginalItem {
			continue
		}
		if err := db.nextSnapshot.Spaces.Set(spaceID, types.SpaceInfo{
			HashMod: *spaceToCommit.HashMod,
			State:   *spaceToCommit.PInfo.State,
			Node:    *spaceToCommit.PInfo.Item,
		}); err != nil {
			return err
		}
	}

	if err := db.nextSnapshot.Snapshots.Set(db.nextSnapshot.SnapshotID, *db.nextSnapshot.SnapshotInfo); err != nil {
		return err
	}

	*photon.FromBytes[types.SingularityNode](db.config.Allocator.Node(0)) = *db.nextSnapshot.SingularityNode

	return db.prepareNextSnapshot(*db.nextSnapshot.SingularityNode)
}

func (db *DB) prepareNextSnapshot(singularityNode types.SingularityNode) error {
	var snapshotID types.SnapshotID
	if singularityNode.SnapshotRoot.State != types.StateFree {
		snapshotID = singularityNode.LastSnapshotID + 1
	}

	snapshotInfo := types.SnapshotInfo{
		PreviousSnapshotID: singularityNode.LastSnapshotID,
		NextSnapshotID:     snapshotID + 1,
	}

	deallocationLists := &space.Space[types.SnapshotID, types.NodeAddress]{}
	allocator := alloc.NewSnapshotAllocator(
		snapshotID,
		db.config.Allocator,
		deallocationLists,
		db.listNodeAllocator,
	)
	*deallocationLists = *space.New[types.SnapshotID, types.NodeAddress](space.Config[types.SnapshotID, types.NodeAddress]{
		SnapshotID: snapshotID,
		HashMod:    &snapshotInfo.DeallocationRoot.HashMod,
		SpaceRoot: types.ParentInfo{
			State: &snapshotInfo.DeallocationRoot.State,
			Item:  &snapshotInfo.DeallocationRoot.Node,
		},
		PointerNodeAllocator: db.pointerNodeAllocator,
		DataNodeAllocator:    db.snapshotToNodeNodeAllocator,
		Allocator:            allocator,
	})

	snapshots := space.New[types.SnapshotID, types.SnapshotInfo](space.Config[types.SnapshotID, types.SnapshotInfo]{
		SnapshotID: snapshotID,
		HashMod:    &singularityNode.SnapshotRoot.HashMod,
		SpaceRoot: types.ParentInfo{
			State: &singularityNode.SnapshotRoot.State,
			Item:  &singularityNode.SnapshotRoot.Node,
		},
		PointerNodeAllocator: db.pointerNodeAllocator,
		DataNodeAllocator:    db.snapshotInfoNodeAllocator,
		Allocator:            allocator,
	})

	if singularityNode.SnapshotRoot.State != types.StateFree {
		lastSnapshotInfo, exists := snapshots.Get(singularityNode.LastSnapshotID)
		if !exists {
			return errors.Errorf("snapshot %d does not exist", singularityNode.LastSnapshotID)
		}
		snapshotInfo.SpaceRoot = lastSnapshotInfo.SpaceRoot
		snapshotInfo.DeallocationRoot = lastSnapshotInfo.DeallocationRoot
	}

	singularityNode.LastSnapshotID = snapshotID

	db.nextSnapshot = Snapshot{
		SnapshotID:      snapshotID,
		SingularityNode: &singularityNode,
		SnapshotInfo:    &snapshotInfo,
		Snapshots:       snapshots,
		Spaces: space.New[types.SpaceID, types.SpaceInfo](space.Config[types.SpaceID, types.SpaceInfo]{
			SnapshotID: snapshotID,
			HashMod:    &snapshotInfo.SpaceRoot.HashMod,
			SpaceRoot: types.ParentInfo{
				State: &snapshotInfo.SpaceRoot.State,
				Item:  &snapshotInfo.SpaceRoot.Node,
			},
			PointerNodeAllocator: db.pointerNodeAllocator,
			DataNodeAllocator:    db.spaceInfoNodeAllocator,
			Allocator:            allocator,
		}),
		SpacesToCommit: map[types.SpaceID]SpaceToCommit{},
		Allocator:      allocator,
	}

	return nil
}

// GetSpace retrieves space from snapshot.
func GetSpace[K, V comparable](spaceID types.SpaceID, db *DB) (*space.Space[K, V], error) {
	s, exists := db.nextSnapshot.SpacesToCommit[spaceID]
	if !exists {
		spaceInfo, _ := db.nextSnapshot.Spaces.Get(spaceID)
		s = SpaceToCommit{
			HashMod: &spaceInfo.HashMod,
			PInfo: types.ParentInfo{
				State: &spaceInfo.State,
				Item:  &spaceInfo.Node,
			},
			OriginalItem: spaceInfo.Node,
		}
		db.nextSnapshot.SpacesToCommit[spaceID] = s
	}

	dataNodeAllocator, err := space.NewNodeAllocator[types.DataItem[K, V]](db.config.Allocator)
	if err != nil {
		return nil, err
	}

	return space.New[K, V](space.Config[K, V]{
		SnapshotID:           db.nextSnapshot.SnapshotID,
		HashMod:              s.HashMod,
		SpaceRoot:            s.PInfo,
		PointerNodeAllocator: db.pointerNodeAllocator,
		DataNodeAllocator:    dataNodeAllocator,
		Allocator:            db.nextSnapshot.Allocator,
	}), nil
}
