package quantum

import (
	"sort"

	"github.com/pkg/errors"
	"github.com/samber/lo"

	"github.com/outofforest/photon"
)

// Config stores snapshot configuration.
type Config struct {
	Allocator *Allocator
}

// SpaceToCommit represents requested space which might require to be committed.
type SpaceToCommit struct {
	HashMod      *uint64
	PInfo        ParentInfo
	OriginalItem NodeAddress
}

// Snapshot represents snapshot.
type Snapshot struct {
	SnapshotID      SnapshotID
	SingularityNode *SingularityNode
	SnapshotInfo    *SnapshotInfo
	Snapshots       *Space[SnapshotID, SnapshotInfo]
	Spaces          *Space[SpaceID, SpaceInfo]
	SpacesToCommit  map[SpaceID]SpaceToCommit
	Allocator       SnapshotAllocator
}

// New creates new database.
func New(config Config) (*DB, error) {
	pointerNodeAllocator, err := NewSpaceNodeAllocator[NodeAddress](config.Allocator)
	if err != nil {
		return nil, err
	}

	snapshotInfoNodeAllocator, err := NewSpaceNodeAllocator[DataItem[SnapshotID, SnapshotInfo]](config.Allocator)
	if err != nil {
		return nil, err
	}

	snapshotToNodeNodeAllocator, err := NewSpaceNodeAllocator[DataItem[SnapshotID, NodeAddress]](config.Allocator)
	if err != nil {
		return nil, err
	}

	spaceInfoNodeAllocator, err := NewSpaceNodeAllocator[DataItem[SpaceID, SpaceInfo]](config.Allocator)
	if err != nil {
		return nil, err
	}

	listNodeAllocator, err := NewListNodeAllocator(config.Allocator)
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
	if err := s.prepareNextSnapshot(*photon.FromBytes[SingularityNode](config.Allocator.Node(0))); err != nil {
		return nil, err
	}
	return s, nil
}

// DB represents the database.
type DB struct {
	config       Config
	nextSnapshot Snapshot

	pointerNodeAllocator        SpaceNodeAllocator[NodeAddress]
	snapshotInfoNodeAllocator   SpaceNodeAllocator[DataItem[SnapshotID, SnapshotInfo]]
	spaceInfoNodeAllocator      SpaceNodeAllocator[DataItem[SpaceID, SpaceInfo]]
	snapshotToNodeNodeAllocator SpaceNodeAllocator[DataItem[SnapshotID, NodeAddress]]
	listNodeAllocator           ListNodeAllocator
}

// DeleteSnapshot deletes snapshot.
func (db *DB) DeleteSnapshot(snapshotID SnapshotID) error {
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

		deallocationLists := NewSpace[SnapshotID, NodeAddress](SpaceConfig[SnapshotID, NodeAddress]{
			SpaceRoot: ParentInfo{
				State: lo.ToPtr(snapshotInfo.DeallocationRoot.State),
				Item:  lo.ToPtr(snapshotInfo.DeallocationRoot.Node),
			},
			PointerNodeAllocator: db.pointerNodeAllocator,
			DataNodeAllocator:    db.snapshotToNodeNodeAllocator,
		})

		nextDeallocationLists := NewSpace[SnapshotID, NodeAddress](SpaceConfig[SnapshotID, NodeAddress]{
			SnapshotID: snapshotInfo.NextSnapshotID,
			HashMod:    &nextSnapshotInfo.DeallocationRoot.HashMod,
			SpaceRoot: ParentInfo{
				State: &nextSnapshotInfo.DeallocationRoot.State,
				Item:  &nextSnapshotInfo.DeallocationRoot.Node,
			},
			PointerNodeAllocator: db.pointerNodeAllocator,
			DataNodeAllocator:    db.snapshotToNodeNodeAllocator,
			Allocator:            db.nextSnapshot.Allocator,
		})

		var startSnapshotID SnapshotID
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

			list := NewList(ListConfig{
				Item:          listNodeAddress,
				NodeAllocator: db.listNodeAllocator,
			})
			list.Deallocate(db.config.Allocator)
			nextDeallocationLists.Delete(sID)
		}

		// FIXME (wojciech): Iterate over space instead
		for sID := db.nextSnapshot.SingularityNode.FirstSnapshotID; sID < snapshotID; sID++ {
			listNodeAddress, exists := deallocationLists.Get(sID)
			if !exists {
				continue
			}

			nextListNodeAddress, _ := nextDeallocationLists.Get(sID)
			nextList := NewList(ListConfig{
				Item: nextListNodeAddress,
			})
			nextList.Attach(listNodeAddress)
			if nextList.config.Item != nextListNodeAddress {
				nextDeallocationLists.Set(sID, nextList.config.Item)
			}
		}

		nextSnapshotInfo.PreviousSnapshotID = snapshotInfo.PreviousSnapshotID
		db.nextSnapshot.Snapshots.Set(snapshotInfo.NextSnapshotID, nextSnapshotInfo)
	}

	if snapshotInfo.PreviousSnapshotID > db.nextSnapshot.SingularityNode.FirstSnapshotID {
		previousSnapshotInfo, exists := db.nextSnapshot.Snapshots.Get(snapshotInfo.PreviousSnapshotID)
		if !exists {
			return errors.Errorf("snapshot %d does not exist", snapshotID)
		}
		previousSnapshotInfo.NextSnapshotID = snapshotInfo.NextSnapshotID
		db.nextSnapshot.Snapshots.Set(snapshotInfo.PreviousSnapshotID, previousSnapshotInfo)
	}

	if snapshotID == db.nextSnapshot.SingularityNode.FirstSnapshotID {
		db.nextSnapshot.SingularityNode.FirstSnapshotID = snapshotInfo.NextSnapshotID
	}

	// FIXME (wojciech): Deallocate nodes used by deleted snapshot (DeallocationRoot).

	return nil
}

// Commit commits current snapshot and returns next one.
func (db *DB) Commit() error {
	spaces := make([]SpaceID, 0, len(db.nextSnapshot.SpacesToCommit))
	for spaceID := range db.nextSnapshot.SpacesToCommit {
		spaces = append(spaces, spaceID)
	}
	sort.Slice(spaces, func(i, j int) bool { return spaces[i] < spaces[j] })

	for _, spaceID := range spaces {
		spaceToCommit := db.nextSnapshot.SpacesToCommit[spaceID]
		if *spaceToCommit.PInfo.State == stateFree || *spaceToCommit.PInfo.Item == spaceToCommit.OriginalItem {
			continue
		}
		db.nextSnapshot.Spaces.Set(spaceID, SpaceInfo{
			HashMod: *spaceToCommit.HashMod,
			State:   *spaceToCommit.PInfo.State,
			Node:    *spaceToCommit.PInfo.Item,
		})
	}

	db.nextSnapshot.Snapshots.Set(db.nextSnapshot.SnapshotID, *db.nextSnapshot.SnapshotInfo)

	*photon.FromBytes[SingularityNode](db.config.Allocator.Node(0)) = *db.nextSnapshot.SingularityNode

	return db.prepareNextSnapshot(*db.nextSnapshot.SingularityNode)
}

func (db *DB) prepareNextSnapshot(singularityNode SingularityNode) error {
	var snapshotID SnapshotID
	if singularityNode.SnapshotRoot.State != stateFree {
		snapshotID = singularityNode.LastSnapshotID + 1
	}

	snapshotInfo := SnapshotInfo{
		PreviousSnapshotID: singularityNode.LastSnapshotID,
		NextSnapshotID:     snapshotID + 1,
	}

	allocator := NewSnapshotAllocator(
		snapshotID,
		db.config.Allocator,
		NewSpace[SnapshotID, NodeAddress](SpaceConfig[SnapshotID, NodeAddress]{
			SnapshotID: snapshotID,
			HashMod:    &snapshotInfo.DeallocationRoot.HashMod,
			SpaceRoot: ParentInfo{
				State: &snapshotInfo.DeallocationRoot.State,
				Item:  &snapshotInfo.DeallocationRoot.Node,
			},
			PointerNodeAllocator: db.pointerNodeAllocator,
			DataNodeAllocator:    db.snapshotToNodeNodeAllocator,
		}),
		db.listNodeAllocator,
	)
	allocator.deallocationLists.config.Allocator = allocator

	snapshots := NewSpace[SnapshotID, SnapshotInfo](SpaceConfig[SnapshotID, SnapshotInfo]{
		SnapshotID: snapshotID,
		HashMod:    &singularityNode.SnapshotRoot.HashMod,
		SpaceRoot: ParentInfo{
			State: &singularityNode.SnapshotRoot.State,
			Item:  &singularityNode.SnapshotRoot.Node,
		},
		PointerNodeAllocator: db.pointerNodeAllocator,
		DataNodeAllocator:    db.snapshotInfoNodeAllocator,
		Allocator:            allocator,
	})

	if singularityNode.SnapshotRoot.State != stateFree {
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
		Spaces: NewSpace[SpaceID, SpaceInfo](SpaceConfig[SpaceID, SpaceInfo]{
			SnapshotID: snapshotID,
			HashMod:    &snapshotInfo.SpaceRoot.HashMod,
			SpaceRoot: ParentInfo{
				State: &snapshotInfo.SpaceRoot.State,
				Item:  &snapshotInfo.SpaceRoot.Node,
			},
			PointerNodeAllocator: db.pointerNodeAllocator,
			DataNodeAllocator:    db.spaceInfoNodeAllocator,
			Allocator:            allocator,
		}),
		SpacesToCommit: map[SpaceID]SpaceToCommit{},
		Allocator:      allocator,
	}

	return nil
}

// GetSpace retrieves space from snapshot.
func GetSpace[K, V comparable](spaceID SpaceID, db *DB) (*Space[K, V], error) {
	space, exists := db.nextSnapshot.SpacesToCommit[spaceID]
	if !exists {
		spaceInfo, _ := db.nextSnapshot.Spaces.Get(spaceID)
		space = SpaceToCommit{
			HashMod: &spaceInfo.HashMod,
			PInfo: ParentInfo{
				State: &spaceInfo.State,
				Item:  &spaceInfo.Node,
			},
			OriginalItem: spaceInfo.Node,
		}
		db.nextSnapshot.SpacesToCommit[spaceID] = space
	}

	dataNodeAllocator, err := NewSpaceNodeAllocator[DataItem[K, V]](db.config.Allocator)
	if err != nil {
		return nil, err
	}

	return NewSpace[K, V](SpaceConfig[K, V]{
		SnapshotID:           db.nextSnapshot.SnapshotID,
		HashMod:              space.HashMod,
		SpaceRoot:            space.PInfo,
		PointerNodeAllocator: db.pointerNodeAllocator,
		DataNodeAllocator:    dataNodeAllocator,
		Allocator:            db.nextSnapshot.Allocator,
	}), nil
}
