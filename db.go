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
	HashMod         *uint64
	PInfo           types.ParentInfo
	OriginalAddress types.NodeAddress
}

// Snapshot represents snapshot.
type Snapshot struct {
	SnapshotID        types.SnapshotID
	SingularityNode   *types.SingularityNode
	SnapshotInfo      *types.SnapshotInfo
	Snapshots         *space.Space[types.SnapshotID, types.SnapshotInfo]
	Spaces            *space.Space[types.SpaceID, types.SpaceInfo]
	DeallocationLists *space.Space[types.SnapshotID, types.NodeAddress]
	Allocator         types.SnapshotAllocator
}

// New creates new database.
func New(config Config) (*DB, error) {
	pointerNodeAllocator, err := space.NewNodeAllocator[types.Pointer](config.Allocator)
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
		spacesToCommit:              map[types.SpaceID]SpaceToCommit{},
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

	pointerNodeAllocator        space.NodeAllocator[types.Pointer]
	snapshotInfoNodeAllocator   space.NodeAllocator[types.DataItem[types.SnapshotID, types.SnapshotInfo]]
	spaceInfoNodeAllocator      space.NodeAllocator[types.DataItem[types.SpaceID, types.SpaceInfo]]
	snapshotToNodeNodeAllocator space.NodeAllocator[types.DataItem[types.SnapshotID, types.NodeAddress]]
	listNodeAllocator           list.NodeAllocator

	spacesToCommit map[types.SpaceID]SpaceToCommit
}

// DeleteSnapshot deletes snapshot.
func (db *DB) DeleteSnapshot(snapshotID types.SnapshotID) error {
	snapshotInfo, exists := db.nextSnapshot.Snapshots.Get(snapshotID)
	if !exists {
		return errors.Errorf("snapshot %d does not exist", snapshotID)
	}

	allocator := alloc.NewImmediateSnapshotAllocator(db.nextSnapshot.SnapshotID,
		db.nextSnapshot.Allocator)

	var nextSnapshotInfo *types.SnapshotInfo
	var nextDeallocationLists *space.Space[types.SnapshotID, types.NodeAddress]
	if snapshotInfo.NextSnapshotID < db.nextSnapshot.SnapshotID {
		tmpNextSnapshotInfo, exists := db.nextSnapshot.Snapshots.Get(snapshotInfo.NextSnapshotID)
		if !exists {
			return errors.Errorf("snapshot %d does not exist", snapshotID)
		}
		nextSnapshotInfo = &tmpNextSnapshotInfo
		nextDeallocationLists = space.New[types.SnapshotID, types.NodeAddress](
			space.Config[types.SnapshotID, types.NodeAddress]{
				SnapshotID: db.nextSnapshot.SnapshotID,
				HashMod:    &nextSnapshotInfo.DeallocationRoot.HashMod,
				SpaceRoot: types.ParentInfo{
					State:   &nextSnapshotInfo.DeallocationRoot.State,
					Pointer: &nextSnapshotInfo.DeallocationRoot.Pointer,
				},
				PointerNodeAllocator: db.pointerNodeAllocator,
				DataNodeAllocator:    db.snapshotToNodeNodeAllocator,
				Allocator:            allocator,
			},
		)
	} else {
		nextSnapshotInfo = db.nextSnapshot.SnapshotInfo
		nextDeallocationLists = db.nextSnapshot.DeallocationLists
	}

	deallocationLists := space.New[types.SnapshotID, types.NodeAddress](space.Config[types.SnapshotID, types.NodeAddress]{
		SnapshotID: db.nextSnapshot.SnapshotID,
		HashMod:    lo.ToPtr(snapshotInfo.DeallocationRoot.HashMod),
		SpaceRoot: types.ParentInfo{
			State:   lo.ToPtr(snapshotInfo.DeallocationRoot.State),
			Pointer: lo.ToPtr(snapshotInfo.DeallocationRoot.Pointer),
		},
		PointerNodeAllocator: db.pointerNodeAllocator,
		DataNodeAllocator:    db.snapshotToNodeNodeAllocator,
		Allocator:            allocator,
	})

	var startSnapshotID types.SnapshotID
	if snapshotID != db.nextSnapshot.SingularityNode.FirstSnapshotID {
		startSnapshotID = snapshotInfo.PreviousSnapshotID + 1
	}
	for snapshotItem := range nextDeallocationLists.Iterator() {
		if snapshotItem.Key < startSnapshotID || snapshotItem.Key > snapshotID {
			continue
		}

		l := list.New(list.Config{
			Item:          &snapshotItem.Value,
			NodeAllocator: db.listNodeAllocator,
		})
		l.Deallocate(db.config.Allocator)
		if err := nextDeallocationLists.Delete(snapshotItem.Key); err != nil {
			return err
		}
	}

	for snapshotItem := range deallocationLists.Iterator() {
		nextListNodeAddress, _ := nextDeallocationLists.Get(snapshotItem.Key)
		newNextListNodeAddress := nextListNodeAddress
		nextList := list.New(list.Config{
			SnapshotID:    db.nextSnapshot.SnapshotID,
			Item:          &newNextListNodeAddress,
			NodeAllocator: db.listNodeAllocator,
			Allocator:     allocator,
		})
		if err := nextList.Attach(snapshotItem.Value); err != nil {
			return err
		}
		if newNextListNodeAddress != nextListNodeAddress {
			if err := nextDeallocationLists.Set(snapshotItem.Key, newNextListNodeAddress); err != nil {
				return err
			}
		}
	}

	if err := deallocationLists.DeallocateAll(); err != nil {
		return err
	}

	if snapshotID == db.nextSnapshot.SingularityNode.FirstSnapshotID {
		nextSnapshotInfo.PreviousSnapshotID = snapshotInfo.NextSnapshotID
	} else {
		nextSnapshotInfo.PreviousSnapshotID = snapshotInfo.PreviousSnapshotID
	}

	if snapshotInfo.NextSnapshotID < db.nextSnapshot.SnapshotID {
		if err := db.nextSnapshot.Snapshots.Set(snapshotInfo.NextSnapshotID, *nextSnapshotInfo); err != nil {
			return err
		}
	}

	if snapshotID > db.nextSnapshot.SingularityNode.FirstSnapshotID {
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
	if snapshotID == db.nextSnapshot.SnapshotInfo.PreviousSnapshotID {
		db.nextSnapshot.SnapshotInfo.PreviousSnapshotID = db.nextSnapshot.SnapshotID
	}

	return db.nextSnapshot.Snapshots.Delete(snapshotID)
}

// Commit commits current snapshot and returns next one.
func (db *DB) Commit() error {
	spaces := make([]types.SpaceID, 0, len(db.spacesToCommit))
	for spaceID := range db.spacesToCommit {
		spaces = append(spaces, spaceID)
	}
	sort.Slice(spaces, func(i, j int) bool { return spaces[i] < spaces[j] })

	for _, spaceID := range spaces {
		spaceToCommit := db.spacesToCommit[spaceID]
		if *spaceToCommit.PInfo.State == types.StateFree ||
			spaceToCommit.PInfo.Pointer.Address == spaceToCommit.OriginalAddress {
			continue
		}
		if err := db.nextSnapshot.Spaces.Set(spaceID, types.SpaceInfo{
			HashMod: *spaceToCommit.HashMod,
			State:   *spaceToCommit.PInfo.State,
			Pointer: *spaceToCommit.PInfo.Pointer,
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
	snapshots := &space.Space[types.SnapshotID, types.SnapshotInfo]{}
	allocator := alloc.NewSnapshotAllocator(
		snapshotID,
		db.config.Allocator,
		snapshots,
		deallocationLists,
		db.listNodeAllocator,
	)
	immediateAllocator := alloc.NewImmediateSnapshotAllocator(snapshotID, allocator)
	*deallocationLists = *space.New[types.SnapshotID, types.NodeAddress](space.Config[types.SnapshotID, types.NodeAddress]{
		SnapshotID: snapshotID,
		HashMod:    &snapshotInfo.DeallocationRoot.HashMod,
		SpaceRoot: types.ParentInfo{
			State:   &snapshotInfo.DeallocationRoot.State,
			Pointer: &snapshotInfo.DeallocationRoot.Pointer,
		},
		PointerNodeAllocator: db.pointerNodeAllocator,
		DataNodeAllocator:    db.snapshotToNodeNodeAllocator,
		Allocator:            immediateAllocator,
	})

	*snapshots = *space.New[types.SnapshotID, types.SnapshotInfo](space.Config[types.SnapshotID, types.SnapshotInfo]{
		SnapshotID: snapshotID,
		HashMod:    &singularityNode.SnapshotRoot.HashMod,
		SpaceRoot: types.ParentInfo{
			State:   &singularityNode.SnapshotRoot.State,
			Pointer: &singularityNode.SnapshotRoot.Pointer,
		},
		PointerNodeAllocator: db.pointerNodeAllocator,
		DataNodeAllocator:    db.snapshotInfoNodeAllocator,
		Allocator:            immediateAllocator,
	})

	if singularityNode.SnapshotRoot.State != types.StateFree {
		lastSnapshotInfo, exists := snapshots.Get(singularityNode.LastSnapshotID)
		if !exists {
			return errors.Errorf("snapshot %d does not exist", singularityNode.LastSnapshotID)
		}
		snapshotInfo.SpaceRoot = lastSnapshotInfo.SpaceRoot
	}

	singularityNode.LastSnapshotID = snapshotID

	clear(db.spacesToCommit)
	db.nextSnapshot = Snapshot{
		SnapshotID:      snapshotID,
		SingularityNode: &singularityNode,
		SnapshotInfo:    &snapshotInfo,
		Snapshots:       snapshots,
		Spaces: space.New[types.SpaceID, types.SpaceInfo](space.Config[types.SpaceID, types.SpaceInfo]{
			SnapshotID: snapshotID,
			HashMod:    &snapshotInfo.SpaceRoot.HashMod,
			SpaceRoot: types.ParentInfo{
				State:   &snapshotInfo.SpaceRoot.State,
				Pointer: &snapshotInfo.SpaceRoot.Pointer,
			},
			PointerNodeAllocator: db.pointerNodeAllocator,
			DataNodeAllocator:    db.spaceInfoNodeAllocator,
			Allocator:            allocator,
		}),
		DeallocationLists: deallocationLists,
		Allocator:         allocator,
	}

	return nil
}

// GetSpace retrieves space from snapshot.
func GetSpace[K, V comparable](spaceID types.SpaceID, db *DB) (*space.Space[K, V], error) {
	s, exists := db.spacesToCommit[spaceID]
	if !exists {
		spaceInfo, _ := db.nextSnapshot.Spaces.Get(spaceID)
		s = SpaceToCommit{
			HashMod: &spaceInfo.HashMod,
			PInfo: types.ParentInfo{
				State:   &spaceInfo.State,
				Pointer: &spaceInfo.Pointer,
			},
			OriginalAddress: spaceInfo.Pointer.Address,
		}
		db.spacesToCommit[spaceID] = s
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
