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
	SpaceInfoValue  space.Entry[types.SpaceID, types.SpaceInfo]
	OriginalPointer types.Pointer
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

	db := &DB{
		config:                      config,
		singularityNode:             *photon.FromPointer[types.SingularityNode](config.Allocator.Node(0)),
		pointerNodeAllocator:        pointerNodeAllocator,
		snapshotInfoNodeAllocator:   snapshotInfoNodeAllocator,
		spaceInfoNodeAllocator:      spaceInfoNodeAllocator,
		snapshotToNodeNodeAllocator: snapshotToNodeNodeAllocator,
		listNodeAllocator:           listNodeAllocator,
		spacesToCommit:              map[types.SpaceID]SpaceToCommit{},
		deallocationListsToCommit:   map[types.SnapshotID]alloc.ListToCommit{},
		availableSnapshots:          map[types.SnapshotID]struct{}{},
	}

	db.snapshotAllocator = alloc.NewSnapshotAllocator(
		config.Allocator,
		db.deallocationListsToCommit,
		db.availableSnapshots,
		db.listNodeAllocator,
	)
	db.immediateSnapshotAllocator = alloc.NewImmediateSnapshotAllocator(db.snapshotAllocator)

	db.snapshots = space.New[types.SnapshotID, types.SnapshotInfo](space.Config[types.SnapshotID, types.SnapshotInfo]{
		HashMod: &db.singularityNode.SnapshotRoot.HashMod,
		SpaceRoot: types.ParentInfo{
			State:   &db.singularityNode.SnapshotRoot.State,
			Pointer: &db.singularityNode.SnapshotRoot.Pointer,
		},
		PointerNodeAllocator: db.pointerNodeAllocator,
		DataNodeAllocator:    db.snapshotInfoNodeAllocator,
		Allocator:            db.immediateSnapshotAllocator,
	})

	db.spaces = space.New[types.SpaceID, types.SpaceInfo](space.Config[types.SpaceID, types.SpaceInfo]{
		HashMod: &db.snapshotInfo.SpaceRoot.HashMod,
		SpaceRoot: types.ParentInfo{
			State:   &db.snapshotInfo.SpaceRoot.State,
			Pointer: &db.snapshotInfo.SpaceRoot.Pointer,
		},
		PointerNodeAllocator: db.pointerNodeAllocator,
		DataNodeAllocator:    db.spaceInfoNodeAllocator,
		Allocator:            db.snapshotAllocator,
	})

	db.deallocationLists = space.New[types.SnapshotID, types.NodeAddress](
		space.Config[types.SnapshotID, types.NodeAddress]{
			HashMod: &db.snapshotInfo.DeallocationRoot.HashMod,
			SpaceRoot: types.ParentInfo{
				State:   &db.snapshotInfo.DeallocationRoot.State,
				Pointer: &db.snapshotInfo.DeallocationRoot.Pointer,
			},
			PointerNodeAllocator: db.pointerNodeAllocator,
			DataNodeAllocator:    db.snapshotToNodeNodeAllocator,
			Allocator:            db.immediateSnapshotAllocator,
		},
	)

	if err := db.prepareNextSnapshot(); err != nil {
		return nil, err
	}
	return db, nil
}

// DB represents the database.
type DB struct {
	config                     Config
	snapshotAllocator          types.SnapshotAllocator
	immediateSnapshotAllocator types.SnapshotAllocator
	singularityNode            types.SingularityNode
	snapshots                  *space.Space[types.SnapshotID, types.SnapshotInfo]
	snapshotInfo               types.SnapshotInfo
	spaces                     *space.Space[types.SpaceID, types.SpaceInfo]
	deallocationLists          *space.Space[types.SnapshotID, types.NodeAddress]

	pointerNodeAllocator        space.NodeAllocator[types.Pointer]
	snapshotInfoNodeAllocator   space.NodeAllocator[types.DataItem[types.SnapshotID, types.SnapshotInfo]]
	spaceInfoNodeAllocator      space.NodeAllocator[types.DataItem[types.SpaceID, types.SpaceInfo]]
	snapshotToNodeNodeAllocator space.NodeAllocator[types.DataItem[types.SnapshotID, types.NodeAddress]]
	listNodeAllocator           list.NodeAllocator

	spacesToCommit            map[types.SpaceID]SpaceToCommit
	deallocationListsToCommit map[types.SnapshotID]alloc.ListToCommit
	availableSnapshots        map[types.SnapshotID]struct{}
}

// DeleteSnapshot deletes snapshot.
func (db *DB) DeleteSnapshot(snapshotID types.SnapshotID) error {
	snapshotInfoValue := db.snapshots.Get(snapshotID)
	if !snapshotInfoValue.Exists() {
		return errors.Errorf("snapshot %d does not exist", snapshotID)
	}
	snapshotInfo := snapshotInfoValue.Value()

	var nextSnapshotInfo *types.SnapshotInfo
	var nextDeallocationLists *space.Space[types.SnapshotID, types.NodeAddress]
	if snapshotInfo.NextSnapshotID < db.snapshotAllocator.SnapshotID() {
		nextSnapshotInfoValue := db.snapshots.Get(snapshotInfo.NextSnapshotID)
		if !nextSnapshotInfoValue.Exists() {
			return errors.Errorf("snapshot %d does not exist", snapshotID)
		}
		tmpNextSnapshotInfo := nextSnapshotInfoValue.Value()
		nextSnapshotInfo = &tmpNextSnapshotInfo
		nextDeallocationLists = space.New[types.SnapshotID, types.NodeAddress](
			space.Config[types.SnapshotID, types.NodeAddress]{
				HashMod: &nextSnapshotInfo.DeallocationRoot.HashMod,
				SpaceRoot: types.ParentInfo{
					State:   &nextSnapshotInfo.DeallocationRoot.State,
					Pointer: &nextSnapshotInfo.DeallocationRoot.Pointer,
				},
				PointerNodeAllocator: db.pointerNodeAllocator,
				DataNodeAllocator:    db.snapshotToNodeNodeAllocator,
				Allocator:            db.immediateSnapshotAllocator,
			},
		)
	} else {
		if err := db.commitDeallocationLists(); err != nil {
			return err
		}

		nextSnapshotInfo = &db.snapshotInfo
		nextDeallocationLists = db.deallocationLists
	}

	deallocationLists := space.New[types.SnapshotID, types.NodeAddress](space.Config[types.SnapshotID, types.NodeAddress]{
		HashMod: lo.ToPtr(snapshotInfo.DeallocationRoot.HashMod),
		SpaceRoot: types.ParentInfo{
			State:   lo.ToPtr(snapshotInfo.DeallocationRoot.State),
			Pointer: lo.ToPtr(snapshotInfo.DeallocationRoot.Pointer),
		},
		PointerNodeAllocator: db.pointerNodeAllocator,
		DataNodeAllocator:    db.snapshotToNodeNodeAllocator,
		Allocator:            db.immediateSnapshotAllocator,
	})

	var startSnapshotID types.SnapshotID
	if snapshotID != db.singularityNode.FirstSnapshotID {
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
		nextDeallocationListValue := nextDeallocationLists.Get(snapshotItem.Key)
		if err := nextDeallocationListValue.Delete(); err != nil {
			return err
		}
	}

	for snapshotItem := range deallocationLists.Iterator() {
		nextDeallocationListValue := nextDeallocationLists.Get(snapshotItem.Key)
		nextListNodeAddress := nextDeallocationListValue.Value()
		newNextListNodeAddress := nextListNodeAddress
		nextList := list.New(list.Config{
			Item:          &newNextListNodeAddress,
			NodeAllocator: db.listNodeAllocator,
			Allocator:     db.immediateSnapshotAllocator,
		})
		if err := nextList.Attach(snapshotItem.Value); err != nil {
			return err
		}
		if newNextListNodeAddress != nextListNodeAddress {
			if _, err := nextDeallocationListValue.Set(newNextListNodeAddress); err != nil {
				return err
			}
		}
	}

	if err := deallocationLists.DeallocateAll(); err != nil {
		return err
	}

	if snapshotID == db.singularityNode.FirstSnapshotID {
		nextSnapshotInfo.PreviousSnapshotID = snapshotInfo.NextSnapshotID
	} else {
		nextSnapshotInfo.PreviousSnapshotID = snapshotInfo.PreviousSnapshotID
	}

	if snapshotInfo.NextSnapshotID < db.snapshotAllocator.SnapshotID() {
		nextSnapshotInfoValue := db.snapshots.Get(snapshotInfo.NextSnapshotID)
		if _, err := nextSnapshotInfoValue.Set(*nextSnapshotInfo); err != nil {
			return err
		}
	}

	if snapshotID > db.singularityNode.FirstSnapshotID {
		previousSnapshotInfoValue := db.snapshots.Get(snapshotInfo.PreviousSnapshotID)
		if !previousSnapshotInfoValue.Exists() {
			return errors.Errorf("snapshot %d does not exist", snapshotID)
		}

		previousSnapshotInfo := previousSnapshotInfoValue.Value()
		previousSnapshotInfo.NextSnapshotID = snapshotInfo.NextSnapshotID

		if _, err := previousSnapshotInfoValue.Set(previousSnapshotInfo); err != nil {
			return err
		}
	}

	if snapshotID == db.singularityNode.FirstSnapshotID {
		db.singularityNode.FirstSnapshotID = snapshotInfo.NextSnapshotID
	}
	if snapshotID == db.snapshotInfo.PreviousSnapshotID {
		db.snapshotInfo.PreviousSnapshotID = db.snapshotAllocator.SnapshotID()
	}

	if err := snapshotInfoValue.Delete(); err != nil {
		return err
	}
	delete(db.availableSnapshots, snapshotID)
	return nil
}

// Commit commits current snapshot and returns next one.
func (db *DB) Commit() error {
	if len(db.spacesToCommit) > 0 {
		spaces := make([]types.SpaceID, 0, len(db.spacesToCommit))
		for spaceID := range db.spacesToCommit {
			spaces = append(spaces, spaceID)
		}
		sort.Slice(spaces, func(i, j int) bool { return spaces[i] < spaces[j] })

		for _, spaceID := range spaces {
			spaceToCommit := db.spacesToCommit[spaceID]
			if *spaceToCommit.PInfo.Pointer == spaceToCommit.OriginalPointer {
				continue
			}
			spaceToCommit.OriginalPointer = *spaceToCommit.PInfo.Pointer
			if _, err := spaceToCommit.SpaceInfoValue.Set(types.SpaceInfo{
				HashMod: *spaceToCommit.HashMod,
				State:   *spaceToCommit.PInfo.State,
				Pointer: *spaceToCommit.PInfo.Pointer,
			}); err != nil {
				return err
			}
		}
	}

	if err := db.commitDeallocationLists(); err != nil {
		return err
	}

	nextSnapshotInfoValue := db.snapshots.Get(db.snapshotAllocator.SnapshotID())
	if _, err := nextSnapshotInfoValue.Set(db.snapshotInfo); err != nil {
		return err
	}

	*photon.FromPointer[types.SingularityNode](db.config.Allocator.Node(0)) = db.singularityNode

	db.availableSnapshots[db.snapshotAllocator.SnapshotID()] = struct{}{}

	return db.prepareNextSnapshot()
}

func (db *DB) prepareNextSnapshot() error {
	var snapshotID types.SnapshotID
	if db.singularityNode.SnapshotRoot.State != types.StateFree {
		snapshotID = db.singularityNode.LastSnapshotID + 1
	}

	db.snapshotInfo = types.SnapshotInfo{
		PreviousSnapshotID: db.singularityNode.LastSnapshotID,
		NextSnapshotID:     snapshotID + 1,
		SpaceRoot:          db.snapshotInfo.SpaceRoot,
	}

	db.immediateSnapshotAllocator.SetSnapshotID(snapshotID)

	db.singularityNode.LastSnapshotID = snapshotID

	return nil
}

func (db *DB) commitDeallocationLists() error {
	if len(db.deallocationListsToCommit) > 0 {
		lists := make([]types.SnapshotID, 0, len(db.deallocationListsToCommit))
		for snapshotID := range db.deallocationListsToCommit {
			lists = append(lists, snapshotID)
		}
		sort.Slice(lists, func(i, j int) bool { return lists[i] < lists[j] })

		for _, snapshotID := range lists {
			_, err := db.deallocationLists.Get(snapshotID).Set(*db.deallocationListsToCommit[snapshotID].Item)
			if err != nil {
				return err
			}
		}

		clear(db.deallocationListsToCommit)
	}

	return nil
}

// GetSpace retrieves space from snapshot.
func GetSpace[K, V comparable](spaceID types.SpaceID, db *DB) (*space.Space[K, V], error) {
	s, exists := db.spacesToCommit[spaceID]
	if !exists {
		spaceInfoValue := db.spaces.Get(spaceID)
		spaceInfo := spaceInfoValue.Value()
		s = SpaceToCommit{
			HashMod: &spaceInfo.HashMod,
			PInfo: types.ParentInfo{
				State:   &spaceInfo.State,
				Pointer: &spaceInfo.Pointer,
			},
			OriginalPointer: spaceInfo.Pointer,
			SpaceInfoValue:  spaceInfoValue,
		}
		db.spacesToCommit[spaceID] = s
	}

	dataNodeAllocator, err := space.NewNodeAllocator[types.DataItem[K, V]](db.config.Allocator)
	if err != nil {
		return nil, err
	}

	return space.New[K, V](space.Config[K, V]{
		HashMod:              s.HashMod,
		SpaceRoot:            s.PInfo,
		PointerNodeAllocator: db.pointerNodeAllocator,
		DataNodeAllocator:    dataNodeAllocator,
		Allocator:            db.snapshotAllocator,
	}), nil
}
