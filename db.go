package quantum

import (
	"context"
	"fmt"
	"sort"

	"github.com/pkg/errors"
	"github.com/samber/lo"

	"github.com/outofforest/parallel"
	"github.com/outofforest/photon"
	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/list"
	"github.com/outofforest/quantum/space"
	"github.com/outofforest/quantum/types"
)

// Config stores snapshot configuration.
type Config struct {
	Allocator            types.Allocator
	DirtyDataNodeWorkers uint
	DirtyListNodeWorkers uint
}

// SpaceToCommit represents requested space which might require to be committed.
type SpaceToCommit struct {
	HashMod         *uint64
	PInfo           types.ParentEntry
	SpaceInfoValue  space.Entry[types.SpaceID, types.SpaceInfo]
	OriginalPointer types.SpacePointer
}

// New creates new database.
func New(config Config) (*DB, error) {
	db := &DB{
		config:                    config,
		singularityNode:           *photon.FromPointer[types.SingularityNode](config.Allocator.Node(0)),
		spacesToCommit:            map[types.SpaceID]SpaceToCommit{},
		deallocationListsToCommit: map[types.SnapshotID]alloc.ListToCommit{},
		availableSnapshots:        map[types.SnapshotID]struct{}{},
		dirtyDataNodesCh:          make(chan types.DirtyDataNode, 100),
		dirtyListNodesCh:          make(chan types.DirtyListNode, 100),
		doneCh:                    make(chan struct{}),
	}

	db.snapshotAllocator = alloc.NewSnapshotAllocator(
		config.Allocator,
		db.deallocationListsToCommit,
		db.availableSnapshots,
		db.dirtyListNodesCh,
	)

	db.immediateSnapshotAllocator = alloc.NewImmediateSnapshotAllocator(db.snapshotAllocator)

	var err error
	db.snapshots, err = space.New[types.SnapshotID, types.SnapshotInfo](space.Config[types.SnapshotID, types.SnapshotInfo]{
		HashMod: &db.singularityNode.SnapshotRoot.HashMod,
		SpaceRoot: types.ParentEntry{
			State:        &db.singularityNode.SnapshotRoot.State,
			SpacePointer: &db.singularityNode.SnapshotRoot.Pointer,
		},
		Allocator:         config.Allocator,
		SnapshotAllocator: db.immediateSnapshotAllocator,
		DirtyDataNodesCh:  db.dirtyDataNodesCh,
	})
	if err != nil {
		return nil, err
	}

	db.spaces, err = space.New[types.SpaceID, types.SpaceInfo](space.Config[types.SpaceID, types.SpaceInfo]{
		HashMod: &db.snapshotInfo.SpaceRoot.HashMod,
		SpaceRoot: types.ParentEntry{
			State:        &db.snapshotInfo.SpaceRoot.State,
			SpacePointer: &db.snapshotInfo.SpaceRoot.Pointer,
		},
		Allocator:         config.Allocator,
		SnapshotAllocator: db.snapshotAllocator,
		DirtyDataNodesCh:  db.dirtyDataNodesCh,
	})
	if err != nil {
		return nil, err
	}

	db.deallocationLists, err = space.New[types.SnapshotID, types.Pointer](
		space.Config[types.SnapshotID, types.Pointer]{
			HashMod: &db.snapshotInfo.DeallocationRoot.HashMod,
			SpaceRoot: types.ParentEntry{
				State:        &db.snapshotInfo.DeallocationRoot.State,
				SpacePointer: &db.snapshotInfo.DeallocationRoot.Pointer,
			},
			Allocator:         config.Allocator,
			SnapshotAllocator: db.immediateSnapshotAllocator,
			DirtyDataNodesCh:  db.dirtyDataNodesCh,
		},
	)

	if err != nil {
		return nil, err
	}

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
	deallocationLists          *space.Space[types.SnapshotID, types.Pointer]

	spacesToCommit            map[types.SpaceID]SpaceToCommit
	deallocationListsToCommit map[types.SnapshotID]alloc.ListToCommit
	availableSnapshots        map[types.SnapshotID]struct{}

	dirtyDataNodesCh chan types.DirtyDataNode
	dirtyListNodesCh chan types.DirtyListNode
	doneCh           chan struct{}
}

// DeleteSnapshot deletes snapshot.
func (db *DB) DeleteSnapshot(snapshotID types.SnapshotID) error {
	snapshotInfoValue := db.snapshots.Get(snapshotID)
	if !snapshotInfoValue.Exists() {
		return errors.Errorf("snapshot %d does not exist", snapshotID)
	}
	snapshotInfo := snapshotInfoValue.Value()

	var nextSnapshotInfo *types.SnapshotInfo
	var nextDeallocationLists *space.Space[types.SnapshotID, types.Pointer]
	if snapshotInfo.NextSnapshotID < db.snapshotAllocator.SnapshotID() {
		nextSnapshotInfoValue := db.snapshots.Get(snapshotInfo.NextSnapshotID)
		if !nextSnapshotInfoValue.Exists() {
			return errors.Errorf("snapshot %d does not exist", snapshotID)
		}
		tmpNextSnapshotInfo := nextSnapshotInfoValue.Value()
		nextSnapshotInfo = &tmpNextSnapshotInfo
		var err error
		nextDeallocationLists, err = space.New[types.SnapshotID, types.Pointer](
			space.Config[types.SnapshotID, types.Pointer]{
				HashMod: &nextSnapshotInfo.DeallocationRoot.HashMod,
				SpaceRoot: types.ParentEntry{
					State:        &nextSnapshotInfo.DeallocationRoot.State,
					SpacePointer: &nextSnapshotInfo.DeallocationRoot.Pointer,
				},
				Allocator:         db.config.Allocator,
				SnapshotAllocator: db.immediateSnapshotAllocator,
				DirtyDataNodesCh:  db.dirtyDataNodesCh,
			},
		)
		if err != nil {
			return err
		}
	} else {
		if err := db.commitDeallocationLists(); err != nil {
			return err
		}

		nextSnapshotInfo = &db.snapshotInfo
		nextDeallocationLists = db.deallocationLists
	}

	deallocationLists, err := space.New[types.SnapshotID, types.Pointer](
		space.Config[types.SnapshotID, types.Pointer]{
			HashMod: lo.ToPtr(snapshotInfo.DeallocationRoot.HashMod),
			SpaceRoot: types.ParentEntry{
				State:        lo.ToPtr(snapshotInfo.DeallocationRoot.State),
				SpacePointer: lo.ToPtr(snapshotInfo.DeallocationRoot.Pointer),
			},
			Allocator:         db.config.Allocator,
			SnapshotAllocator: db.immediateSnapshotAllocator,
			DirtyDataNodesCh:  db.dirtyDataNodesCh,
		},
	)
	if err != nil {
		return err
	}

	var startSnapshotID types.SnapshotID
	if snapshotID != db.singularityNode.FirstSnapshotID {
		startSnapshotID = snapshotInfo.PreviousSnapshotID + 1
	}
	for snapshotItem := range nextDeallocationLists.Iterator() {
		if snapshotItem.Key < startSnapshotID || snapshotItem.Key > snapshotID {
			continue
		}

		l, err := list.New(list.Config{
			ListRoot:  &snapshotItem.Value,
			Allocator: db.config.Allocator,
		})
		if err != nil {
			return err
		}

		l.Deallocate()
		nextDeallocationListValue := nextDeallocationLists.Get(snapshotItem.Key)
		if err := nextDeallocationListValue.Delete(); err != nil {
			return err
		}
	}

	for snapshotItem := range deallocationLists.Iterator() {
		nextDeallocationListValue := nextDeallocationLists.Get(snapshotItem.Key)
		nextListNodeAddress := nextDeallocationListValue.Value()
		newNextListNodeAddress := nextListNodeAddress
		nextList, err := list.New(list.Config{
			ListRoot:          &newNextListNodeAddress,
			Allocator:         db.config.Allocator,
			SnapshotAllocator: db.immediateSnapshotAllocator,
			DirtyListNodesCh:  db.dirtyListNodesCh,
		})
		if err != nil {
			return err
		}
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
			if *spaceToCommit.PInfo.SpacePointer == spaceToCommit.OriginalPointer {
				continue
			}
			spaceToCommit.OriginalPointer = *spaceToCommit.PInfo.SpacePointer
			if _, err := spaceToCommit.SpaceInfoValue.Set(types.SpaceInfo{
				HashMod: *spaceToCommit.HashMod,
				State:   *spaceToCommit.PInfo.State,
				Pointer: *spaceToCommit.PInfo.SpacePointer,
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

// Close closed DB.
func (db *DB) Close() {
	close(db.dirtyDataNodesCh)
	close(db.dirtyListNodesCh)

	<-db.doneCh
}

// Run runs db goroutines.
func (db *DB) Run(ctx context.Context) error {
	defer close(db.doneCh)

	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		for i := range db.config.DirtyDataNodeWorkers {
			spawn(fmt.Sprintf("dirtyDataNodeWorker-%02d", i), parallel.Continue, func(ctx context.Context) error {
				return db.processDirtyDataNodes(ctx, db.dirtyDataNodesCh)
			})
		}
		for i := range db.config.DirtyListNodeWorkers {
			spawn(fmt.Sprintf("dirtyListNodeWorker-%02d", i), parallel.Continue, func(ctx context.Context) error {
				return db.processDirtyListNodes(ctx, db.dirtyListNodesCh)
			})
		}
		return nil
	})
}

func (db *DB) processDirtyDataNodes(ctx context.Context, dirtyDataNodesCh <-chan types.DirtyDataNode) error {
	for range dirtyDataNodesCh {

	}
	return errors.WithStack(ctx.Err())
}

func (db *DB) processDirtyListNodes(ctx context.Context, dirtyListNodesCh <-chan types.DirtyListNode) error {
	for range dirtyListNodesCh {
	}
	return errors.WithStack(ctx.Err())
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
			_, err := db.deallocationLists.Get(snapshotID).Set(*db.deallocationListsToCommit[snapshotID].ListRoot)
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
			PInfo: types.ParentEntry{
				State:        &spaceInfo.State,
				SpacePointer: &spaceInfo.Pointer,
			},
			OriginalPointer: spaceInfo.Pointer,
			SpaceInfoValue:  spaceInfoValue,
		}
		db.spacesToCommit[spaceID] = s
	}

	return space.New[K, V](space.Config[K, V]{
		HashMod:           s.HashMod,
		SpaceRoot:         s.PInfo,
		Allocator:         db.config.Allocator,
		SnapshotAllocator: db.snapshotAllocator,
		DirtyDataNodesCh:  db.dirtyDataNodesCh,
	})
}
