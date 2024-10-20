package quantum

import (
	"context"
	"sort"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/samber/lo"

	"github.com/outofforest/mass"
	"github.com/outofforest/parallel"
	"github.com/outofforest/photon"
	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/list"
	"github.com/outofforest/quantum/persistent"
	"github.com/outofforest/quantum/space"
	"github.com/outofforest/quantum/types"
)

// Config stores snapshot configuration.
type Config struct {
	State *alloc.State
	Store persistent.Store
}

// SpaceToCommit represents requested space which might require to be committed.
type SpaceToCommit struct {
	HashMod         *uint64
	PInfo           types.ParentEntry
	SpaceInfoValue  *space.Entry[types.SpaceID, types.SpaceInfo]
	OriginalPointer types.SpacePointer
}

// ListToCommit contains cached deallocation list.
type ListToCommit struct {
	List     *list.List
	ListRoot *types.Pointer
}

// New creates new database.
func New(config Config) (*DB, error) {
	pointerNodeAllocator, err := space.NewNodeAllocator[space.PointerNodeHeader, types.SpacePointer](config.State)
	if err != nil {
		return nil, err
	}

	snapshotInfoNodeAllocator, err := space.NewNodeAllocator[
		space.DataNodeHeader,
		types.DataItem[types.SnapshotID, types.SnapshotInfo],
	](
		config.State,
	)
	if err != nil {
		return nil, err
	}

	snapshotToNodeNodeAllocator, err := space.NewNodeAllocator[
		space.DataNodeHeader,
		types.DataItem[types.SnapshotID, types.Pointer],
	](
		config.State,
	)
	if err != nil {
		return nil, err
	}

	spaceInfoNodeAllocator, err := space.NewNodeAllocator[
		space.DataNodeHeader,
		types.DataItem[types.SpaceID, types.SpaceInfo],
	](
		config.State,
	)
	if err != nil {
		return nil, err
	}

	listNodeAllocator, err := list.NewNodeAllocator(config.State)
	if err != nil {
		return nil, err
	}

	db := &DB{
		config:                      config,
		statePool:                   config.State.NewPool(),
		persistentAllocationCh:      config.State.NewPhysicalAllocationCh(),
		singularityNode:             *photon.FromPointer[types.SingularityNode](config.State.Node(0)),
		pointerNodeAllocator:        pointerNodeAllocator,
		snapshotToNodeNodeAllocator: snapshotToNodeNodeAllocator,
		listNodeAllocator:           listNodeAllocator,
		pointerNode:                 pointerNodeAllocator.NewNode(),
		snapshotInfoNode:            snapshotInfoNodeAllocator.NewNode(),
		snapshotToNodeNode:          snapshotToNodeNodeAllocator.NewNode(),
		spaceInfoNode:               spaceInfoNodeAllocator.NewNode(),
		listNode:                    listNodeAllocator.NewNode(),
		massSnapshotToNodeEntry:     mass.New[space.Entry[types.SnapshotID, types.Pointer]](1000),
		spacesToCommit:              map[types.SpaceID]SpaceToCommit{},
		deallocationListsToCommit:   map[types.SnapshotID]ListToCommit{},
		availableSnapshots:          map[types.SnapshotID]struct{}{},
		eventCh:                     make(chan any, 100),
		doneCh:                      make(chan struct{}),
	}

	db.snapshots = space.New[types.SnapshotID, types.SnapshotInfo](space.Config[types.SnapshotID, types.SnapshotInfo]{
		HashMod: &db.singularityNode.SnapshotRoot.HashMod,
		SpaceRoot: types.ParentEntry{
			State:        &db.singularityNode.SnapshotRoot.State,
			SpacePointer: &db.singularityNode.SnapshotRoot.Pointer,
		},
		State:                config.State,
		PointerNodeAllocator: pointerNodeAllocator,
		DataNodeAllocator:    snapshotInfoNodeAllocator,
		MassEntry:            mass.New[space.Entry[types.SnapshotID, types.SnapshotInfo]](1000),
		StorageEventCh:       db.eventCh,
	})

	db.spaces = space.New[types.SpaceID, types.SpaceInfo](space.Config[types.SpaceID, types.SpaceInfo]{
		HashMod: &db.snapshotInfo.SpaceRoot.HashMod,
		SpaceRoot: types.ParentEntry{
			State:        &db.snapshotInfo.SpaceRoot.State,
			SpacePointer: &db.snapshotInfo.SpaceRoot.Pointer,
		},
		State:                config.State,
		PointerNodeAllocator: pointerNodeAllocator,
		DataNodeAllocator:    spaceInfoNodeAllocator,
		MassEntry:            mass.New[space.Entry[types.SpaceID, types.SpaceInfo]](1000),
		StorageEventCh:       db.eventCh,
	})

	db.deallocationLists = space.New[types.SnapshotID, types.Pointer](
		space.Config[types.SnapshotID, types.Pointer]{
			HashMod: &db.snapshotInfo.DeallocationRoot.HashMod,
			SpaceRoot: types.ParentEntry{
				State:        &db.snapshotInfo.DeallocationRoot.State,
				SpacePointer: &db.snapshotInfo.DeallocationRoot.Pointer,
			},
			State:                config.State,
			PointerNodeAllocator: pointerNodeAllocator,
			DataNodeAllocator:    snapshotToNodeNodeAllocator,
			MassEntry:            mass.New[space.Entry[types.SnapshotID, types.Pointer]](1000),
			StorageEventCh:       db.eventCh,
		},
	)

	if err := db.prepareNextSnapshot(); err != nil {
		return nil, err
	}
	return db, nil
}

// DB represents the database.
type DB struct {
	config                 Config
	statePool              *alloc.Pool[types.LogicalAddress]
	persistentAllocationCh chan []types.PhysicalAddress
	singularityNode        types.SingularityNode
	snapshotInfo           types.SnapshotInfo
	snapshots              *space.Space[types.SnapshotID, types.SnapshotInfo]
	spaces                 *space.Space[types.SpaceID, types.SpaceInfo]
	deallocationLists      *space.Space[types.SnapshotID, types.Pointer]

	pointerNodeAllocator        *space.NodeAllocator[space.PointerNodeHeader, types.SpacePointer]
	snapshotToNodeNodeAllocator *space.NodeAllocator[space.DataNodeHeader, types.DataItem[types.SnapshotID, types.Pointer]]
	listNodeAllocator           *list.NodeAllocator

	pointerNode        *space.Node[space.PointerNodeHeader, types.SpacePointer]
	snapshotInfoNode   *space.Node[space.DataNodeHeader, types.DataItem[types.SnapshotID, types.SnapshotInfo]]
	snapshotToNodeNode *space.Node[space.DataNodeHeader, types.DataItem[types.SnapshotID, types.Pointer]]
	spaceInfoNode      *space.Node[space.DataNodeHeader, types.DataItem[types.SpaceID, types.SpaceInfo]]
	listNode           *list.Node

	massSnapshotToNodeEntry *mass.Mass[space.Entry[types.SnapshotID, types.Pointer]]

	spacesToCommit            map[types.SpaceID]SpaceToCommit
	deallocationListsToCommit map[types.SnapshotID]ListToCommit
	availableSnapshots        map[types.SnapshotID]struct{}

	eventCh chan any
	doneCh  chan struct{}
}

// DeleteSnapshot deletes snapshot.
func (db *DB) DeleteSnapshot(snapshotID types.SnapshotID, pool *alloc.Pool[types.LogicalAddress]) error {
	snapshotInfoValue := db.snapshots.Get(snapshotID, db.pointerNode, db.snapshotInfoNode)
	if !snapshotInfoValue.Exists() {
		return errors.Errorf("snapshot %d does not exist", snapshotID)
	}

	if err := snapshotInfoValue.Delete(db.pointerNode, db.snapshotInfoNode); err != nil {
		return err
	}
	delete(db.availableSnapshots, snapshotID)

	snapshotInfo := snapshotInfoValue.Value()

	var nextSnapshotInfo *types.SnapshotInfo
	var nextDeallocationLists *space.Space[types.SnapshotID, types.Pointer]
	if snapshotInfo.NextSnapshotID < db.singularityNode.LastSnapshotID {
		nextSnapshotInfoValue := db.snapshots.Get(snapshotInfo.NextSnapshotID, db.pointerNode, db.snapshotInfoNode)
		if !nextSnapshotInfoValue.Exists() {
			return errors.Errorf("snapshot %d does not exist", snapshotID)
		}
		tmpNextSnapshotInfo := nextSnapshotInfoValue.Value()
		nextSnapshotInfo = &tmpNextSnapshotInfo

		nextDeallocationLists = space.New[types.SnapshotID, types.Pointer](
			space.Config[types.SnapshotID, types.Pointer]{
				HashMod: &nextSnapshotInfo.DeallocationRoot.HashMod,
				SpaceRoot: types.ParentEntry{
					State:        &nextSnapshotInfo.DeallocationRoot.State,
					SpacePointer: &nextSnapshotInfo.DeallocationRoot.Pointer,
				},
				State:                db.config.State,
				PointerNodeAllocator: db.pointerNodeAllocator,
				DataNodeAllocator:    db.snapshotToNodeNodeAllocator,
				MassEntry:            db.massSnapshotToNodeEntry,
				StorageEventCh:       db.eventCh,
			},
		)
	} else {
		if err := db.commitDeallocationLists(pool); err != nil {
			return err
		}

		nextSnapshotInfo = &db.snapshotInfo
		nextDeallocationLists = db.deallocationLists
	}

	var startSnapshotID types.SnapshotID
	if snapshotID != db.singularityNode.FirstSnapshotID {
		startSnapshotID = snapshotInfo.PreviousSnapshotID + 1
	}
	for snapshotItem := range nextDeallocationLists.Iterator(db.pointerNode, db.snapshotToNodeNode) {
		if snapshotItem.Key < startSnapshotID || snapshotItem.Key > snapshotID {
			continue
		}

		db.eventCh <- types.ListDeallocationEvent{
			ListRoot: snapshotItem.Value,
		}

		nextDeallocationListValue := nextDeallocationLists.Get(snapshotItem.Key, db.pointerNode, db.snapshotToNodeNode)
		if err := nextDeallocationListValue.Delete(db.pointerNode, db.snapshotToNodeNode); err != nil {
			return err
		}
	}

	deallocationListsRoot := types.ParentEntry{
		State:        lo.ToPtr(snapshotInfo.DeallocationRoot.State),
		SpacePointer: lo.ToPtr(snapshotInfo.DeallocationRoot.Pointer),
	}
	deallocationLists := space.New[types.SnapshotID, types.Pointer](
		space.Config[types.SnapshotID, types.Pointer]{
			HashMod:              lo.ToPtr(snapshotInfo.DeallocationRoot.HashMod),
			SpaceRoot:            deallocationListsRoot,
			State:                db.config.State,
			PointerNodeAllocator: db.pointerNodeAllocator,
			DataNodeAllocator:    db.snapshotToNodeNodeAllocator,
			MassEntry:            db.massSnapshotToNodeEntry,
		},
	)

	for snapshotItem := range deallocationLists.Iterator(db.pointerNode, db.snapshotToNodeNode) {
		nextDeallocationListValue := nextDeallocationLists.Get(snapshotItem.Key, db.pointerNode, db.snapshotToNodeNode)
		nextListNodeAddress := nextDeallocationListValue.Value()
		newNextListNodeAddress := nextListNodeAddress
		nextList, err := list.New(list.Config{
			ListRoot:       &newNextListNodeAddress,
			State:          db.config.State,
			NodeAllocator:  db.listNodeAllocator,
			StorageEventCh: db.eventCh,
		})
		if err != nil {
			return err
		}
		if err := nextList.Attach(snapshotItem.Value, pool, db.listNode); err != nil {
			return err
		}
		if newNextListNodeAddress != nextListNodeAddress {
			if err := nextDeallocationListValue.Set(
				newNextListNodeAddress,
				pool,
				db.pointerNode,
				db.snapshotToNodeNode,
			); err != nil {
				return err
			}
		}
	}

	db.eventCh <- types.SpaceDeallocationEvent{
		SpaceRoot: deallocationListsRoot,
	}

	if snapshotID == db.singularityNode.FirstSnapshotID {
		nextSnapshotInfo.PreviousSnapshotID = snapshotInfo.NextSnapshotID
	} else {
		nextSnapshotInfo.PreviousSnapshotID = snapshotInfo.PreviousSnapshotID
	}

	if snapshotInfo.NextSnapshotID < db.singularityNode.LastSnapshotID {
		nextSnapshotInfoValue := db.snapshots.Get(snapshotInfo.NextSnapshotID, db.pointerNode, db.snapshotInfoNode)
		if err := nextSnapshotInfoValue.Set(
			*nextSnapshotInfo,
			pool,
			db.pointerNode,
			db.snapshotInfoNode,
		); err != nil {
			return err
		}
	}

	if snapshotID > db.singularityNode.FirstSnapshotID {
		previousSnapshotInfoValue := db.snapshots.Get(snapshotInfo.PreviousSnapshotID, db.pointerNode,
			db.snapshotInfoNode)
		if !previousSnapshotInfoValue.Exists() {
			return errors.Errorf("snapshot %d does not exist", snapshotID)
		}

		previousSnapshotInfo := previousSnapshotInfoValue.Value()
		previousSnapshotInfo.NextSnapshotID = snapshotInfo.NextSnapshotID

		if err := previousSnapshotInfoValue.Set(
			previousSnapshotInfo,
			pool,
			db.pointerNode,
			db.snapshotInfoNode,
		); err != nil {
			return err
		}
	}

	if snapshotID == db.singularityNode.FirstSnapshotID {
		db.singularityNode.FirstSnapshotID = snapshotInfo.NextSnapshotID
	}
	if snapshotID == db.snapshotInfo.PreviousSnapshotID {
		db.snapshotInfo.PreviousSnapshotID = db.singularityNode.LastSnapshotID
	}

	return nil
}

// Commit commits current snapshot and returns next one.
func (db *DB) Commit(pool *alloc.Pool[types.LogicalAddress]) error {
	doneCh := make(chan struct{})
	db.eventCh <- types.DBCommitEvent{
		DoneCh: doneCh,
	}
	<-doneCh

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
			if err := spaceToCommit.SpaceInfoValue.Set(types.SpaceInfo{
				HashMod: *spaceToCommit.HashMod,
				State:   *spaceToCommit.PInfo.State,
				Pointer: *spaceToCommit.PInfo.SpacePointer,
			}, pool, db.pointerNode, db.spaceInfoNode); err != nil {
				return err
			}
		}
	}

	if err := db.commitDeallocationLists(pool); err != nil {
		return err
	}

	nextSnapshotInfoValue := db.snapshots.Get(db.singularityNode.LastSnapshotID, db.pointerNode, db.snapshotInfoNode)
	if err := nextSnapshotInfoValue.Set(
		db.snapshotInfo,
		pool,
		db.pointerNode,
		db.snapshotInfoNode,
	); err != nil {
		return err
	}

	doneCh = make(chan struct{})
	db.eventCh <- types.DBCommitEvent{
		DoneCh: doneCh,
	}
	<-doneCh

	*photon.FromPointer[types.SingularityNode](db.config.State.Node(0)) = db.singularityNode

	db.availableSnapshots[db.singularityNode.LastSnapshotID] = struct{}{}

	return db.prepareNextSnapshot()
}

// Close closed DB.
func (db *DB) Close() {
	close(db.eventCh)
	db.config.State.Close()

	<-db.doneCh
}

// Run runs db goroutines.
func (db *DB) Run(ctx context.Context) error {
	defer close(db.doneCh)

	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		storeRequestCh := make(chan types.StoreRequest, 500)

		spawn("state", parallel.Continue, db.config.State.Run)
		spawn("events", parallel.Continue, func(ctx context.Context) error {
			defer close(storeRequestCh)
			return db.processEvents(ctx, db.eventCh, storeRequestCh)
		})
		spawn("storeRequests", parallel.Continue, func(ctx context.Context) error {
			return db.processStoreRequests(ctx, storeRequestCh)
		})

		return nil
	})
}

func (db *DB) processEvents(
	ctx context.Context,
	eventCh <-chan any,
	storeRequestCh chan<- types.StoreRequest,
) error {
	volatilePool := db.config.State.NewPool()
	persistentPool := alloc.NewPool[types.PhysicalAddress](db.persistentAllocationCh, db.persistentAllocationCh)

	pointerNode := db.pointerNodeAllocator.NewNode()
	parentPointerNode := db.pointerNodeAllocator.NewNode()
	listNode := db.listNodeAllocator.NewNode()

	for event := range eventCh {
		switch e := event.(type) {
		case types.SpacePointerNodeAllocatedEvent:
			if err := db.storeSpacePointerNodes(
				e.NodeAddress,
				e.RootPointer,
				pointerNode,
				parentPointerNode,
				persistentPool,
				storeRequestCh,
			); err != nil {
				return err
			}
		case types.SpaceDataNodeAllocatedEvent:
			header := photon.FromPointer[space.DataNodeHeader](db.config.State.Node(e.Pointer.LogicalAddress))
			header.RevisionHeader.SnapshotID = db.singularityNode.LastSnapshotID

			revision := atomic.AddUint64(&header.RevisionHeader.Revision, 1)

			var err error
			e.Pointer.PhysicalAddress, err = persistentPool.Allocate()
			if err != nil {
				return err
			}

			storeRequestCh <- types.StoreRequest{
				Revision: revision,
				Pointer:  e.Pointer,
			}

			if e.PNodeAddress == 0 {
				continue
			}

			if err := db.storeSpacePointerNodes(
				e.PNodeAddress,
				e.RootPointer,
				pointerNode,
				parentPointerNode,
				persistentPool,
				storeRequestCh,
			); err != nil {
				return err
			}
		case types.SpaceDataNodeUpdatedEvent:
			header := photon.FromPointer[space.DataNodeHeader](db.config.State.Node(e.Pointer.LogicalAddress))
			header.RevisionHeader.SnapshotID = db.singularityNode.LastSnapshotID

			revision := atomic.AddUint64(&header.RevisionHeader.Revision, 1)

			if header.RevisionHeader.SnapshotID != db.singularityNode.LastSnapshotID {
				header.RevisionHeader.SnapshotID = db.singularityNode.LastSnapshotID
				var err error
				e.Pointer.PhysicalAddress, err = persistentPool.Allocate()
				if err != nil {
					return err
				}

				if e.PNodeAddress != 0 {
					if err := db.storeSpacePointerNodes(
						e.PNodeAddress,
						e.RootPointer,
						pointerNode,
						parentPointerNode,
						persistentPool,
						storeRequestCh,
					); err != nil {
						return err
					}
				}
			}

			storeRequestCh <- types.StoreRequest{
				Revision: revision,
				Pointer:  e.Pointer,
			}
		case types.SpaceDataNodeDeallocationEvent:
			volatilePool.Deallocate(e.Pointer.LogicalAddress)
			persistentPool.Deallocate(e.Pointer.PhysicalAddress)
		case types.SpaceDeallocationEvent:
			space.Deallocate(
				db.config.State,
				e.SpaceRoot,
				volatilePool,
				persistentPool,
				db.pointerNodeAllocator,
				pointerNode,
			)
		case types.ListNodeAllocatedEvent:
			header := photon.FromPointer[list.NodeHeader](db.config.State.Node(e.Pointer.LogicalAddress))
			header.RevisionHeader.SnapshotID = db.singularityNode.LastSnapshotID
		case types.ListNodeUpdatedEvent:
			header := photon.FromPointer[list.NodeHeader](db.config.State.Node(e.Pointer.LogicalAddress))
			if header.RevisionHeader.SnapshotID != db.singularityNode.LastSnapshotID {
				header.RevisionHeader.SnapshotID = db.singularityNode.LastSnapshotID
			}
		case types.ListDeallocationEvent:
			list.Deallocate(
				db.config.State,
				e.ListRoot,
				volatilePool,
				persistentPool,
				db.listNodeAllocator,
				listNode,
			)
		case types.DBCommitEvent:
			ch := make(chan struct{}, 1)
			storeRequestCh <- types.StoreRequest{
				DoneCh: ch,
			}
			<-ch
			close(e.DoneCh)
		}
	}
	return errors.WithStack(ctx.Err())
}

func (db *DB) storeSpacePointerNodes(
	nodeAddress types.LogicalAddress,
	rootPointer *types.Pointer,
	pointerNode *space.Node[space.PointerNodeHeader, types.SpacePointer],
	parentPointerNode *space.Node[space.PointerNodeHeader, types.SpacePointer],
	persistentPool *alloc.Pool[types.PhysicalAddress],
	storeRequestCh chan<- types.StoreRequest,
) error {
	db.pointerNodeAllocator.Get(nodeAddress, pointerNode)

	for {
		var pointer *types.Pointer
		if pointerNode.Header.ParentNodeAddress == 0 {
			pointer = rootPointer
		} else {
			db.pointerNodeAllocator.Get(pointerNode.Header.ParentNodeAddress, parentPointerNode)
			spacePointer, _ := parentPointerNode.Item(pointerNode.Header.ParentNodeIndex)
			pointer = &spacePointer.Pointer
		}

		revision := atomic.AddUint64(&pointerNode.Header.RevisionHeader.Revision, 1)

		if pointerNode.Header.RevisionHeader.SnapshotID != db.singularityNode.LastSnapshotID {
			pointerNode.Header.RevisionHeader.SnapshotID = db.singularityNode.LastSnapshotID
			pointer.PhysicalAddress = 0
		}

		terminate := true
		if pointer.PhysicalAddress == 0 {
			var err error
			pointer.PhysicalAddress, err = persistentPool.Allocate()
			if err != nil {
				return err
			}

			terminate = pointer == rootPointer
		}

		storeRequestCh <- types.StoreRequest{
			Revision: revision,
			Pointer:  pointer,
		}

		if terminate {
			return nil
		}

		pointerNode = parentPointerNode
	}
}

func (db *DB) processStoreRequests(ctx context.Context, storeRequestCh <-chan types.StoreRequest) error {
	for req := range storeRequestCh {
		if req.Pointer != nil {
			header := photon.FromPointer[types.RevisionHeader](db.config.State.Node(req.Pointer.LogicalAddress))
			revision := atomic.LoadUint64(&header.Revision)

			if revision != req.Revision {
				continue
			}

			if err := db.config.Store.Write(
				req.Pointer.PhysicalAddress,
				db.config.State.Bytes(req.Pointer.LogicalAddress),
			); err != nil {
				return err
			}

			continue
		}

		req.DoneCh <- struct{}{}
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

	db.singularityNode.LastSnapshotID = snapshotID

	return nil
}

func (db *DB) commitDeallocationLists(pool *alloc.Pool[types.LogicalAddress]) error {
	if len(db.deallocationListsToCommit) > 0 {
		lists := make([]types.SnapshotID, 0, len(db.deallocationListsToCommit))
		for snapshotID := range db.deallocationListsToCommit {
			lists = append(lists, snapshotID)
		}
		sort.Slice(lists, func(i, j int) bool { return lists[i] < lists[j] })

		for _, snapshotID := range lists {
			err := db.deallocationLists.Get(snapshotID, db.pointerNode, db.snapshotToNodeNode).
				Set(*db.deallocationListsToCommit[snapshotID].ListRoot, pool, db.pointerNode, db.snapshotToNodeNode)
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
		spaceInfoValue := db.spaces.Get(spaceID, db.pointerNode, db.spaceInfoNode)
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

	dataNodeAllocator, err := space.NewNodeAllocator[space.DataNodeHeader, types.DataItem[K, V]](db.config.State)
	if err != nil {
		return nil, err
	}

	return space.New[K, V](space.Config[K, V]{
		HashMod:              s.HashMod,
		SpaceRoot:            s.PInfo,
		State:                db.config.State,
		StorageEventCh:       db.eventCh,
		PointerNodeAllocator: db.pointerNodeAllocator,
		DataNodeAllocator:    dataNodeAllocator,
		MassEntry:            mass.New[space.Entry[K, V]](1000),
	}), nil
}
