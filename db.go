package quantum

import (
	"context"
	"sort"
	"sync/atomic"

	"github.com/pkg/errors"

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
	OriginalPointer types.Pointer
}

// ListToCommit contains cached deallocation list.
type ListToCommit struct {
	List     *list.List
	ListRoot *types.Pointer
}

// New creates new database.
func New(config Config) (*DB, error) {
	pointerNodeAssistant, err := space.NewNodeAssistant[space.PointerNodeHeader, types.Pointer](config.State)
	if err != nil {
		return nil, err
	}

	snapshotInfoNodeAssistant, err := space.NewNodeAssistant[
		space.DataNodeHeader,
		types.DataItem[types.SnapshotID, types.SnapshotInfo],
	](
		config.State,
	)
	if err != nil {
		return nil, err
	}

	snapshotToNodeNodeAssistant, err := space.NewNodeAssistant[
		space.DataNodeHeader,
		types.DataItem[types.SnapshotID, types.Pointer],
	](
		config.State,
	)
	if err != nil {
		return nil, err
	}

	listNodeAssistant, err := list.NewNodeAssistant(config.State)
	if err != nil {
		return nil, err
	}

	db := &DB{
		config:                      config,
		singularityNode:             photon.FromPointer[types.SingularityNode](config.State.Node(0)),
		singularityNodePointer:      &types.Pointer{},
		pointerNodeAssistant:        pointerNodeAssistant,
		snapshotToNodeNodeAssistant: snapshotToNodeNodeAssistant,
		listNodeAssistant:           listNodeAssistant,
		pointerNode:                 pointerNodeAssistant.NewNode(),
		snapshotInfoNode:            snapshotInfoNodeAssistant.NewNode(),
		snapshotToNodeNode:          snapshotToNodeNodeAssistant.NewNode(),
		listNode:                    listNodeAssistant.NewNode(),
		massSnapshotToNodeEntry:     mass.New[space.Entry[types.SnapshotID, types.Pointer]](1000),
		deallocationListsToCommit:   map[types.SnapshotID]ListToCommit{},
		eventCh:                     make(chan any, 100),
		storeRequestCh:              make(chan types.StoreRequest, 1000),
	}

	// Logical nodes might be deallocated immediately.
	db.snapshots = space.New[types.SnapshotID, types.SnapshotInfo](space.Config[types.SnapshotID, types.SnapshotInfo]{
		HashMod: &db.singularityNode.SnapshotRoot.HashMod,
		SpaceRoot: types.ParentEntry{
			State:   &db.singularityNode.SnapshotRoot.State,
			Pointer: &db.singularityNode.SnapshotRoot.Pointer,
		},
		State:                 config.State,
		PointerNodeAssistant:  pointerNodeAssistant,
		DataNodeAssistant:     snapshotInfoNodeAssistant,
		MassEntry:             mass.New[space.Entry[types.SnapshotID, types.SnapshotInfo]](1000),
		EventCh:               db.eventCh,
		ImmediateDeallocation: true,
	})

	db.deallocationLists = space.New[types.SnapshotID, types.Pointer](
		space.Config[types.SnapshotID, types.Pointer]{
			HashMod: &db.snapshotInfo.DeallocationRoot.HashMod,
			SpaceRoot: types.ParentEntry{
				State:   &db.snapshotInfo.DeallocationRoot.State,
				Pointer: &db.snapshotInfo.DeallocationRoot.Pointer,
			},
			State:                 config.State,
			PointerNodeAssistant:  pointerNodeAssistant,
			DataNodeAssistant:     snapshotToNodeNodeAssistant,
			MassEntry:             mass.New[space.Entry[types.SnapshotID, types.Pointer]](1000),
			EventCh:               db.eventCh,
			ImmediateDeallocation: true,
		},
	)

	if err := db.prepareNextSnapshot(); err != nil {
		return nil, err
	}
	return db, nil
}

// DB represents the database.
// FIXME (wojciech): Need a mechanism to quit/fail Set (and any other) operation if Run exits.
type DB struct {
	config                 Config
	singularityNode        *types.SingularityNode
	singularityNodePointer *types.Pointer
	snapshotInfo           types.SnapshotInfo
	snapshots              *space.Space[types.SnapshotID, types.SnapshotInfo]
	deallocationLists      *space.Space[types.SnapshotID, types.Pointer]

	pointerNodeAssistant        *space.NodeAssistant[space.PointerNodeHeader, types.Pointer]
	snapshotToNodeNodeAssistant *space.NodeAssistant[space.DataNodeHeader, types.DataItem[types.SnapshotID, types.Pointer]]
	listNodeAssistant           *list.NodeAssistant

	pointerNode        *space.Node[space.PointerNodeHeader, types.Pointer]
	snapshotInfoNode   *space.Node[space.DataNodeHeader, types.DataItem[types.SnapshotID, types.SnapshotInfo]]
	snapshotToNodeNode *space.Node[space.DataNodeHeader, types.DataItem[types.SnapshotID, types.Pointer]]
	listNode           *list.Node

	massSnapshotToNodeEntry *mass.Mass[space.Entry[types.SnapshotID, types.Pointer]]

	deallocationListsToCommit map[types.SnapshotID]ListToCommit

	eventCh        chan any
	storeRequestCh chan types.StoreRequest
}

// NewVolatilePool creates new volatile allocation pool.
func (db *DB) NewVolatilePool() *alloc.Pool[types.VolatileAddress] {
	return db.config.State.NewVolatilePool()
}

// NewPersistentPool creates new persistent allocation pool.
func (db *DB) NewPersistentPool() *alloc.Pool[types.PersistentAddress] {
	return db.config.State.NewPersistentPool()
}

// DeleteSnapshot deletes snapshot.
func (db *DB) DeleteSnapshot(
	snapshotID types.SnapshotID,
	volatilePool *alloc.Pool[types.VolatileAddress],
	persistentPool *alloc.Pool[types.PersistentAddress],
) error {
	snapshotInfoValue := db.snapshots.Find(snapshotID, db.pointerNode, db.snapshotInfoNode)
	if !snapshotInfoValue.Exists() {
		return errors.Errorf("snapshot %d does not exist", snapshotID)
	}

	if err := snapshotInfoValue.Delete(db.pointerNode, db.snapshotInfoNode); err != nil {
		return err
	}

	snapshotInfo := snapshotInfoValue.Value()

	var nextSnapshotInfo *types.SnapshotInfo
	var nextDeallocationListRoot types.ParentEntry
	var nextDeallocationLists *space.Space[types.SnapshotID, types.Pointer]
	if snapshotInfo.NextSnapshotID < db.singularityNode.LastSnapshotID {
		nextSnapshotInfoValue := db.snapshots.Find(snapshotInfo.NextSnapshotID, db.pointerNode, db.snapshotInfoNode)
		if !nextSnapshotInfoValue.Exists() {
			return errors.Errorf("snapshot %d does not exist", snapshotID)
		}
		tmpNextSnapshotInfo := nextSnapshotInfoValue.Value()
		nextSnapshotInfo = &tmpNextSnapshotInfo

		nextDeallocationListRoot = types.ParentEntry{
			State:   &nextSnapshotInfo.DeallocationRoot.State,
			Pointer: &nextSnapshotInfo.DeallocationRoot.Pointer,
		}
		nextDeallocationLists = space.New[types.SnapshotID, types.Pointer](
			space.Config[types.SnapshotID, types.Pointer]{
				HashMod:              &nextSnapshotInfo.DeallocationRoot.HashMod,
				SpaceRoot:            nextDeallocationListRoot,
				State:                db.config.State,
				PointerNodeAssistant: db.pointerNodeAssistant,
				DataNodeAssistant:    db.snapshotToNodeNodeAssistant,
				MassEntry:            db.massSnapshotToNodeEntry,
				EventCh:              db.eventCh,
			},
		)
	} else {
		nextSnapshotInfo = &db.snapshotInfo
		nextDeallocationListRoot = types.ParentEntry{
			State:   &db.snapshotInfo.DeallocationRoot.State,
			Pointer: &db.snapshotInfo.DeallocationRoot.Pointer,
		}
		nextDeallocationLists = db.deallocationLists
	}

	deallocationListsRoot := types.ParentEntry{
		State:   &snapshotInfo.DeallocationRoot.State,
		Pointer: &snapshotInfo.DeallocationRoot.Pointer,
	}
	deallocationLists := space.New[types.SnapshotID, types.Pointer](
		space.Config[types.SnapshotID, types.Pointer]{
			HashMod:              &snapshotInfo.DeallocationRoot.HashMod,
			SpaceRoot:            deallocationListsRoot,
			State:                db.config.State,
			PointerNodeAssistant: db.pointerNodeAssistant,
			DataNodeAssistant:    db.snapshotToNodeNodeAssistant,
			MassEntry:            db.massSnapshotToNodeEntry,
		},
	)

	var startSnapshotID types.SnapshotID
	if snapshotID != db.singularityNode.FirstSnapshotID {
		startSnapshotID = snapshotInfo.PreviousSnapshotID + 1
	}
	for nextDeallocSnapshot := range nextDeallocationLists.Iterator(db.pointerNode, db.snapshotToNodeNode) {
		if nextDeallocSnapshot.Key >= startSnapshotID && nextDeallocSnapshot.Key <= snapshotID {
			db.eventCh <- types.ListDeallocationEvent{
				ListRoot: nextDeallocSnapshot.Value,
			}

			continue
		}

		deallocationListValue := deallocationLists.Find(nextDeallocSnapshot.Key, db.pointerNode, db.snapshotToNodeNode)
		listNodeAddress := deallocationListValue.Value()
		newListNodeAddress := listNodeAddress
		list := list.New(list.Config{
			ListRoot:       &newListNodeAddress,
			State:          db.config.State,
			NodeAssistant:  db.listNodeAssistant,
			StoreRequestCh: db.storeRequestCh,
		})
		if err := list.Attach(
			nextDeallocSnapshot.Value,
			db.singularityNode.LastSnapshotID,
			volatilePool,
			persistentPool,
			db.listNode,
		); err != nil {
			return err
		}
		if newListNodeAddress != listNodeAddress {
			if err := deallocationListValue.Set(
				newListNodeAddress,
				volatilePool,
				db.pointerNode,
				db.snapshotToNodeNode,
			); err != nil {
				return err
			}
		}
	}

	nextSnapshotInfo.DeallocationRoot = snapshotInfo.DeallocationRoot

	db.eventCh <- types.SpaceDeallocationEvent{
		SpaceRoot: nextDeallocationListRoot,
	}

	if snapshotID == db.singularityNode.FirstSnapshotID {
		nextSnapshotInfo.PreviousSnapshotID = snapshotInfo.NextSnapshotID
	} else {
		nextSnapshotInfo.PreviousSnapshotID = snapshotInfo.PreviousSnapshotID
	}

	if snapshotInfo.NextSnapshotID < db.singularityNode.LastSnapshotID {
		nextSnapshotInfoValue := db.snapshots.Find(snapshotInfo.NextSnapshotID, db.pointerNode, db.snapshotInfoNode)
		if err := nextSnapshotInfoValue.Set(
			*nextSnapshotInfo,
			volatilePool,
			db.pointerNode,
			db.snapshotInfoNode,
		); err != nil {
			return err
		}
	}

	if snapshotID > db.singularityNode.FirstSnapshotID {
		previousSnapshotInfoValue := db.snapshots.Find(snapshotInfo.PreviousSnapshotID, db.pointerNode,
			db.snapshotInfoNode)
		if !previousSnapshotInfoValue.Exists() {
			return errors.Errorf("snapshot %d does not exist", snapshotID)
		}

		previousSnapshotInfo := previousSnapshotInfoValue.Value()
		previousSnapshotInfo.NextSnapshotID = snapshotInfo.NextSnapshotID

		if err := previousSnapshotInfoValue.Set(
			previousSnapshotInfo,
			volatilePool,
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
func (db *DB) Commit(
	ctx context.Context,
	volatilePool *alloc.Pool[types.VolatileAddress],
	persistentPool *alloc.Pool[types.PersistentAddress],
) error {
	//nolint:nestif
	if len(db.deallocationListsToCommit) > 0 {
		if err := db.sync(ctx); err != nil {
			return err
		}

		lists := make([]types.SnapshotID, 0, len(db.deallocationListsToCommit))
		for snapshotID := range db.deallocationListsToCommit {
			lists = append(lists, snapshotID)
		}
		sort.Slice(lists, func(i, j int) bool { return lists[i] < lists[j] })

		for _, snapshotID := range lists {
			deallocationListValue := db.deallocationLists.Find(snapshotID, db.pointerNode, db.snapshotToNodeNode)
			if deallocationListValue.Exists() {
				if err := db.deallocationListsToCommit[snapshotID].List.Attach(
					deallocationListValue.Value(),
					db.singularityNode.LastSnapshotID,
					volatilePool,
					persistentPool,
					db.listNode,
				); err != nil {
					return err
				}
			}
			if err := deallocationListValue.Set(
				*db.deallocationListsToCommit[snapshotID].ListRoot,
				volatilePool,
				db.pointerNode,
				db.snapshotToNodeNode,
			); err != nil {
				return err
			}
		}

		clear(db.deallocationListsToCommit)
	}

	nextSnapshotInfoValue := db.snapshots.Find(db.singularityNode.LastSnapshotID, db.pointerNode, db.snapshotInfoNode)
	if err := nextSnapshotInfoValue.Set(
		db.snapshotInfo,
		volatilePool,
		db.pointerNode,
		db.snapshotInfoNode,
	); err != nil {
		return err
	}

	syncCh := make(chan struct{})
	// FIXME (wojciech): Use RoundRobin for storing singularity node.
	db.eventCh <- types.DBCommitEvent{
		SingularityNodePointer: db.singularityNodePointer,
		SyncCh:                 syncCh,
	}
	<-syncCh

	db.config.State.Commit()

	if err := db.prepareNextSnapshot(); err != nil {
		return err
	}

	return errors.WithStack(ctx.Err())
}

// Run runs db goroutines.
// FIXME (wojciech): Test errors in goroutines.
func (db *DB) Run(ctx context.Context) error {
	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("watchdog", parallel.Fail, func(ctx context.Context) error {
			<-ctx.Done()

			close(db.eventCh)
			for range db.eventCh {
			}
			for range db.storeRequestCh {
			}

			return errors.WithStack(ctx.Err())
		})
		spawn("state", parallel.Fail, db.config.State.Run)
		spawn("events", parallel.Continue, func(ctx context.Context) error {
			defer close(db.storeRequestCh)

			return db.processEvents(ctx, db.eventCh, db.storeRequestCh)
		})
		spawn("store", parallel.Continue, func(ctx context.Context) error {
			return db.processStoreRequests(ctx, db.storeRequestCh)
		})

		return nil
	})
}

func (db *DB) processEvents(
	ctx context.Context,
	eventCh <-chan any,
	storeRequestCh chan<- types.StoreRequest,
) error {
	volatilePool := db.NewVolatilePool()
	persistentPool := db.NewPersistentPool()

	pointerNode := db.pointerNodeAssistant.NewNode()
	parentPointerNode := db.pointerNodeAssistant.NewNode()
	listNode := db.listNodeAssistant.NewNode()

	for event := range eventCh {
		switch e := event.(type) {
		case types.SpacePointerNodeAllocatedEvent:
			if err := db.storeSpacePointerNodes(
				e.NodeAddress,
				e.RootPointer,
				pointerNode,
				parentPointerNode,
				listNode,
				volatilePool,
				persistentPool,
				storeRequestCh,
				e.ImmediateDeallocation,
			); err != nil {
				return err
			}
		case types.SpaceDataNodeAllocatedEvent:
			header := photon.FromPointer[space.DataNodeHeader](db.config.State.Node(e.Pointer.LogicalAddress))
			header.SnapshotID = db.singularityNode.LastSnapshotID

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
				listNode,
				volatilePool,
				persistentPool,
				storeRequestCh,
				e.ImmediateDeallocation,
			); err != nil {
				return err
			}
		case types.SpaceDataNodeUpdatedEvent:
			header := photon.FromPointer[space.DataNodeHeader](db.config.State.Node(e.Pointer.LogicalAddress))
			revision := atomic.AddUint64(&header.RevisionHeader.Revision, 1)

			//nolint:nestif
			if header.SnapshotID != db.singularityNode.LastSnapshotID {
				if e.Pointer.PhysicalAddress != 0 {
					if err := db.deallocateNode(
						*e.Pointer,
						header.SnapshotID,
						volatilePool,
						persistentPool,
						listNode,
						e.ImmediateDeallocation,
						false,
					); err != nil {
						return err
					}
				}

				header.SnapshotID = db.singularityNode.LastSnapshotID

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
						listNode,
						volatilePool,
						persistentPool,
						storeRequestCh,
						e.ImmediateDeallocation,
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
			header := photon.FromPointer[space.DataNodeHeader](db.config.State.Node(e.Pointer.LogicalAddress))

			if header.SnapshotID == db.singularityNode.LastSnapshotID {
				// This is done to ignore any potential pending writes of this block.
				atomic.AddUint64(&header.RevisionHeader.Revision, 1)
			}

			if err := db.deallocateNode(
				e.Pointer,
				header.SnapshotID,
				volatilePool,
				persistentPool,
				listNode,
				e.ImmediateDeallocation,
				true,
			); err != nil {
				return err
			}
		case types.SpaceDeallocationEvent:
			space.Deallocate(
				e.SpaceRoot,
				volatilePool,
				persistentPool,
				db.pointerNodeAssistant,
				pointerNode,
			)
		case types.ListDeallocationEvent:
			list.Deallocate(
				e.ListRoot,
				volatilePool,
				persistentPool,
				db.listNodeAssistant,
				listNode,
			)
		case types.SyncEvent:
			close(e.SyncCh)
		case types.DBCommitEvent:
			storeRequestCh <- types.StoreRequest{
				Revision: 0,
				Pointer:  e.SingularityNodePointer,
				SyncCh:   e.SyncCh,
			}
		}
	}
	return errors.WithStack(ctx.Err())
}

func (db *DB) storeSpacePointerNodes(
	nodeAddress types.VolatileAddress,
	rootPointer *types.Pointer,
	pointerNode *space.Node[space.PointerNodeHeader, types.Pointer],
	parentPointerNode *space.Node[space.PointerNodeHeader, types.Pointer],
	listNode *list.Node,
	volatilePool *alloc.Pool[types.VolatileAddress],
	persistentPool *alloc.Pool[types.PersistentAddress],
	storeRequestCh chan<- types.StoreRequest,
	immediateDeallocation bool,
) error {
	db.pointerNodeAssistant.Project(nodeAddress, pointerNode)

	for {
		var pointer *types.Pointer
		if pointerNode.Header.ParentNodeAddress == 0 {
			pointer = rootPointer
		} else {
			db.pointerNodeAssistant.Project(pointerNode.Header.ParentNodeAddress, parentPointerNode)
			pointer, _ = parentPointerNode.Item(pointerNode.Header.ParentNodeIndex)
		}

		revision := atomic.AddUint64(&pointerNode.Header.RevisionHeader.Revision, 1)

		if pointerNode.Header.SnapshotID != db.singularityNode.LastSnapshotID {
			if pointer.PhysicalAddress != 0 {
				if err := db.deallocateNode(
					*pointer,
					pointerNode.Header.SnapshotID,
					volatilePool,
					persistentPool,
					listNode,
					immediateDeallocation,
					false,
				); err != nil {
					return err
				}
				pointer.PhysicalAddress = 0
			}
			pointerNode.Header.SnapshotID = db.singularityNode.LastSnapshotID
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

		if req.SyncCh != nil {
			if err := db.config.Store.Sync(); err != nil {
				return err
			}
			req.SyncCh <- struct{}{}
		}
	}
	return errors.WithStack(ctx.Err())
}

func (db *DB) deallocateNode(
	pointer types.Pointer,
	srcSnapshotID types.SnapshotID,
	volatilePool *alloc.Pool[types.VolatileAddress],
	persistentPool *alloc.Pool[types.PersistentAddress],
	node *list.Node,
	immediateDeallocation bool,
	volatileDeallocation bool,
) error {
	if db.snapshotInfo.PreviousSnapshotID == db.singularityNode.LastSnapshotID ||
		srcSnapshotID > db.snapshotInfo.PreviousSnapshotID || immediateDeallocation {
		volatilePool.Deallocate(pointer.LogicalAddress)
		persistentPool.Deallocate(pointer.PhysicalAddress)

		return nil
	}

	if volatileDeallocation {
		volatilePool.Deallocate(pointer.LogicalAddress)
	}

	l, exists := db.deallocationListsToCommit[srcSnapshotID]
	if !exists {
		l.ListRoot = &types.Pointer{}
		l.List = list.New(list.Config{
			ListRoot:       l.ListRoot,
			State:          db.config.State,
			NodeAssistant:  db.listNodeAssistant,
			StoreRequestCh: db.storeRequestCh,
		})
		db.deallocationListsToCommit[srcSnapshotID] = l
	}

	return l.List.Add(
		pointer,
		db.singularityNode.LastSnapshotID,
		volatilePool,
		persistentPool,
		node,
	)
}

func (db *DB) prepareNextSnapshot() error {
	var snapshotID types.SnapshotID
	if db.singularityNode.SnapshotRoot.State != types.StateFree {
		snapshotID = db.singularityNode.LastSnapshotID + 1
	}

	db.snapshotInfo.PreviousSnapshotID = db.singularityNode.LastSnapshotID
	db.snapshotInfo.NextSnapshotID = snapshotID + 1
	db.snapshotInfo.DeallocationRoot = types.SpaceInfo{}
	db.singularityNode.LastSnapshotID = snapshotID

	return nil
}

func (db *DB) sync(ctx context.Context) error {
	syncCh := make(chan struct{})

	select {
	case <-ctx.Done():
		return errors.WithStack(ctx.Err())
	case db.eventCh <- types.SyncEvent{
		SyncCh: syncCh,
	}:
	}

	select {
	case <-ctx.Done():
		return errors.WithStack(ctx.Err())
	case <-syncCh:
	}

	return nil
}

// GetSpace retrieves space from snapshot.
func GetSpace[K, V comparable](spaceID types.SpaceID, db *DB) (*space.Space[K, V], error) {
	if spaceID >= types.SpaceID(len(db.snapshotInfo.Spaces)) {
		return nil, errors.Errorf("space %d is not defined", spaceID)
	}
	spaceInfo := &db.snapshotInfo.Spaces[spaceID]

	dataNodeAssistant, err := space.NewNodeAssistant[space.DataNodeHeader, types.DataItem[K, V]](db.config.State)
	if err != nil {
		return nil, err
	}

	return space.New[K, V](space.Config[K, V]{
		HashMod: &spaceInfo.HashMod,
		SpaceRoot: types.ParentEntry{
			State:   &spaceInfo.State,
			Pointer: &spaceInfo.Pointer,
		},
		State:                db.config.State,
		EventCh:              db.eventCh,
		PointerNodeAssistant: db.pointerNodeAssistant,
		DataNodeAssistant:    dataNodeAssistant,
		MassEntry:            mass.New[space.Entry[K, V]](1000),
	}), nil
}
