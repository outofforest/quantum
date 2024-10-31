package quantum

import (
	"context"
	"fmt"
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
	"github.com/outofforest/quantum/queue"
	"github.com/outofforest/quantum/space"
	"github.com/outofforest/quantum/types"
)

// Config stores snapshot configuration.
type Config struct {
	State  *alloc.State
	Stores []persistent.Store
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
	pointerNodeAssistant, err := space.NewNodeAssistant[types.Pointer](config.State)
	if err != nil {
		return nil, err
	}

	snapshotInfoNodeAssistant, err := space.NewNodeAssistant[types.DataItem[types.SnapshotID, types.SnapshotInfo]](
		config.State,
	)
	if err != nil {
		return nil, err
	}

	snapshotToNodeNodeAssistant, err := space.NewNodeAssistant[types.DataItem[types.SnapshotID, types.Pointer]](
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
		pointerNodeAssistant:        pointerNodeAssistant,
		snapshotToNodeNodeAssistant: snapshotToNodeNodeAssistant,
		listNodeAssistant:           listNodeAssistant,
		pointerNode:                 pointerNodeAssistant.NewNode(),
		snapshotInfoNode:            snapshotInfoNodeAssistant.NewNode(),
		snapshotToNodeNode:          snapshotToNodeNodeAssistant.NewNode(),
		listNode:                    listNodeAssistant.NewNode(),
		massSnapshotToNodeEntry:     mass.New[space.Entry[types.SnapshotID, types.Pointer]](1000),
		deallocationListsToCommit:   map[types.SnapshotID]ListToCommit{},
		storeQ:                      queue.New(),
		closedCh:                    make(chan struct{}),
	}

	// Logical nodes might be deallocated immediately.
	db.snapshots = space.New[types.SnapshotID, types.SnapshotInfo](space.Config[types.SnapshotID, types.SnapshotInfo]{
		SpaceRoot: types.ParentEntry{
			State:   &db.singularityNode.SnapshotRoot.State,
			Pointer: &db.singularityNode.SnapshotRoot.Pointer,
		},
		State:                 config.State,
		PointerNodeAssistant:  pointerNodeAssistant,
		DataNodeAssistant:     snapshotInfoNodeAssistant,
		MassEntry:             mass.New[space.Entry[types.SnapshotID, types.SnapshotInfo]](1000),
		ImmediateDeallocation: true,
		StoreQ:                db.storeQ,
	})

	db.deallocationLists = space.New[types.SnapshotID, types.Pointer](
		space.Config[types.SnapshotID, types.Pointer]{
			SpaceRoot: types.ParentEntry{
				State:   &db.snapshotInfo.DeallocationRoot.State,
				Pointer: &db.snapshotInfo.DeallocationRoot.Pointer,
			},
			State:                 config.State,
			PointerNodeAssistant:  pointerNodeAssistant,
			DataNodeAssistant:     snapshotToNodeNodeAssistant,
			MassEntry:             mass.New[space.Entry[types.SnapshotID, types.Pointer]](1000),
			ImmediateDeallocation: true,
			StoreQ:                db.storeQ,
		},
	)

	if err := db.prepareNextSnapshot(); err != nil {
		return nil, err
	}
	return db, nil
}

// DB represents the database.
type DB struct {
	config            Config
	singularityNode   *types.SingularityNode
	snapshotInfo      types.SnapshotInfo
	snapshots         *space.Space[types.SnapshotID, types.SnapshotInfo]
	deallocationLists *space.Space[types.SnapshotID, types.Pointer]

	pointerNodeAssistant        *space.NodeAssistant[types.Pointer]
	snapshotToNodeNodeAssistant *space.NodeAssistant[types.DataItem[types.SnapshotID, types.Pointer]]
	listNodeAssistant           *list.NodeAssistant

	pointerNode        *space.Node[types.Pointer]
	snapshotInfoNode   *space.Node[types.DataItem[types.SnapshotID, types.SnapshotInfo]]
	snapshotToNodeNode *space.Node[types.DataItem[types.SnapshotID, types.Pointer]]
	listNode           *list.Node

	massSnapshotToNodeEntry *mass.Mass[space.Entry[types.SnapshotID, types.Pointer]]

	deallocationListsToCommit map[types.SnapshotID]ListToCommit

	storeQ   *queue.Queue
	closedCh chan struct{}
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
				SpaceRoot:            nextDeallocationListRoot,
				State:                db.config.State,
				PointerNodeAssistant: db.pointerNodeAssistant,
				DataNodeAssistant:    db.snapshotToNodeNodeAssistant,
				MassEntry:            db.massSnapshotToNodeEntry,
				StoreQ:               db.storeQ,
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
			SpaceRoot:            deallocationListsRoot,
			State:                db.config.State,
			PointerNodeAssistant: db.pointerNodeAssistant,
			DataNodeAssistant:    db.snapshotToNodeNodeAssistant,
			MassEntry:            db.massSnapshotToNodeEntry,
			StoreQ:               db.storeQ,
		},
	)

	var startSnapshotID types.SnapshotID
	if snapshotID != db.singularityNode.FirstSnapshotID {
		startSnapshotID = snapshotInfo.PreviousSnapshotID + 1
	}

	for nextDeallocSnapshot := range nextDeallocationLists.Iterator(db.pointerNode, db.snapshotToNodeNode) {
		if nextDeallocSnapshot.Key >= startSnapshotID && nextDeallocSnapshot.Key <= snapshotID {
			list.Deallocate(
				nextDeallocSnapshot.Value,
				volatilePool,
				persistentPool,
				db.listNodeAssistant,
				db.listNode,
			)

			continue
		}

		deallocationListValue := deallocationLists.Find(nextDeallocSnapshot.Key, db.pointerNode, db.snapshotToNodeNode)
		listNodeAddress := deallocationListValue.Value()
		newListNodeAddress := listNodeAddress
		list := list.New(list.Config{
			ListRoot:      &newListNodeAddress,
			State:         db.config.State,
			NodeAssistant: db.listNodeAssistant,
		})

		pointerToStore, err := list.Attach(&nextDeallocSnapshot.Value, volatilePool, db.listNode)
		if err != nil {
			return err
		}
		if pointerToStore != nil {
			db.storeQ.Push(&queue.Request{
				Store:                 [queue.StoreCapacity]*types.Pointer{pointerToStore},
				PointersToStore:       1,
				ImmediateDeallocation: true,
			})
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

	space.Deallocate(
		nextDeallocationListRoot,
		volatilePool,
		persistentPool,
		db.pointerNodeAssistant,
		db.pointerNode,
	)

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
func (db *DB) Commit(volatilePool *alloc.Pool[types.VolatileAddress]) error {
	//nolint:nestif
	if len(db.deallocationListsToCommit) > 0 {
		syncCh := make(chan error, len(db.config.Stores))
		db.storeQ.Push(&queue.Request{
			Type:   queue.Sync,
			SyncCh: syncCh,
		})

		for range cap(syncCh) {
			<-syncCh
		}

		lists := make([]types.SnapshotID, 0, len(db.deallocationListsToCommit))
		for snapshotID := range db.deallocationListsToCommit {
			lists = append(lists, snapshotID)
		}
		sort.Slice(lists, func(i, j int) bool { return lists[i] < lists[j] })

		var sr queue.Request
		for _, snapshotID := range lists {
			deallocationListValue := db.deallocationLists.Find(snapshotID, db.pointerNode, db.snapshotToNodeNode)
			if deallocationListValue.Exists() {
				pointerToStore, err := db.deallocationListsToCommit[snapshotID].List.Attach(
					lo.ToPtr(deallocationListValue.Value()),
					volatilePool,
					db.listNode,
				)
				if err != nil {
					return err
				}
				if pointerToStore != nil {
					sr.Store[sr.PointersToStore] = pointerToStore
					sr.PointersToStore++
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
		if sr.PointersToStore > 0 {
			db.storeQ.Push(&sr)
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

	pointer := db.config.State.SingularityNodePointer(db.singularityNode.LastSnapshotID)
	syncCh := make(chan error, len(db.config.Stores))
	db.storeQ.Push(&queue.Request{
		RequestedRevision: pointer.Revision,
		Store:             [queue.StoreCapacity]*types.Pointer{pointer},
		PointersToStore:   1,
		SyncCh:            syncCh,
	})

	for range cap(syncCh) {
		if err := <-syncCh; err != nil {
			return err
		}
	}

	db.config.State.Commit()

	return db.prepareNextSnapshot()
}

// Close tells that there will be no more operations done.
func (db *DB) Close() {
	select {
	case <-db.closedCh:
	default:
		db.storeQ.Push(&queue.Request{Type: queue.Close})

		close(db.closedCh)
		db.config.State.Close()
	}
}

// Run runs db goroutines.
// FIXME (wojciech): Commit should return error if any goroutine fails.
func (db *DB) Run(ctx context.Context) error {
	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		revisionQReader := db.storeQ.NewReader()
		allocateQReader := revisionQReader.NewReader()
		deallocateQReader := allocateQReader.NewReader()
		storeQReaders := make([]*queue.Reader, 0, len(db.config.Stores))
		for range cap(storeQReaders) {
			storeQReaders = append(storeQReaders, deallocateQReader.NewReader())
		}

		spawn("supervisor", parallel.Exit, func(ctx context.Context) error {
			ctxDone := ctx.Done()
			for {
				select {
				case <-ctxDone:
					ctxDone = nil
				case <-db.closedCh:
					// FIXME (wojciech): Read items from store q reader
					return errors.WithStack(ctx.Err())
				}
			}
		})
		spawn("state", parallel.Continue, db.config.State.Run)
		spawn("revision", parallel.Continue, func(ctx context.Context) error {
			return db.processRevisionRequests(ctx, revisionQReader)
		})
		spawn("allocate", parallel.Continue, func(ctx context.Context) error {
			return db.processAllocationRequests(ctx, allocateQReader)
		})
		spawn("deallocate", parallel.Continue, func(ctx context.Context) error {
			return db.processDeallocationRequests(ctx, deallocateQReader)
		})
		for i, store := range db.config.Stores {
			spawn(fmt.Sprintf("store-%02d", i), parallel.Continue, func(ctx context.Context) error {
				return db.processStoreRequests(ctx, store, storeQReaders[i])
			})
		}

		return nil
	})
}

func (db *DB) processRevisionRequests(
	ctx context.Context,
	storeQReader *queue.Reader,
) error {
	var revision uint64

	var count uint64
	for {
		count = storeQReader.Count(count)
		for range count {
			req := storeQReader.Read()
			if req.Type == queue.Close {
				storeQReader.Acknowledge(count)
				return errors.WithStack(ctx.Err())
			}

			revision++
			req.RequestedRevision = revision
			for i := range req.PointersToStore {
				atomic.StoreUint64(&req.Store[i].Revision, revision)
			}
		}
	}
}

func (db *DB) processAllocationRequests(
	ctx context.Context,
	storeQReader *queue.Reader,
) error {
	massPointer := mass.New[types.Pointer](1000)
	persistentPool := db.NewPersistentPool()

	var count uint64
	for {
		count = storeQReader.Count(count)
		for range count {
			req := storeQReader.Read()
			if req.Type == queue.Close {
				storeQReader.Acknowledge(count)
				return errors.WithStack(ctx.Err())
			}

			//nolint:nestif
			if req.PointersToStore > 0 {
				// FIXME (wojciech): I believe not all the requests require deallocation.
				req.Deallocate = massPointer.NewSlice(req.PointersToStore)
				var numOfPointersToDeallocate uint
				for i := range req.PointersToStore {
					p := req.Store[i]
					if p.SnapshotID != db.singularityNode.LastSnapshotID {
						if p.PersistentAddress != 0 {
							req.Deallocate[numOfPointersToDeallocate] = *p
							numOfPointersToDeallocate++
						}
						p.SnapshotID = db.singularityNode.LastSnapshotID

						var err error
						p.PersistentAddress, err = persistentPool.Allocate()
						if err != nil {
							return err
						}
					}
				}

				req.Deallocate = req.Deallocate[:numOfPointersToDeallocate]
			}
		}
	}
}

func (db *DB) processDeallocationRequests(
	ctx context.Context,
	storeQReader *queue.Reader,
) error {
	volatilePool := db.NewVolatilePool()
	persistentPool := db.NewPersistentPool()

	listNode := db.listNodeAssistant.NewNode()

	var count uint64
	for {
		count = storeQReader.Count(count)
		for range count {
			req := storeQReader.Read()
			if req.Type == queue.Close {
				storeQReader.Acknowledge(count)
				return errors.WithStack(ctx.Err())
			}

			for _, p := range req.Deallocate {
				pointerToStore, err := db.deallocateNode(
					&p,
					req.ImmediateDeallocation,
					volatilePool,
					persistentPool,
					listNode,
				)
				if err != nil {
					return err
				}

				//nolint:staticcheck
				if pointerToStore != nil {
					// FIXME (wojciech): This must be handled somehow
				}
			}
		}
	}
}

func (db *DB) processStoreRequests(
	ctx context.Context,
	store persistent.Store,
	storeQReader *queue.Reader,
) error {
	// uniqueNodes := map[types.VolatileAddress]struct{}{}
	// var numOfWrites uint

	var count uint64
	for {
		count = storeQReader.Count(count)
		for range count {
			req := storeQReader.Read()
			if req.Type == queue.Sync {
				req.SyncCh <- nil
				continue
			}
			if req.Type == queue.Close {
				return errors.WithStack(ctx.Err())
			}
			for i := range req.PointersToStore {
				p := req.Store[i]
				if atomic.LoadUint64(&p.Revision) != req.RequestedRevision {
					continue
				}

				// uniqueNodes[p.VolatileAddress] = struct{}{}
				// numOfWrites++

				// https://github.com/zeebo/blake3
				// p.Checksum = blake3.Sum256(db.config.State.Bytes(p.VolatileAddress))

				if err := store.Write(
					p.PersistentAddress,
					db.config.State.Bytes(p.VolatileAddress),
				); err != nil {
					return err
				}
			}

			if req.SyncCh != nil {
				// fmt.Println("========== STORE")
				// fmt.Println(len(uniqueNodes))
				// fmt.Println(numOfWrites)
				// fmt.Println(float64(numOfWrites) / float64(len(uniqueNodes)))
				// clear(uniqueNodes)
				// numOfWrites = 0

				err := store.Sync()
				req.SyncCh <- err
				if err != nil {
					return err
				}
			}
		}
	}
}

func (db *DB) deallocateNode(
	pointer *types.Pointer,
	immediateDeallocation bool,
	volatilePool *alloc.Pool[types.VolatileAddress],
	persistentPool *alloc.Pool[types.PersistentAddress],
	node *list.Node,
) (*types.Pointer, error) {
	if db.snapshotInfo.PreviousSnapshotID == db.singularityNode.LastSnapshotID ||
		pointer.SnapshotID > db.snapshotInfo.PreviousSnapshotID || immediateDeallocation {
		volatilePool.Deallocate(pointer.VolatileAddress)
		persistentPool.Deallocate(pointer.PersistentAddress)

		//nolint:nilnil
		return nil, nil
	}

	l, exists := db.deallocationListsToCommit[pointer.SnapshotID]
	if !exists {
		l.ListRoot = &types.Pointer{}
		l.List = list.New(list.Config{
			ListRoot:      l.ListRoot,
			State:         db.config.State,
			NodeAssistant: db.listNodeAssistant,
		})
		db.deallocationListsToCommit[pointer.SnapshotID] = l
	}

	return l.List.Add(
		pointer,
		volatilePool,
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

// GetSpace retrieves space from snapshot.
func GetSpace[K, V comparable](spaceID types.SpaceID, db *DB) (*space.Space[K, V], error) {
	if spaceID >= types.SpaceID(len(db.snapshotInfo.Spaces)) {
		return nil, errors.Errorf("space %d is not defined", spaceID)
	}
	spaceInfo := &db.snapshotInfo.Spaces[spaceID]

	dataNodeAssistant, err := space.NewNodeAssistant[types.DataItem[K, V]](db.config.State)
	if err != nil {
		return nil, err
	}

	return space.New[K, V](space.Config[K, V]{
		SpaceRoot: types.ParentEntry{
			State:   &spaceInfo.State,
			Pointer: &spaceInfo.Pointer,
		},
		State:                db.config.State,
		PointerNodeAssistant: db.pointerNodeAssistant,
		DataNodeAssistant:    dataNodeAssistant,
		MassEntry:            mass.New[space.Entry[K, V]](1000),
		StoreQ:               db.storeQ,
	}), nil
}
