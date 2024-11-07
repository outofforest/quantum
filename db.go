package quantum

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/samber/lo"

	"github.com/outofforest/mass"
	"github.com/outofforest/parallel"
	"github.com/outofforest/photon"
	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/checksum"
	"github.com/outofforest/quantum/list"
	"github.com/outofforest/quantum/persistent"
	"github.com/outofforest/quantum/pipeline"
	"github.com/outofforest/quantum/space"
	"github.com/outofforest/quantum/tx"
	"github.com/outofforest/quantum/types"
)

// Config stores snapshot configuration.
type Config struct {
	State            *alloc.State
	TxRequestFactory *pipeline.TransactionRequestFactory
	Stores           []persistent.Store
}

// SpaceToCommit represents requested space which might require to be committed.
type SpaceToCommit struct {
	HashMod         *uint64
	Root            *types.Pointer
	SpaceInfoValue  *space.Entry[types.SpaceID, types.Pointer]
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
		queue:                       pipeline.New(),
	}

	// Logical nodes might be deallocated immediately.
	db.snapshots = space.New[types.SnapshotID, types.SnapshotInfo](space.Config[types.SnapshotID, types.SnapshotInfo]{
		SpaceRoot:             &db.singularityNode.SnapshotRoot,
		State:                 config.State,
		PointerNodeAssistant:  pointerNodeAssistant,
		DataNodeAssistant:     snapshotInfoNodeAssistant,
		MassEntry:             mass.New[space.Entry[types.SnapshotID, types.SnapshotInfo]](1000),
		ImmediateDeallocation: true,
	})

	db.deallocationLists = space.New[types.SnapshotID, types.Pointer](
		space.Config[types.SnapshotID, types.Pointer]{
			SpaceRoot:             &db.snapshotInfo.DeallocationRoot,
			State:                 config.State,
			PointerNodeAssistant:  pointerNodeAssistant,
			DataNodeAssistant:     snapshotToNodeNodeAssistant,
			MassEntry:             mass.New[space.Entry[types.SnapshotID, types.Pointer]](1000),
			ImmediateDeallocation: true,
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

	queue *pipeline.Pipeline
}

// NewVolatilePool creates new volatile allocation pool.
func (db *DB) NewVolatilePool() *alloc.Pool[types.VolatileAddress] {
	return db.config.State.NewVolatilePool()
}

// NewPersistentPool creates new persistent allocation pool.
func (db *DB) NewPersistentPool() *alloc.Pool[types.PersistentAddress] {
	return db.config.State.NewPersistentPool()
}

// ApplyTransaction adds transaction to the queue.
func (db *DB) ApplyTransaction(tx any) {
	txRequest := db.config.TxRequestFactory.New()
	txRequest.Transaction = tx
	db.queue.Push(txRequest)
}

// ApplyTransactionRequest adds transaction request to the queue.
func (db *DB) ApplyTransactionRequest(txRequest *pipeline.TransactionRequest) {
	db.queue.Push(txRequest)
}

// DeleteSnapshot deletes snapshot.
func (db *DB) DeleteSnapshot(
	snapshotID types.SnapshotID,
	volatilePool *alloc.Pool[types.VolatileAddress],
	persistentPool *alloc.Pool[types.PersistentAddress],
) error {
	tx := db.config.TxRequestFactory.New()

	snapshotInfoValue := db.snapshots.Find(snapshotID, db.pointerNode)
	if !snapshotInfoValue.Exists(db.pointerNode, db.snapshotInfoNode) {
		return errors.Errorf("snapshot %d does not exist", snapshotID)
	}

	if err := snapshotInfoValue.Delete(tx, db.pointerNode, db.snapshotInfoNode); err != nil {
		return err
	}

	snapshotInfo := snapshotInfoValue.Value(db.pointerNode, db.snapshotInfoNode)

	var nextSnapshotInfo *types.SnapshotInfo
	var nextDeallocationListRoot *types.Pointer
	var nextDeallocationLists *space.Space[types.SnapshotID, types.Pointer]
	if snapshotInfo.NextSnapshotID < db.singularityNode.LastSnapshotID {
		nextSnapshotInfoValue := db.snapshots.Find(snapshotInfo.NextSnapshotID, db.pointerNode)
		if !nextSnapshotInfoValue.Exists(db.pointerNode, db.snapshotInfoNode) {
			return errors.Errorf("snapshot %d does not exist", snapshotID)
		}
		tmpNextSnapshotInfo := nextSnapshotInfoValue.Value(db.pointerNode, db.snapshotInfoNode)
		nextSnapshotInfo = &tmpNextSnapshotInfo

		nextDeallocationListRoot = &nextSnapshotInfo.DeallocationRoot
		nextDeallocationLists = space.New[types.SnapshotID, types.Pointer](
			space.Config[types.SnapshotID, types.Pointer]{
				SpaceRoot:            nextDeallocationListRoot,
				State:                db.config.State,
				PointerNodeAssistant: db.pointerNodeAssistant,
				DataNodeAssistant:    db.snapshotToNodeNodeAssistant,
				MassEntry:            db.massSnapshotToNodeEntry,
			},
		)
	} else {
		nextSnapshotInfo = &db.snapshotInfo
		nextDeallocationListRoot = &db.snapshotInfo.DeallocationRoot
		nextDeallocationLists = db.deallocationLists
	}

	deallocationListsRoot := &snapshotInfo.DeallocationRoot
	deallocationLists := space.New[types.SnapshotID, types.Pointer](
		space.Config[types.SnapshotID, types.Pointer]{
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
			list.Deallocate(
				nextDeallocSnapshot.Value,
				volatilePool,
				persistentPool,
				db.listNodeAssistant,
				db.listNode,
			)

			continue
		}

		deallocationListValue := deallocationLists.Find(nextDeallocSnapshot.Key, db.pointerNode)
		listNodeAddress := deallocationListValue.Value(db.pointerNode, db.snapshotToNodeNode)
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
			tx.AddStoreRequest(&pipeline.StoreRequest{
				Store:                 [pipeline.StoreCapacity]*types.Pointer{pointerToStore},
				PointersToStore:       1,
				ImmediateDeallocation: true,
			})
		}
		if newListNodeAddress != listNodeAddress {
			if err := deallocationListValue.Set(
				newListNodeAddress,
				tx,
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
		nextSnapshotInfoValue := db.snapshots.Find(snapshotInfo.NextSnapshotID, db.pointerNode)
		if err := nextSnapshotInfoValue.Set(
			*nextSnapshotInfo,
			tx,
			volatilePool,
			db.pointerNode,
			db.snapshotInfoNode,
		); err != nil {
			return err
		}
	}

	if snapshotID > db.singularityNode.FirstSnapshotID {
		previousSnapshotInfoValue := db.snapshots.Find(snapshotInfo.PreviousSnapshotID, db.pointerNode)
		if !previousSnapshotInfoValue.Exists(db.pointerNode, db.snapshotInfoNode) {
			return errors.Errorf("snapshot %d does not exist", snapshotID)
		}

		previousSnapshotInfo := previousSnapshotInfoValue.Value(db.pointerNode, db.snapshotInfoNode)
		previousSnapshotInfo.NextSnapshotID = snapshotInfo.NextSnapshotID

		if err := previousSnapshotInfoValue.Set(
			previousSnapshotInfo,
			tx,
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

	db.queue.Push(tx)

	return nil
}

// Commit commits current snapshot and returns next one.
func (db *DB) Commit(volatilePool *alloc.Pool[types.VolatileAddress]) error {
	commitTx := db.config.TxRequestFactory.New()
	commitTx.Type = pipeline.Commit

	//nolint:nestif
	if len(db.deallocationListsToCommit) > 0 {
		syncCh := make(chan error, len(db.config.Stores)+1) // 1 is for supervisor
		tx := db.config.TxRequestFactory.New()
		tx.Type = pipeline.Sync
		tx.SyncCh = syncCh
		db.queue.Push(tx)

		for range len(db.config.Stores) {
			if err := <-syncCh; err != nil {
				return err
			}
		}

		lists := make([]types.SnapshotID, 0, len(db.deallocationListsToCommit))
		for snapshotID := range db.deallocationListsToCommit {
			lists = append(lists, snapshotID)
		}
		sort.Slice(lists, func(i, j int) bool { return lists[i] < lists[j] })

		var sr pipeline.StoreRequest
		for _, snapshotID := range lists {
			deallocationListValue := db.deallocationLists.Find(snapshotID, db.pointerNode)
			if deallocationListValue.Exists(db.pointerNode, db.snapshotToNodeNode) {
				pointerToStore, err := db.deallocationListsToCommit[snapshotID].List.Attach(
					lo.ToPtr(deallocationListValue.Value(db.pointerNode, db.snapshotToNodeNode)),
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
				tx,
				volatilePool,
				db.pointerNode,
				db.snapshotToNodeNode,
			); err != nil {
				return err
			}
		}
		if sr.PointersToStore > 0 {
			commitTx.AddStoreRequest(&sr)
		}

		clear(db.deallocationListsToCommit)
	}

	nextSnapshotInfoValue := db.snapshots.Find(db.singularityNode.LastSnapshotID, db.pointerNode)
	if err := nextSnapshotInfoValue.Set(
		db.snapshotInfo,
		commitTx,
		volatilePool,
		db.pointerNode,
		db.snapshotInfoNode,
	); err != nil {
		return err
	}

	pointer := db.config.State.SingularityNodePointer(db.singularityNode.LastSnapshotID)
	syncCh := make(chan error, len(db.config.Stores)+1) // 1 is for supervisor
	commitTx.SyncCh = syncCh
	commitTx.AddStoreRequest(&pipeline.StoreRequest{
		Store:           [pipeline.StoreCapacity]*types.Pointer{pointer},
		PointersToStore: 1,
	})
	db.queue.Push(commitTx)

	for range len(db.config.Stores) {
		if err := <-syncCh; err != nil {
			return err
		}
	}

	db.config.State.Commit()

	return db.prepareNextSnapshot()
}

// Close tells that there will be no more operations done.
func (db *DB) Close() {
	tx := db.config.TxRequestFactory.New()
	tx.Type = pipeline.Close
	db.queue.Push(tx)
}

// Run runs db goroutines.
func (db *DB) Run(ctx context.Context) error {
	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("state", parallel.Fail, db.config.State.Run)
		spawn("pipeline", parallel.Exit, func(ctx context.Context) error {
			defer db.config.State.Close()

			return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
				supervisorQReader := db.queue.NewReader()
				prepareTxQReader := supervisorQReader.NewReader()
				executeTxQReader := prepareTxQReader.NewReader()
				allocateQReader := executeTxQReader.NewReader()
				deallocateQReader := allocateQReader.NewReader()
				checksumQReader1 := deallocateQReader.NewReader()
				checksumQReader2 := checksumQReader1.NewReader()
				checksumQReader3 := checksumQReader2.NewReader()
				storeQReaders := make([]*pipeline.Reader, 0, len(db.config.Stores))
				for range cap(storeQReaders) {
					storeQReaders = append(storeQReaders, checksumQReader3.NewReader())
				}

				spawn("supervisor", parallel.Exit, func(ctx context.Context) error {
					var lastSyncCh chan<- error

					for {
						count, err := supervisorQReader.Count(ctx)
						if err != nil && lastSyncCh != nil {
							lastSyncCh <- err
							lastSyncCh = nil
						}

						for range count {
							req := supervisorQReader.Read()
							if req.SyncCh != nil {
								lastSyncCh = req.SyncCh
								continue
							}
							if req.Type == pipeline.Close {
								return errors.WithStack(ctx.Err())
							}
						}
					}
				})
				spawn("prepareTx", parallel.Fail, func(ctx context.Context) error {
					return db.prepareTransactions(ctx, prepareTxQReader)
				})
				spawn("executeTx", parallel.Fail, func(ctx context.Context) error {
					return db.executeTransactions(ctx, executeTxQReader)
				})
				spawn("allocate", parallel.Fail, func(ctx context.Context) error {
					return db.processAllocationRequests(ctx, allocateQReader)
				})
				spawn("deallocate", parallel.Fail, func(ctx context.Context) error {
					return db.processDeallocationRequests(ctx, deallocateQReader)
				})
				spawn("checksum1", parallel.Fail, func(ctx context.Context) error {
					return db.updateChecksums(ctx, checksumQReader1, 3, 2)
				})
				spawn("checksum2", parallel.Fail, func(ctx context.Context) error {
					return db.updateChecksums(ctx, checksumQReader2, 3, 1)
				})
				spawn("checksum3", parallel.Fail, func(ctx context.Context) error {
					return db.updateChecksums(ctx, checksumQReader3, 0, 0)
				})
				for i, store := range db.config.Stores {
					spawn(fmt.Sprintf("store-%02d", i), parallel.Fail, func(ctx context.Context) error {
						return db.processStoreRequests(ctx, store, storeQReaders[i])
					})
				}
				return nil
			})
		})

		return nil
	})
}

func (db *DB) prepareTransactions(
	ctx context.Context,
	pipeReader *pipeline.Reader,
) error {
	s, err := GetSpace[tx.Account, tx.Balance](tx.BalanceSpaceID, db)
	if err != nil {
		return err
	}
	pointerNode := s.NewPointerNode()

	for {
		count, err := pipeReader.Count(ctx)
		if err != nil {
			return err
		}
		for range count {
			req := pipeReader.Read()
			if req.Transaction == nil {
				continue
			}

			transferTx, ok := req.Transaction.(*tx.Transfer)
			if !ok {
				return errors.New("unknown transaction type")
			}
			transferTx.Prepare(s, pointerNode)
		}
	}
}

func (db *DB) executeTransactions(
	ctx context.Context,
	pipeReader *pipeline.Reader,
) error {
	volatilePool := db.NewVolatilePool()

	s, err := GetSpace[tx.Account, tx.Balance](tx.BalanceSpaceID, db)
	if err != nil {
		return err
	}
	pointerNode := s.NewPointerNode()
	dataNode := s.NewDataNode()

	for {
		count, err := pipeReader.Count(ctx)
		if err != nil {
			return err
		}
		for range count {
			req := pipeReader.Read()
			if req.Transaction == nil {
				continue
			}

			transferTx, ok := req.Transaction.(*tx.Transfer)
			if !ok {
				return errors.New("unknown transaction type")
			}
			if err := transferTx.Execute(req, volatilePool, pointerNode, dataNode); err != nil {
				return err
			}
		}
	}
}

func (db *DB) processAllocationRequests(
	ctx context.Context,
	pipeReader *pipeline.Reader,
) error {
	massPointer := mass.New[types.Pointer](1000)
	persistentPool := db.NewPersistentPool()

	for {
		count, err := pipeReader.Count(ctx)
		if err != nil {
			return err
		}
		for range count {
			req := pipeReader.Read()
			for sr := req.StoreRequest; sr != nil; sr = sr.Next {
				if sr.PointersToStore == 0 {
					continue
				}

				// FIXME (wojciech): I believe not all the requests require deallocation.
				sr.Deallocate = massPointer.NewSlice(sr.PointersToStore)
				var numOfPointersToDeallocate uint
				for i := range sr.PointersToStore {
					p := sr.Store[i]
					if p.SnapshotID != db.singularityNode.LastSnapshotID {
						if p.PersistentAddress != 0 {
							sr.Deallocate[numOfPointersToDeallocate] = *p
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

				sr.Deallocate = sr.Deallocate[:numOfPointersToDeallocate]
			}
		}
	}
}

func (db *DB) processDeallocationRequests(
	ctx context.Context,
	pipeReader *pipeline.Reader,
) error {
	volatilePool := db.NewVolatilePool()
	persistentPool := db.NewPersistentPool()

	listNode := db.listNodeAssistant.NewNode()

	for {
		count, err := pipeReader.Count(ctx)
		if err != nil {
			return err
		}
		for range count {
			req := pipeReader.Read()
			for sr := req.StoreRequest; sr != nil; sr = sr.Next {
				for _, p := range sr.Deallocate {
					pointerToStore, err := db.deallocateNode(
						&p,
						sr.ImmediateDeallocation,
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
}

var (
	zeroBlock  = make([]byte, 64)
	zp         = &zeroBlock[0]
	zeroMatrix = [16][16]*byte{
		{zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp},
		{zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp},
		{zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp},
		{zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp},
		{zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp},
		{zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp},
		{zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp},
		{zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp},
		{zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp},
		{zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp},
		{zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp},
		{zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp},
		{zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp},
		{zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp},
		{zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp},
		{zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp, zp},
	}
)

// FIXME (wojciech): Checksums must be computed in right order: data nodes first, then parents, root at the end.
func (db *DB) updateChecksums(
	ctx context.Context,
	pipeReader *pipeline.Reader,
	divider uint64,
	mod uint64,
) error {
	matrix := [16][16]*byte{}
	matrixP := &matrix[0][0]
	checksums := [16]*[32]byte{{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}}
	checksumsP := (**byte)(unsafe.Pointer(&checksums[0]))

	matrix = zeroMatrix
	var nodesWaiting int
	for {
		if nodesWaiting > 0 {
			checksum.Blake3(matrixP, checksumsP)
			matrix = zeroMatrix
			nodesWaiting = 0
		}

		count, err := pipeReader.Count(ctx)
		if err != nil {
			return err
		}
		for range count {
			req := pipeReader.Read()

			if !req.ChecksumProcessed && (divider == 0 || (req.RequestedRevision/16)%divider == mod) {
				req.ChecksumProcessed = true
				for sr := req.StoreRequest; sr != nil; sr = sr.Next {
					for i := range sr.PointersToStore {
						p := sr.Store[i]
						if atomic.LoadUint64(&p.Revision) != req.RequestedRevision {
							continue
						}
						node := db.config.State.Node(p.VolatileAddress)
						for bi := range 16 {
							matrix[nodesWaiting][bi] = (*byte)(unsafe.Add(node, bi*64))
						}
						nodesWaiting++

						if nodesWaiting == 16 {
							checksum.Blake3(matrixP, checksumsP)
							matrix = zeroMatrix
							nodesWaiting = 0
						}
					}
				}
			}

			if (req.Type == pipeline.Sync || req.Type == pipeline.Commit) && nodesWaiting > 0 {
				checksum.Blake3(matrixP, checksumsP)
				matrix = zeroMatrix
				nodesWaiting = 0
			}
		}
	}
}

func (db *DB) processStoreRequests(
	ctx context.Context,
	store persistent.Store,
	pipeReader *pipeline.Reader,
) error {
	// uniqueNodes := map[types.VolatileAddress]struct{}{}
	// var numOfWrites uint

	for {
		count, err := pipeReader.Count(ctx)
		if err != nil {
			return err
		}
		for range count {
			req := pipeReader.Read()
			if req.Type == pipeline.Sync {
				req.SyncCh <- nil
				continue
			}

			for sr := req.StoreRequest; sr != nil; sr = sr.Next {
				for i := range sr.PointersToStore {
					p := sr.Store[i]
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
	db.snapshotInfo.DeallocationRoot = types.Pointer{}
	db.singularityNode.LastSnapshotID = snapshotID

	return nil
}

// GetSpace retrieves space from snapshot.
func GetSpace[K, V comparable](spaceID types.SpaceID, db *DB) (*space.Space[K, V], error) {
	if spaceID >= types.SpaceID(len(db.snapshotInfo.Spaces)) {
		return nil, errors.Errorf("space %d is not defined", spaceID)
	}

	dataNodeAssistant, err := space.NewNodeAssistant[types.DataItem[K, V]](db.config.State)
	if err != nil {
		return nil, err
	}

	return space.New[K, V](space.Config[K, V]{
		SpaceRoot:            &db.snapshotInfo.Spaces[spaceID],
		State:                db.config.State,
		PointerNodeAssistant: db.pointerNodeAssistant,
		DataNodeAssistant:    dataNodeAssistant,
		MassEntry:            mass.New[space.Entry[K, V]](1000),
	}), nil
}
