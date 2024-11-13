package quantum

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync/atomic"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/samber/lo"

	"github.com/outofforest/mass"
	"github.com/outofforest/parallel"
	"github.com/outofforest/photon"
	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/hash"
	"github.com/outofforest/quantum/list"
	"github.com/outofforest/quantum/persistent"
	"github.com/outofforest/quantum/pipeline"
	"github.com/outofforest/quantum/space"
	"github.com/outofforest/quantum/tx/genesis"
	"github.com/outofforest/quantum/tx/transfer"
	txtypes "github.com/outofforest/quantum/tx/types"
	"github.com/outofforest/quantum/tx/types/spaces"
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
	Root            *types.Pointer
	SpaceInfoValue  *space.Entry[types.SpaceID, types.Pointer]
	OriginalPointer types.Pointer
}

// ListToCommit contains cached deallocation list.
type ListToCommit struct {
	List     *list.List
	ListRoot types.NodeRoot
}

// New creates new database.
func New(config Config) (*DB, error) {
	snapshotInfoNodeAssistant, err := space.NewDataNodeAssistant[types.SnapshotID, types.SnapshotInfo]()
	if err != nil {
		return nil, err
	}

	snapshotToNodeNodeAssistant, err := space.NewDataNodeAssistant[types.SnapshotID, types.Root]()
	if err != nil {
		return nil, err
	}

	listNodeAssistant, err := list.NewNodeAssistant(config.State)
	if err != nil {
		return nil, err
	}

	queue, queueReader := pipeline.New()

	db := &DB{
		config:                      config,
		txRequestFactory:            pipeline.NewTransactionRequestFactory(),
		singularityNode:             photon.FromPointer[types.SingularityNode](config.State.Node(0)),
		snapshotInfoNodeAssistant:   snapshotInfoNodeAssistant,
		snapshotToNodeNodeAssistant: snapshotToNodeNodeAssistant,
		listNodeAssistant:           listNodeAssistant,
		massSnapshotToNodeEntry:     mass.New[space.Entry[types.SnapshotID, types.Root]](1000),
		deallocationListsToCommit:   map[types.SnapshotID]ListToCommit{},
		queue:                       queue,
		queueReader:                 queueReader,
	}

	// Logical nodes might be deallocated immediately.
	db.snapshots = space.New[types.SnapshotID, types.SnapshotInfo](space.Config[types.SnapshotID, types.SnapshotInfo]{
		SpaceRoot: types.NodeRoot{
			Hash:    &db.singularityNode.SnapshotRoot.Hash,
			Pointer: &db.singularityNode.SnapshotRoot.Pointer,
		},
		State:                 config.State,
		DataNodeAssistant:     snapshotInfoNodeAssistant,
		MassEntry:             mass.New[space.Entry[types.SnapshotID, types.SnapshotInfo]](1000),
		ImmediateDeallocation: true,
	})

	db.deallocationLists = space.New[types.SnapshotID, types.Root](
		space.Config[types.SnapshotID, types.Root]{
			SpaceRoot: types.NodeRoot{
				Hash:    &db.snapshotInfo.DeallocationRoot.Hash,
				Pointer: &db.snapshotInfo.DeallocationRoot.Pointer,
			},
			State:                 config.State,
			DataNodeAssistant:     snapshotToNodeNodeAssistant,
			MassEntry:             mass.New[space.Entry[types.SnapshotID, types.Root]](1000),
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
	txRequestFactory  *pipeline.TransactionRequestFactory
	singularityNode   *types.SingularityNode
	snapshotInfo      types.SnapshotInfo
	snapshots         *space.Space[types.SnapshotID, types.SnapshotInfo]
	deallocationLists *space.Space[types.SnapshotID, types.Root]

	snapshotInfoNodeAssistant   *space.DataNodeAssistant[types.SnapshotID, types.SnapshotInfo]
	snapshotToNodeNodeAssistant *space.DataNodeAssistant[types.SnapshotID, types.Root]
	listNodeAssistant           *list.NodeAssistant

	massSnapshotToNodeEntry *mass.Mass[space.Entry[types.SnapshotID, types.Root]]

	deallocationListsToCommit map[types.SnapshotID]ListToCommit

	queueReader *pipeline.Reader
	queue       *pipeline.Pipeline
}

// ApplyTransaction adds transaction to the queue.
func (db *DB) ApplyTransaction(tx any) {
	txRequest := db.txRequestFactory.New()
	txRequest.Transaction = tx
	db.queue.Push(txRequest)
}

// DeleteSnapshot deletes snapshot.
func (db *DB) DeleteSnapshot(snapshotID types.SnapshotID) {
	tx := db.txRequestFactory.New()
	tx.Transaction = &deleteSnapshotTx{
		SnapshotID: snapshotID,
	}
	db.queue.Push(tx)
}

// Commit commits current snapshot and returns next one.
func (db *DB) Commit() error {
	syncCh := make(chan struct{}, 2) //  1 is for deallocation, 1 is for supervisor
	tx := db.txRequestFactory.New()
	tx.Type = pipeline.Sync
	tx.SyncCh = syncCh
	db.queue.Push(tx)

	commitCh := make(chan error, len(db.config.Stores)+1) // 1 is for supervisor
	tx = db.txRequestFactory.New()
	tx.Type = pipeline.Commit
	tx.CommitCh = commitCh
	tx.Transaction = &commitTx{
		SnapshotID: db.singularityNode.LastSnapshotID,
		SyncCh:     syncCh,
	}
	db.queue.Push(tx)

	for range len(db.config.Stores) {
		if err := <-commitCh; err != nil {
			return err
		}
	}

	db.config.State.Commit()
	return db.prepareNextSnapshot()
}

// Close tells that there will be no more operations done.
func (db *DB) Close() {
	tx := db.txRequestFactory.New()
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
				prepareTxQReader := pipeline.NewReader(db.queueReader)
				executeTxQReader := pipeline.NewReader(prepareTxQReader)
				allocateQReader := pipeline.NewReader(executeTxQReader)
				hashReaders := make([]*pipeline.Reader, 0, 3)
				for range cap(hashReaders) {
					hashReaders = append(hashReaders, pipeline.NewReader(executeTxQReader))
				}
				storeQReaders := make([]*pipeline.Reader, 0, len(db.config.Stores))
				for range cap(storeQReaders) {
					storeQReaders = append(storeQReaders, pipeline.NewReader(hashReaders...))
				}

				spawn("supervisor", parallel.Exit, func(ctx context.Context) error {
					var lastSyncCh chan<- struct{}
					var lastCommitCh chan<- error
					var processedCount uint64

					for {
						req, err := db.queueReader.Read(ctx)
						if err != nil {
							if lastSyncCh != nil {
								lastSyncCh <- struct{}{}
								lastSyncCh = nil
							}
							if lastCommitCh != nil {
								lastCommitCh <- err
								lastCommitCh = nil
							}
						}

						if req != nil {
							processedCount++
							db.queueReader.Acknowledge(processedCount, req)

							if req.Type == pipeline.Sync {
								lastSyncCh = req.SyncCh
								continue
							}
							if req.Type == pipeline.Commit {
								lastCommitCh = req.CommitCh
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
				for i, reader := range hashReaders {
					spawn(fmt.Sprintf("hash-%02d", i), parallel.Fail, func(ctx context.Context) error {
						return db.updateHashes(ctx, reader, uint64(len(hashReaders)), uint64(i))
					})
				}
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

func (db *DB) deleteSnapshot(
	snapshotID types.SnapshotID,
	tx *pipeline.TransactionRequest,
	volatilePool *alloc.Pool[types.VolatileAddress],
	persistentPool *alloc.Pool[types.PersistentAddress],
	listNode *list.Node,
) error {
	snapshotInfoValue := db.snapshots.Find(snapshotID)
	if !snapshotInfoValue.Exists() {
		return errors.Errorf("snapshot %d does not exist", snapshotID)
	}

	if err := snapshotInfoValue.Delete(tx); err != nil {
		return err
	}

	snapshotInfo := snapshotInfoValue.Value()

	var nextSnapshotInfo *types.SnapshotInfo
	var nextDeallocationListRoot types.NodeRoot
	var nextDeallocationLists *space.Space[types.SnapshotID, types.Root]
	if snapshotInfo.NextSnapshotID < db.singularityNode.LastSnapshotID {
		nextSnapshotInfoValue := db.snapshots.Find(snapshotInfo.NextSnapshotID)
		if !nextSnapshotInfoValue.Exists() {
			return errors.Errorf("snapshot %d does not exist", snapshotID)
		}
		tmpNextSnapshotInfo := nextSnapshotInfoValue.Value()
		nextSnapshotInfo = &tmpNextSnapshotInfo

		nextDeallocationListRoot = types.NodeRoot{
			Hash:    &nextSnapshotInfo.DeallocationRoot.Hash,
			Pointer: &nextSnapshotInfo.DeallocationRoot.Pointer,
		}
		nextDeallocationLists = space.New[types.SnapshotID, types.Root](
			space.Config[types.SnapshotID, types.Root]{
				SpaceRoot:         nextDeallocationListRoot,
				State:             db.config.State,
				DataNodeAssistant: db.snapshotToNodeNodeAssistant,
				MassEntry:         db.massSnapshotToNodeEntry,
			},
		)
	} else {
		nextSnapshotInfo = &db.snapshotInfo
		nextDeallocationListRoot = types.NodeRoot{
			Hash:    &db.snapshotInfo.DeallocationRoot.Hash,
			Pointer: &db.snapshotInfo.DeallocationRoot.Pointer,
		}
		nextDeallocationLists = db.deallocationLists
	}

	deallocationListsRoot := types.NodeRoot{
		Hash:    &snapshotInfo.DeallocationRoot.Hash,
		Pointer: &snapshotInfo.DeallocationRoot.Pointer,
	}
	deallocationLists := space.New[types.SnapshotID, types.Root](
		space.Config[types.SnapshotID, types.Root]{
			SpaceRoot:         deallocationListsRoot,
			State:             db.config.State,
			DataNodeAssistant: db.snapshotToNodeNodeAssistant,
			MassEntry:         db.massSnapshotToNodeEntry,
		},
	)

	var startSnapshotID types.SnapshotID
	if snapshotID != db.singularityNode.FirstSnapshotID {
		startSnapshotID = snapshotInfo.PreviousSnapshotID + 1
	}

	for nextDeallocSnapshot := range nextDeallocationLists.Iterator() {
		if nextDeallocSnapshot.Key >= startSnapshotID && nextDeallocSnapshot.Key <= snapshotID {
			list.Deallocate(
				nextDeallocSnapshot.Value.Pointer,
				volatilePool,
				persistentPool,
				db.listNodeAssistant,
				listNode,
			)

			continue
		}

		deallocationListValue := deallocationLists.Find(nextDeallocSnapshot.Key)
		listNodeAddress := deallocationListValue.Value()
		newListNodeAddress := listNodeAddress
		list := list.New(list.Config{
			ListRoot: types.NodeRoot{
				Hash:    &newListNodeAddress.Hash,
				Pointer: &newListNodeAddress.Pointer,
			},
			State:         db.config.State,
			NodeAssistant: db.listNodeAssistant,
		})

		nodeRootToStore, err := list.Attach(&nextDeallocSnapshot.Value.Pointer, volatilePool, listNode)
		if err != nil {
			return err
		}
		if nodeRootToStore.Pointer != nil {
			tx.AddStoreRequest(&pipeline.StoreRequest{
				Store:                 [pipeline.StoreCapacity]types.NodeRoot{nodeRootToStore},
				PointersToStore:       1,
				ImmediateDeallocation: true,
			})
		}
		if newListNodeAddress.Pointer != listNodeAddress.Pointer {
			if err := deallocationListValue.Set(
				newListNodeAddress,
				tx,
				volatilePool,
			); err != nil {
				return err
			}
		}
	}

	nextSnapshotInfo.DeallocationRoot = snapshotInfo.DeallocationRoot

	space.Deallocate(
		nextDeallocationListRoot.Pointer,
		volatilePool,
		persistentPool,
		db.config.State,
	)

	if snapshotID == db.singularityNode.FirstSnapshotID {
		nextSnapshotInfo.PreviousSnapshotID = snapshotInfo.NextSnapshotID
	} else {
		nextSnapshotInfo.PreviousSnapshotID = snapshotInfo.PreviousSnapshotID
	}

	if snapshotInfo.NextSnapshotID < db.singularityNode.LastSnapshotID {
		nextSnapshotInfoValue := db.snapshots.Find(snapshotInfo.NextSnapshotID)
		if err := nextSnapshotInfoValue.Set(
			*nextSnapshotInfo,
			tx,
			volatilePool,
		); err != nil {
			return err
		}
	}

	if snapshotID > db.singularityNode.FirstSnapshotID {
		previousSnapshotInfoValue := db.snapshots.Find(snapshotInfo.PreviousSnapshotID)
		if !previousSnapshotInfoValue.Exists() {
			return errors.Errorf("snapshot %d does not exist", snapshotID)
		}

		previousSnapshotInfo := previousSnapshotInfoValue.Value()
		previousSnapshotInfo.NextSnapshotID = snapshotInfo.NextSnapshotID

		if err := previousSnapshotInfoValue.Set(
			previousSnapshotInfo,
			tx,
			volatilePool,
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

func (db *DB) commit(
	snapshotID types.SnapshotID,
	tx *pipeline.TransactionRequest,
	volatilePool *alloc.Pool[types.VolatileAddress],
	listNode *list.Node,
) error {
	//nolint:nestif
	if len(db.deallocationListsToCommit) > 0 {
		lists := make([]types.SnapshotID, 0, len(db.deallocationListsToCommit))
		for snapshotID := range db.deallocationListsToCommit {
			lists = append(lists, snapshotID)
		}
		sort.Slice(lists, func(i, j int) bool { return lists[i] < lists[j] })

		var sr pipeline.StoreRequest
		for _, snapshotID := range lists {
			deallocationListValue := db.deallocationLists.Find(snapshotID)
			if deallocationListValue.Exists() {
				nodeRootToStore, err := db.deallocationListsToCommit[snapshotID].List.Attach(
					lo.ToPtr(deallocationListValue.Value().Pointer),
					volatilePool,
					listNode,
				)
				if err != nil {
					return err
				}
				if nodeRootToStore.Pointer != nil {
					sr.Store[sr.PointersToStore] = nodeRootToStore
					sr.PointersToStore++
				}
			}
			if err := deallocationListValue.Set(
				types.Root{
					Hash:    *db.deallocationListsToCommit[snapshotID].ListRoot.Hash,
					Pointer: *db.deallocationListsToCommit[snapshotID].ListRoot.Pointer,
				},
				tx,
				volatilePool,
			); err != nil {
				return err
			}
		}
		if sr.PointersToStore > 0 {
			tx.AddStoreRequest(&sr)
		}

		clear(db.deallocationListsToCommit)
	}

	nextSnapshotInfoValue := db.snapshots.Find(db.singularityNode.LastSnapshotID)
	if err := nextSnapshotInfoValue.Set(
		db.snapshotInfo,
		tx,
		volatilePool,
	); err != nil {
		return err
	}

	tx.AddStoreRequest(&pipeline.StoreRequest{
		Store:           [pipeline.StoreCapacity]types.NodeRoot{db.config.State.SingularityNodeRoot(snapshotID)},
		PointersToStore: 1,
	})

	return nil
}

func (db *DB) prepareTransactions(
	ctx context.Context,
	pipeReader *pipeline.Reader,
) error {
	s, err := GetSpace[txtypes.Account, txtypes.Amount](spaces.Balances, db)
	if err != nil {
		return err
	}

	for processedCount := uint64(0); ; processedCount++ {
		req, err := pipeReader.Read(ctx)
		if err != nil {
			return err
		}

		if req.Transaction != nil {
			if transferTx, ok := req.Transaction.(*transfer.Tx); ok {
				transferTx.Prepare(s)
			}
		}

		pipeReader.Acknowledge(processedCount+1, req)
	}
}

func (db *DB) executeTransactions(
	ctx context.Context,
	pipeReader *pipeline.Reader,
) error {
	volatilePool := db.config.State.NewVolatilePool()
	persistentPool := db.config.State.NewPersistentPool()

	s, err := GetSpace[txtypes.Account, txtypes.Amount](spaces.Balances, db)
	if err != nil {
		return err
	}

	listNode := db.listNodeAssistant.NewNode()

	for processedCount := uint64(0); ; processedCount++ {
		req, err := pipeReader.Read(ctx)
		if err != nil {
			return err
		}

		if req.Transaction != nil {
			switch tx := req.Transaction.(type) {
			case *transfer.Tx:
				if err := tx.Execute(req, volatilePool); err != nil {
					return err
				}
			case *commitTx:
				// Syncing must be finished just before committing because inside commit we store the results
				// of deallocations.
				<-tx.SyncCh
				if err := db.commit(tx.SnapshotID, req, volatilePool, listNode); err != nil {
					req.CommitCh <- err
					return err
				}
			case *deleteSnapshotTx:
				if err := db.deleteSnapshot(tx.SnapshotID, req, volatilePool, persistentPool, listNode); err != nil {
					return err
				}
			case *genesis.Tx:
				if err := tx.Execute(s, req, volatilePool); err != nil {
					return err
				}
			default:
				return errors.New("unknown transaction type")
			}
		}

		pipeReader.Acknowledge(processedCount+1, req)
	}
}

func (db *DB) processAllocationRequests(
	ctx context.Context,
	pipeReader *pipeline.Reader,
) error {
	volatilePool := db.config.State.NewVolatilePool()
	persistentPool := db.config.State.NewPersistentPool()

	listNode := db.listNodeAssistant.NewNode()

	for processedCount := uint64(0); ; processedCount++ {
		req, err := pipeReader.Read(ctx)
		if err != nil {
			return err
		}

		for sr := req.StoreRequest; sr != nil; sr = sr.Next {
			for i := range sr.PointersToStore {
				root := sr.Store[i]

				//nolint:nestif
				if root.Pointer.SnapshotID != db.singularityNode.LastSnapshotID {
					if root.Pointer.PersistentAddress != 0 {
						nodeRootToStore, err := db.deallocateNode(
							root.Pointer,
							sr.ImmediateDeallocation,
							volatilePool,
							persistentPool,
							listNode,
						)
						if err != nil {
							return err
						}

						//nolint:staticcheck
						if nodeRootToStore.Pointer != nil {
							// FIXME (wojciech): This must be handled somehow
						}
					}
					root.Pointer.SnapshotID = db.singularityNode.LastSnapshotID

					var err error
					root.Pointer.PersistentAddress, err = persistentPool.Allocate()
					if err != nil {
						return err
					}
				}
			}
			if sr.DeallocateVolatileAddress != 0 {
				volatilePool.Deallocate(sr.DeallocateVolatileAddress)
			}
		}

		// Sync is here in the pipeline because allocation is the last step required before we may proceed
		// with the commit.
		if req.Type == pipeline.Sync {
			req.SyncCh <- struct{}{}
		}

		pipeReader.Acknowledge(processedCount+1, req)
	}
}

var (
	zeroHash   [types.HashLength]byte
	zh         = &zeroHash[0]
	zeroBlock  [types.BlockLength]byte
	zb         = &zeroBlock[0]
	zeroMatrix = [16][types.NumOfBlocks]*byte{
		{zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb}, //nolint:lll
		{zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb}, //nolint:lll
		{zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb}, //nolint:lll
		{zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb}, //nolint:lll
		{zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb}, //nolint:lll
		{zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb}, //nolint:lll
		{zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb}, //nolint:lll
		{zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb}, //nolint:lll
		{zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb}, //nolint:lll
		{zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb}, //nolint:lll
		{zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb}, //nolint:lll
		{zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb}, //nolint:lll
		{zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb}, //nolint:lll
		{zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb}, //nolint:lll
		{zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb}, //nolint:lll
		{zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb}, //nolint:lll
	}
	zeroHashes = [16]*byte{zh, zh, zh, zh, zh, zh, zh, zh, zh, zh, zh, zh, zh, zh, zh, zh}
)

type request struct {
	TxRequest    *pipeline.TransactionRequest
	Count        uint64
	StoreRequest *pipeline.StoreRequest
	PointerIndex int8
}

type reader struct {
	pipeReader *pipeline.Reader
	divider    uint64
	mod        uint64
	read       uint64

	txRequest    *pipeline.TransactionRequest
	storeRequest *pipeline.StoreRequest
	commitMode   bool
}

func (r *reader) Read(ctx context.Context) (request, error) {
	for {
		for r.storeRequest == nil && !r.commitMode {
			var err error
			r.txRequest, err = r.pipeReader.Read(ctx)
			if err != nil {
				return request{}, err
			}

			r.commitMode = r.txRequest.Type == pipeline.Commit
			if r.txRequest.StoreRequest != nil && r.read%r.divider == r.mod {
				r.storeRequest = r.txRequest.StoreRequest
			}
			r.read++
		}
		for r.storeRequest != nil && r.storeRequest.PointersToStore == 0 {
			r.storeRequest = r.storeRequest.Next
		}

		if r.storeRequest == nil && !r.commitMode {
			continue
		}

		sr := r.storeRequest
		req := request{
			StoreRequest: sr,
			TxRequest:    r.txRequest,
			Count:        r.read,
		}

		if sr != nil {
			r.storeRequest = sr.Next
			req.PointerIndex = sr.PointersToStore
		}

		return req, nil
	}
}

func (r *reader) Acknowledge(count uint64, req *pipeline.TransactionRequest, commit bool) {
	if commit {
		r.commitMode = false
	}
	r.pipeReader.Acknowledge(count, req)
}

func (db *DB) updateHashes(
	ctx context.Context,
	pipeReader *pipeline.Reader,
	divider uint64,
	mod uint64,
) error {
	var matrix [16][types.NumOfBlocks]*byte
	matrixP := &matrix[0][0]
	var hashes [16]*byte
	hashesP := &hashes[0]

	r := &reader{
		pipeReader: pipeReader,
		divider:    divider,
		mod:        mod,
	}

	var slots [16]request

	for {
		matrix = zeroMatrix
		hashes = zeroHashes

		var commitReq request
		minReq := request{
			Count: math.MaxUint64,
		}

		var nilSlots int
	riLoop:
		for ri := range slots {
			req := slots[ri]

			var state types.State
			var volatileAddress types.VolatileAddress
			var hash *types.Hash
			for {
				if req.PointerIndex == 0 {
					var err error
					req, err = r.Read(ctx)
					if err != nil {
						return err
					}
					if req.TxRequest.Type == pipeline.Commit {
						commitReq = req
					}
					if req.StoreRequest == nil {
						nilSlots++
						slots[ri] = req
						continue riLoop
					}
				}

				req.PointerIndex--
				root := req.StoreRequest.Store[req.PointerIndex]

				volatileAddress = root.Pointer.VolatileAddress

				if atomic.LoadUint64(&root.Pointer.Revision) != req.StoreRequest.RequestedRevision {
					// Pointers are processed from the data node up t the root node. If at any level
					// revision test fails, it doesn't make sense to process parent nodes because revision
					// test will fail there for sure.
					req.PointerIndex = 0
					continue
				}

				if req.Count < minReq.Count {
					minReq = req
				}

				state = root.Pointer.State
				hash = root.Hash

				break
			}

			slots[ri] = req
			node := db.config.State.Node(volatileAddress)

			numOfBlocks := types.NumOfBlocks
			if state == types.StatePointer || state == types.StatePointerWithHashMod {
				numOfBlocks = space.NumOfBlocksForPointerNode
			}

			for bi := range numOfBlocks {
				matrix[ri][bi] = (*byte)(unsafe.Add(node, bi*types.BlockLength))
			}
			hashes[ri] = &hash[0]
		}

		if nilSlots == len(slots) {
			r.Acknowledge(commitReq.Count, commitReq.TxRequest, true)
		} else {
			hash.Blake3(matrixP, hashesP)
			r.Acknowledge(minReq.Count-1, minReq.TxRequest, false)
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

	for processedCount := uint64(0); ; processedCount++ {
		req, err := pipeReader.Read(ctx)
		if err != nil {
			return err
		}

		for sr := req.StoreRequest; sr != nil; sr = sr.Next {
			// We process starting from the data node, which is the last one.
			for i := sr.PointersToStore - 1; i >= 0; i-- {
				root := sr.Store[i]

				// Volatile address must be copied before verifying revision. Otherwise, the address might be
				// concurrently overwritten by another transaction between revision verification and
				// store write.
				// Persistent address is safe to be used even without atomic, because it is guaranteed that
				// in the same snapshot it is set only once on the first time node is processed by the goroutine
				// allocating persistent addresses,
				volatileAddress := root.Pointer.VolatileAddress

				if atomic.LoadUint64(&root.Pointer.Revision) != sr.RequestedRevision {
					// Pointers are processed from the data node up t the root node. If at any level
					// revision test fails, it doesn't make sense to process parent nodes because revision
					// test will fail there for sure.
					break
				}

				// uniqueNodes[p.VolatileAddress] = struct{}{}
				// numOfWrites++

				// https://github.com/zeebo/blake3
				// p.Checksum = blake3.Sum256(db.config.State.Bytes(p.VolatileAddress))

				if err := store.Write(
					root.Pointer.PersistentAddress,
					db.config.State.Bytes(volatileAddress),
				); err != nil {
					return err
				}
			}
		}

		if req.Type == pipeline.Commit {
			// fmt.Println("========== STORE")
			// fmt.Println(len(uniqueNodes))
			// fmt.Println(numOfWrites)
			// fmt.Println(float64(numOfWrites) / float64(len(uniqueNodes)))
			// clear(uniqueNodes)
			// numOfWrites = 0

			err := store.Sync()
			req.CommitCh <- err
			if err != nil {
				return err
			}
		}

		pipeReader.Acknowledge(processedCount+1, req)
	}
}

func (db *DB) deallocateNode(
	pointer *types.Pointer,
	immediateDeallocation bool,
	volatilePool *alloc.Pool[types.VolatileAddress],
	persistentPool *alloc.Pool[types.PersistentAddress],
	node *list.Node,
) (types.NodeRoot, error) {
	if db.snapshotInfo.PreviousSnapshotID == db.singularityNode.LastSnapshotID ||
		pointer.SnapshotID > db.snapshotInfo.PreviousSnapshotID || immediateDeallocation {
		volatilePool.Deallocate(pointer.VolatileAddress)
		persistentPool.Deallocate(pointer.PersistentAddress)

		return types.NodeRoot{}, nil
	}

	l, exists := db.deallocationListsToCommit[pointer.SnapshotID]
	if !exists {
		l.ListRoot = types.NodeRoot{
			Hash:    &types.Hash{},
			Pointer: &types.Pointer{},
		}
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
	if db.singularityNode.SnapshotRoot.Pointer.State != types.StateFree {
		snapshotID = db.singularityNode.LastSnapshotID + 1
	}

	db.snapshotInfo.PreviousSnapshotID = db.singularityNode.LastSnapshotID
	db.snapshotInfo.NextSnapshotID = snapshotID + 1
	db.snapshotInfo.DeallocationRoot = types.Root{}
	db.singularityNode.LastSnapshotID = snapshotID

	return nil
}

// GetSpace retrieves space from snapshot.
func GetSpace[K, V comparable](spaceID types.SpaceID, db *DB) (*space.Space[K, V], error) {
	if spaceID >= types.SpaceID(len(db.snapshotInfo.Spaces)) {
		return nil, errors.Errorf("space %d is not defined", spaceID)
	}

	dataNodeAssistant, err := space.NewDataNodeAssistant[K, V]()
	if err != nil {
		return nil, err
	}

	return space.New[K, V](space.Config[K, V]{
		SpaceRoot: types.NodeRoot{
			Hash:    &db.snapshotInfo.Spaces[spaceID].Hash,
			Pointer: &db.snapshotInfo.Spaces[spaceID].Pointer,
		},
		State:             db.config.State,
		DataNodeAssistant: dataNodeAssistant,
		MassEntry:         mass.New[space.Entry[K, V]](1000),
	}), nil
}

type commitTx struct {
	SnapshotID types.SnapshotID
	SyncCh     <-chan struct{}
}

type deleteSnapshotTx struct {
	SnapshotID types.SnapshotID
}
