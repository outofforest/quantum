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
	"github.com/outofforest/quantum/wal"
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
	List *list.List
	Root *types.Pointer
}

// New creates new database.
func New(config Config) (*DB, error) {
	snapshotInfoNodeAssistant, err := space.NewDataNodeAssistant[types.SnapshotID, types.SnapshotInfo]()
	if err != nil {
		return nil, err
	}

	snapshotToPointerNodeAssistant, err := space.NewDataNodeAssistant[types.SnapshotID, types.Pointer]()
	if err != nil {
		return nil, err
	}

	queue, queueReader := pipeline.New()

	db := &DB{
		config:                         config,
		txRequestFactory:               pipeline.NewTransactionRequestFactory(),
		singularityNode:                photon.FromPointer[types.SingularityNode](config.State.Node(0)),
		snapshotInfoNodeAssistant:      snapshotInfoNodeAssistant,
		snapshotToPointerNodeAssistant: snapshotToPointerNodeAssistant,
		deallocationListsToCommit:      map[types.SnapshotID]*ListToCommit{},
		queue:                          queue,
		queueReader:                    queueReader,
	}

	// Logical nodes might be deallocated immediately.
	db.snapshots = space.New[types.SnapshotID, types.SnapshotInfo](space.Config[types.SnapshotID, types.SnapshotInfo]{
		SpaceRoot: types.NodeRoot{
			Pointer: &db.singularityNode.SnapshotRoot,
		},
		State:             config.State,
		DataNodeAssistant: snapshotInfoNodeAssistant,
		MassEntry:         mass.New[space.Entry[types.SnapshotID, types.SnapshotInfo]](1000),
		NoSnapshots:       true,
	})

	db.deallocationLists = space.New[types.SnapshotID, types.Pointer](
		space.Config[types.SnapshotID, types.Pointer]{
			SpaceRoot: types.NodeRoot{
				Pointer: &db.snapshotInfo.DeallocationRoot,
			},
			State:             config.State,
			DataNodeAssistant: snapshotToPointerNodeAssistant,
			MassEntry:         mass.New[space.Entry[types.SnapshotID, types.Pointer]](1000),
			NoSnapshots:       true,
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
	deallocationLists *space.Space[types.SnapshotID, types.Pointer]

	snapshotInfoNodeAssistant      *space.DataNodeAssistant[types.SnapshotID, types.SnapshotInfo]
	snapshotToPointerNodeAssistant *space.DataNodeAssistant[types.SnapshotID, types.Pointer]

	deallocationListsToCommit map[types.SnapshotID]*ListToCommit

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
		SyncCh: syncCh,
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
				prepareTxReaders := make([]*pipeline.Reader, 0, 2)
				for range cap(prepareTxReaders) {
					prepareTxReaders = append(prepareTxReaders, pipeline.CloneReader(db.queueReader))
				}
				executeTxReader := pipeline.NewReader(prepareTxReaders...)
				allocateReader := pipeline.NewReader(executeTxReader)
				incrementRevisionReader := pipeline.NewReader(allocateReader)
				hashReaders := make([]*pipeline.Reader, 0, 4)
				for range cap(hashReaders) {
					hashReaders = append(hashReaders, pipeline.NewReader(incrementRevisionReader))
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
				for i, reader := range prepareTxReaders {
					spawn(fmt.Sprintf("prepareTx-%02d", i), parallel.Fail, func(ctx context.Context) error {
						return db.prepareTransactions(ctx, reader, uint64(len(prepareTxReaders)), uint64(i))
					})
				}
				spawn("executeTx", parallel.Fail, func(ctx context.Context) error {
					return db.executeTransactions(ctx, executeTxReader)
				})
				spawn("allocate", parallel.Fail, func(ctx context.Context) error {
					return db.processAllocationRequests(ctx, allocateReader)
				})
				spawn("incrementRevision", parallel.Fail, func(ctx context.Context) error {
					return db.incrementRevisions(ctx, incrementRevisionReader)
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
	walRecorder *wal.Recorder,
	volatilePool *alloc.Pool[types.VolatileAddress],
	persistentPool *alloc.Pool[types.PersistentAddress],
	massSnapshotToPointerEntry *mass.Mass[space.Entry[types.SnapshotID, types.Pointer]],
	massStoreRequest *mass.Mass[pipeline.StoreRequest],
	snapshotHashBuff []byte,
	snapshotHashMatches []uint64,
	deallocationHashBuff []byte,
	deallocationHashMatches []uint64,
) error {
	snapshotInfoValue := db.snapshots.Find(snapshotID, snapshotHashBuff, snapshotHashMatches)
	if !snapshotInfoValue.Exists(snapshotHashBuff, snapshotHashMatches) {
		return errors.Errorf("snapshot %d to delete does not exist", snapshotID)
	}
	snapshotInfo := snapshotInfoValue.Value(snapshotHashBuff, snapshotHashMatches)

	if err := snapshotInfoValue.Delete(tx, walRecorder, snapshotHashBuff, snapshotHashMatches); err != nil {
		return err
	}

	var nextSnapshotInfo *types.SnapshotInfo
	var nextDeallocationListRoot types.NodeRoot
	var nextDeallocationLists *space.Space[types.SnapshotID, types.Pointer]
	if snapshotInfo.NextSnapshotID < db.singularityNode.LastSnapshotID {
		nextSnapshotInfoValue := db.snapshots.Find(snapshotInfo.NextSnapshotID, snapshotHashBuff, snapshotHashMatches)
		if !nextSnapshotInfoValue.Exists(snapshotHashBuff, snapshotHashMatches) {
			return errors.Errorf("next snapshot %d does not exist", snapshotID)
		}
		tmpNextSnapshotInfo := nextSnapshotInfoValue.Value(snapshotHashBuff, snapshotHashMatches)
		nextSnapshotInfo = &tmpNextSnapshotInfo

		nextDeallocationListRoot = types.NodeRoot{
			Pointer: &nextSnapshotInfo.DeallocationRoot,
		}
		nextDeallocationLists = space.New[types.SnapshotID, types.Pointer](
			space.Config[types.SnapshotID, types.Pointer]{
				SpaceRoot:         nextDeallocationListRoot,
				State:             db.config.State,
				DataNodeAssistant: db.snapshotToPointerNodeAssistant,
				MassEntry:         massSnapshotToPointerEntry,
			},
		)
	} else {
		nextSnapshotInfo = &db.snapshotInfo
		nextDeallocationListRoot = types.NodeRoot{
			Pointer: &db.snapshotInfo.DeallocationRoot,
		}
		nextDeallocationLists = db.deallocationLists
	}

	deallocationListsRoot := types.NodeRoot{
		Pointer: &snapshotInfo.DeallocationRoot,
	}
	deallocationLists := space.New[types.SnapshotID, types.Pointer](
		space.Config[types.SnapshotID, types.Pointer]{
			SpaceRoot:         deallocationListsRoot,
			State:             db.config.State,
			DataNodeAssistant: db.snapshotToPointerNodeAssistant,
			MassEntry:         massSnapshotToPointerEntry,
		},
	)

	var sr *pipeline.StoreRequest
	for nextDeallocSnapshot := range nextDeallocationLists.Iterator() {
		if nextDeallocSnapshot.Key > snapshotInfo.PreviousSnapshotID && nextDeallocSnapshot.Key <= snapshotID {
			list.Deallocate(
				nextDeallocSnapshot.Value,
				db.config.State,
				volatilePool,
				persistentPool,
			)

			continue
		}

		deallocationListValue := deallocationLists.Find(nextDeallocSnapshot.Key, deallocationHashBuff,
			deallocationHashMatches)
		listNodeAddress := deallocationListValue.Value(deallocationHashBuff, deallocationHashMatches)
		listNodeAddressP := &listNodeAddress
		newListNodeAddressP := listNodeAddressP
		list := list.New(list.Config{
			Root:  newListNodeAddressP,
			State: db.config.State,
		})

		listRoot, err := list.Attach(&nextDeallocSnapshot.Value, volatilePool)
		if err != nil {
			return err
		}
		if listRoot != nil {
			if sr == nil {
				sr = massStoreRequest.New()
				sr.NoSnapshots = true
			}
			sr.ListStore[sr.ListsToStore] = listRoot
			sr.ListsToStore++

			if sr.ListsToStore == pipeline.StoreCapacity {
				tx.AddStoreRequest(sr)
				sr = nil
			}

			if listRoot != newListNodeAddressP {
				newListNodeAddressP = listRoot
			}
		}
		if newListNodeAddressP != listNodeAddressP {
			if err := deallocationListValue.Set(
				*newListNodeAddressP,
				tx,
				walRecorder,
				volatilePool,
				deallocationHashBuff,
				deallocationHashMatches,
			); err != nil {
				return err
			}
		}
	}
	if sr != nil {
		tx.AddStoreRequest(sr)
		sr = nil
	}

	nextSnapshotInfo.DeallocationRoot = snapshotInfo.DeallocationRoot

	space.Deallocate(
		nextDeallocationListRoot.Pointer,
		volatilePool,
		persistentPool,
		db.config.State,
	)

	nextSnapshotInfo.PreviousSnapshotID = snapshotInfo.PreviousSnapshotID

	if snapshotInfo.NextSnapshotID < db.singularityNode.LastSnapshotID {
		nextSnapshotInfoValue := db.snapshots.Find(snapshotInfo.NextSnapshotID, snapshotHashBuff, snapshotHashMatches)
		if err := nextSnapshotInfoValue.Set(
			*nextSnapshotInfo,
			tx,
			walRecorder,
			volatilePool,
			snapshotHashBuff,
			snapshotHashMatches,
		); err != nil {
			return err
		}
	}

	if snapshotInfo.PreviousSnapshotID > 0 {
		previousSnapshotInfoValue := db.snapshots.Find(snapshotInfo.PreviousSnapshotID, snapshotHashBuff,
			snapshotHashMatches)
		if !previousSnapshotInfoValue.Exists(snapshotHashBuff, snapshotHashMatches) {
			return errors.Errorf("previous snapshot %d does not exist", snapshotID)
		}

		previousSnapshotInfo := previousSnapshotInfoValue.Value(snapshotHashBuff, snapshotHashMatches)
		previousSnapshotInfo.NextSnapshotID = snapshotInfo.NextSnapshotID

		if err := previousSnapshotInfoValue.Set(
			previousSnapshotInfo,
			tx,
			walRecorder,
			volatilePool,
			snapshotHashBuff,
			snapshotHashMatches,
		); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) commit(
	tx *pipeline.TransactionRequest,
	walRecorder *wal.Recorder,
	volatilePool *alloc.Pool[types.VolatileAddress],
	massStoreRequest *mass.Mass[pipeline.StoreRequest],
	snapshotHashBuff []byte,
	snapshotHashMatches []uint64,
	deallocationHashBuff []byte,
	deallocationHashMatches []uint64,
) error {
	//nolint:nestif
	if len(db.deallocationListsToCommit) > 0 {
		lists := make([]types.SnapshotID, 0, len(db.deallocationListsToCommit))
		for snapshotID := range db.deallocationListsToCommit {
			lists = append(lists, snapshotID)
		}
		sort.Slice(lists, func(i, j int) bool { return lists[i] < lists[j] })

		var sr *pipeline.StoreRequest
		for _, snapshotID := range lists {
			l := db.deallocationListsToCommit[snapshotID]
			deallocationListValue := db.deallocationLists.Find(snapshotID, deallocationHashBuff, deallocationHashMatches)
			if deallocationListValue.Exists(deallocationHashBuff, deallocationHashMatches) {
				listRoot, err := l.List.Attach(
					lo.ToPtr(deallocationListValue.Value(deallocationHashBuff, deallocationHashMatches)),
					volatilePool,
				)
				if err != nil {
					return err
				}
				if listRoot != nil {
					if sr == nil {
						sr = massStoreRequest.New()
						sr.NoSnapshots = true
					}

					sr.ListStore[sr.ListsToStore] = listRoot
					sr.ListsToStore++

					if sr.ListsToStore == pipeline.StoreCapacity {
						tx.AddStoreRequest(sr)
						sr = nil
					}

					if listRoot != l.Root {
						l.Root = listRoot
					}
				}
			}
			if err := deallocationListValue.Set(
				*l.Root,
				tx,
				walRecorder,
				volatilePool,
				deallocationHashBuff,
				deallocationHashMatches,
			); err != nil {
				return err
			}
		}
		if sr != nil {
			tx.AddStoreRequest(sr)
		}

		clear(db.deallocationListsToCommit)
	}

	nextSnapshotInfoValue := db.snapshots.Find(db.singularityNode.LastSnapshotID, snapshotHashBuff, snapshotHashMatches)
	if err := nextSnapshotInfoValue.Set(
		db.snapshotInfo,
		tx,
		walRecorder,
		volatilePool,
		snapshotHashBuff,
		snapshotHashMatches,
	); err != nil {
		return err
	}

	sr := massStoreRequest.New()
	sr.Store[0] = db.config.State.SingularityNodeRoot()
	sr.PointersToStore = 1
	tx.AddStoreRequest(sr)

	return nil
}

func (db *DB) prepareTransactions(
	ctx context.Context,
	pipeReader *pipeline.Reader,
	divider uint64,
	mod uint64,
) error {
	s, err := GetSpace[txtypes.Account, txtypes.Amount](spaces.Balances, db)
	if err != nil {
		return err
	}

	hashBuff := s.NewHashBuff()
	hashMatches := s.NewHashMatches()

	for processedCount := uint64(0); ; processedCount++ {
		req, err := pipeReader.Read(ctx)
		if err != nil {
			return err
		}

		if processedCount%divider == mod && req.Transaction != nil {
			if transferTx, ok := req.Transaction.(*transfer.Tx); ok {
				transferTx.Prepare(s, hashBuff, hashMatches)
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

	hashBuff := s.NewHashBuff()
	hashMatches := s.NewHashMatches()
	snapshotHashBuff := db.snapshots.NewHashBuff()
	snapshotHashMatches := db.snapshots.NewHashMatches()
	deallocationHashBuff := db.deallocationLists.NewHashBuff()
	deallocationHashMatches := db.deallocationLists.NewHashMatches()

	massSnapshotToPointerEntry := mass.New[space.Entry[types.SnapshotID, types.Pointer]](1000)
	massStoreRequest := mass.New[pipeline.StoreRequest](1000)

	walRecorder := wal.NewRecorder(db.config.State, volatilePool)

	for processedCount := uint64(0); ; processedCount++ {
		req, err := pipeReader.Read(ctx)
		if err != nil {
			return err
		}

		if req.Transaction != nil {
			switch tx := req.Transaction.(type) {
			case *transfer.Tx:
				if err := tx.Execute(req, walRecorder, volatilePool, hashBuff, hashMatches); err != nil {
					return err
				}
			case *commitTx:
				// Syncing must be finished just before committing because inside commit we store the results
				// of deallocations.
				<-tx.SyncCh
				if err := db.commit(req, walRecorder, volatilePool, massStoreRequest, snapshotHashBuff, snapshotHashMatches,
					deallocationHashBuff, deallocationHashMatches); err != nil {
					req.CommitCh <- err
					return err
				}
				walRecorder.Commit(req)
			case *deleteSnapshotTx:
				if err := db.deleteSnapshot(tx.SnapshotID, req, walRecorder, volatilePool, persistentPool,
					massSnapshotToPointerEntry, massStoreRequest, snapshotHashBuff, snapshotHashMatches,
					deallocationHashBuff, deallocationHashMatches); err != nil {
					return err
				}
			case *genesis.Tx:
				if err := tx.Execute(s, req, walRecorder, volatilePool, hashBuff, hashMatches); err != nil {
					return err
				}
			default:
				return errors.New("unknown transaction type")
			}
		}

		pipeReader.Acknowledge(processedCount+1, req)
	}
}

func (db *DB) allocatePersistentAddress(
	tx *pipeline.TransactionRequest,
	sr *pipeline.StoreRequest,
	walRecorder *wal.Recorder,
	pointer *types.Pointer,
	volatilePool *alloc.Pool[types.VolatileAddress],
	persistentPool *alloc.Pool[types.PersistentAddress],
	listNodesToStore map[types.VolatileAddress]struct{},
) error {
	// 0 address is reserved for the singularity node. We don't do any (de)allocations for this address.
	if pointer.VolatileAddress == 0 {
		return nil
	}

	//nolint:nestif
	if pointer.SnapshotID != db.singularityNode.LastSnapshotID {
		if pointer.PersistentAddress != 0 {
			listRoot, err := db.deallocateNode(
				pointer,
				sr.NoSnapshots,
				volatilePool,
				persistentPool,
			)
			if err != nil {
				return err
			}

			if listRoot != nil {
				if _, exists := listNodesToStore[listRoot.VolatileAddress]; !exists {
					listNodesToStore[listRoot.VolatileAddress] = struct{}{}

					sr.ListStore[sr.ListsToStore] = listRoot
					sr.ListsToStore++
				}
			}

			pointer.PersistentAddress = 0
		}
		pointer.SnapshotID = db.singularityNode.LastSnapshotID

		if walRecorder != nil {
			if _, err := wal.Record(walRecorder, tx, &pointer.SnapshotID); err != nil {
				return err
			}
		}
	}

	if pointer.PersistentAddress == 0 {
		persistentAddress, err := persistentPool.Allocate()
		if err != nil {
			return err
		}
		pointer.PersistentAddress = persistentAddress

		if walRecorder != nil {
			if _, err := wal.Record(walRecorder, tx, &pointer.PersistentAddress); err != nil {
				return err
			}
		}
	}

	return nil
}

func (db *DB) processAllocationRequests(
	ctx context.Context,
	pipeReader *pipeline.Reader,
) error {
	volatilePool := db.config.State.NewVolatilePool()
	persistentPool := db.config.State.NewPersistentPool()
	listNodesToStore := map[types.VolatileAddress]struct{}{}

	walRecorder := wal.NewRecorder(db.config.State, volatilePool)

	for processedCount := uint64(0); ; processedCount++ {
		req, err := pipeReader.Read(ctx)
		if err != nil {
			return err
		}

		for sr := req.StoreRequest; sr != nil; sr = sr.Next {
			clear(listNodesToStore)

			for i := range sr.PointersToStore {
				if err := db.allocatePersistentAddress(req, sr, walRecorder, sr.Store[i].Pointer, volatilePool, persistentPool,
					listNodesToStore); err != nil {
					return err
				}
			}
			for i := range sr.ListsToStore {
				if err := db.allocatePersistentAddress(req, nil, nil, sr.ListStore[i], volatilePool,
					persistentPool, nil); err != nil {
					return err
				}
			}
		}

		// Sync is here in the pipeline because allocation is the last step required before we may proceed
		// with the commit.
		switch req.Type {
		case pipeline.Sync:
			req.SyncCh <- struct{}{}
		case pipeline.Commit:
			walRecorder.Commit(req)
		}

		pipeReader.Acknowledge(processedCount+1, req)
	}
}

func (db *DB) incrementRevisions(
	ctx context.Context,
	pipeReader *pipeline.Reader,
) error {
	var revisionCounter uint32

	for processedCount := uint64(0); ; processedCount++ {
		req, err := pipeReader.Read(ctx)
		if err != nil {
			return err
		}

		for sr := req.StoreRequest; sr != nil; sr = sr.Next {
			if sr.PointersToStore == 0 && sr.ListsToStore == 0 {
				continue
			}

			revisionCounter++
			sr.RequestedRevision = revisionCounter
			for i := range sr.PointersToStore {
				atomic.StoreUint32(&sr.Store[i].Pointer.Revision, revisionCounter)
			}
			for i := range sr.ListsToStore {
				atomic.StoreUint32(&sr.ListStore[i].Revision, revisionCounter)
			}
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
		for r.storeRequest != nil && (r.storeRequest.PointersToStore == 0 || r.storeRequest.NoSnapshots) {
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

// FIXME (wojciech): Fix data races:
// - same parent node where hash is updated must always go to the same goroutine
// - pointer present later in a pipeline must be included in later slot.
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

	walRecorder := wal.NewRecorder(db.config.State, db.config.State.NewVolatilePool())

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

				if atomic.LoadUint32(&root.Pointer.Revision) != req.StoreRequest.RequestedRevision {
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
			if state == types.StatePointer {
				numOfBlocks = space.NumOfBlocksForPointerNode
			}

			for bi := range numOfBlocks {
				matrix[ri][bi] = (*byte)(unsafe.Add(node, bi*types.BlockLength))
			}
			hashes[ri] = &hash[0]
		}

		if nilSlots == len(slots) {
			// FIXME (wojciech): Data race, all hashing goroutines try to add their WAL nodes to the tx at the same time.
			walRecorder.Commit(commitReq.TxRequest)
			r.Acknowledge(commitReq.Count, commitReq.TxRequest, true)
		} else {
			hash.Blake3(matrixP, hashesP)
			for _, h := range hashes {
				if h == zh {
					continue
				}

				// FIXME (wojciech): Blake3 should store the copy of hash.
				if _, err := wal.Record(walRecorder, minReq.TxRequest, h); err != nil {
					return err
				}
			}
			r.Acknowledge(minReq.Count-1, minReq.TxRequest, false)
		}
	}
}

func (db *DB) processStoreRequests(
	ctx context.Context,
	store persistent.Store,
	pipeReader *pipeline.Reader,
) error {
	// var numOfWrites uint

	for processedCount := uint64(0); ; processedCount++ {
		req, err := pipeReader.Read(ctx)
		if err != nil {
			return err
		}

		for wr := req.WALRequest; wr != nil; wr = wr.Next {
			// numOfWrites++
		}

		for sr := req.StoreRequest; sr != nil; sr = sr.Next {
			for i := range sr.ListsToStore {
				pointer := sr.ListStore[i]

				// Volatile address must be copied before verifying revision. Otherwise, the address might be
				// concurrently overwritten by another transaction between revision verification and
				// store write.
				// Persistent address is safe to be used even without atomic, because it is guaranteed that
				// in the same snapshot it is set only once on the first time node is processed by the goroutine
				// allocating persistent addresses,
				volatileAddress := pointer.VolatileAddress

				if atomic.LoadUint32(&pointer.Revision) != sr.RequestedRevision {
					// Pointers are processed from the data node up t the root node. If at any level
					// revision test fails, it doesn't make sense to process parent nodes because revision
					// test will fail there for sure.
					break
				}

				// numOfWrites++

				if err := store.Write(
					pointer.PersistentAddress,
					db.config.State.Bytes(volatileAddress),
				); err != nil {
					return err
				}
			}
		}

		if req.Type == pipeline.Commit {
			// fmt.Println("========== STORE", numOfWrites)
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
	noSnapshots bool,
	volatilePool *alloc.Pool[types.VolatileAddress],
	persistentPool *alloc.Pool[types.PersistentAddress],
) (*types.Pointer, error) {
	if pointer.SnapshotID > db.snapshotInfo.PreviousSnapshotID || noSnapshots {
		persistentPool.Deallocate(pointer.PersistentAddress)

		//nolint:nilnil
		return nil, nil
	}

	l, exists := db.deallocationListsToCommit[pointer.SnapshotID]
	if !exists {
		root := &types.Pointer{}
		l = &ListToCommit{
			Root: root,
			List: list.New(list.Config{
				Root:  root,
				State: db.config.State,
			}),
		}
		db.deallocationListsToCommit[pointer.SnapshotID] = l
	}

	listRoot, err := l.List.Add(
		pointer,
		volatilePool,
	)
	if err != nil {
		return nil, err
	}

	if l.Root != listRoot {
		l.Root = listRoot
	}

	return listRoot, nil
}

func (db *DB) prepareNextSnapshot() error {
	db.snapshotInfo.PreviousSnapshotID = db.singularityNode.LastSnapshotID
	db.singularityNode.LastSnapshotID++
	db.snapshotInfo.NextSnapshotID = db.singularityNode.LastSnapshotID + 1
	db.snapshotInfo.DeallocationRoot = types.Pointer{}

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
	SyncCh <-chan struct{}
}

type deleteSnapshotTx struct {
	SnapshotID types.SnapshotID
}
