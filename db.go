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
	"github.com/outofforest/quantum/checksum"
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
	ListRoot *types.Pointer
}

// New creates new database.
func New(config Config) (*DB, error) {
	pointerNodeAssistant, err := space.NewNodeAssistant[types.Pointer](config.State)
	if err != nil {
		return nil, err
	}

	snapshotInfoNodeAssistant, err := space.NewDataNodeAssistant[types.SnapshotID, types.SnapshotInfo](
		config.State,
	)
	if err != nil {
		return nil, err
	}

	snapshotToNodeNodeAssistant, err := space.NewDataNodeAssistant[types.SnapshotID, types.Pointer](
		config.State,
	)
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
		pointerNodeAssistant:        pointerNodeAssistant,
		snapshotToNodeNodeAssistant: snapshotToNodeNodeAssistant,
		listNodeAssistant:           listNodeAssistant,
		massSnapshotToNodeEntry:     mass.New[space.Entry[types.SnapshotID, types.Pointer]](1000),
		deallocationListsToCommit:   map[types.SnapshotID]ListToCommit{},
		queue:                       queue,
		queueReader:                 queueReader,
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
	txRequestFactory  *pipeline.TransactionRequestFactory
	singularityNode   *types.SingularityNode
	snapshotInfo      types.SnapshotInfo
	snapshots         *space.Space[types.SnapshotID, types.SnapshotInfo]
	deallocationLists *space.Space[types.SnapshotID, types.Pointer]

	pointerNodeAssistant        *space.NodeAssistant[types.Pointer]
	snapshotInfoNodeAssistant   *space.DataNodeAssistant[types.SnapshotID, types.SnapshotInfo]
	snapshotToNodeNodeAssistant *space.DataNodeAssistant[types.SnapshotID, types.Pointer]
	listNodeAssistant           *list.NodeAssistant

	massSnapshotToNodeEntry *mass.Mass[space.Entry[types.SnapshotID, types.Pointer]]

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
				prepareTxQReader := db.queueReader.NewReader()
				executeTxQReader := prepareTxQReader.NewReader()
				allocateQReader := executeTxQReader.NewReader()
				deallocateQReader := allocateQReader.NewReader()
				checksumQReader1 := deallocateQReader.NewReader()
				checksumQReader2 := checksumQReader1.NewReader()
				storeQReaders := make([]*pipeline.Reader, 0, len(db.config.Stores))
				for range cap(storeQReaders) {
					storeQReaders = append(storeQReaders, checksumQReader2.NewReader())
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
				spawn("deallocate", parallel.Fail, func(ctx context.Context) error {
					return db.processDeallocationRequests(ctx, deallocateQReader)
				})
				spawn("checksum1", parallel.Fail, func(ctx context.Context) error {
					return db.updateChecksums(ctx, checksumQReader1, 2, 1)
				})
				spawn("checksum2", parallel.Fail, func(ctx context.Context) error {
					return db.updateChecksums(ctx, checksumQReader2, 0, 0)
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

func (db *DB) deleteSnapshot(
	snapshotID types.SnapshotID,
	tx *pipeline.TransactionRequest,
	volatilePool *alloc.Pool[types.VolatileAddress],
	persistentPool *alloc.Pool[types.PersistentAddress],
	pointerNode *space.Node[types.Pointer],
	listNode *list.Node,
) error {
	snapshotInfoValue := db.snapshots.Find(snapshotID, pointerNode)
	if !snapshotInfoValue.Exists(pointerNode) {
		return errors.Errorf("snapshot %d does not exist", snapshotID)
	}

	if err := snapshotInfoValue.Delete(tx, pointerNode); err != nil {
		return err
	}

	snapshotInfo := snapshotInfoValue.Value(pointerNode)

	var nextSnapshotInfo *types.SnapshotInfo
	var nextDeallocationListRoot *types.Pointer
	var nextDeallocationLists *space.Space[types.SnapshotID, types.Pointer]
	if snapshotInfo.NextSnapshotID < db.singularityNode.LastSnapshotID {
		nextSnapshotInfoValue := db.snapshots.Find(snapshotInfo.NextSnapshotID, pointerNode)
		if !nextSnapshotInfoValue.Exists(pointerNode) {
			return errors.Errorf("snapshot %d does not exist", snapshotID)
		}
		tmpNextSnapshotInfo := nextSnapshotInfoValue.Value(pointerNode)
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

	for nextDeallocSnapshot := range nextDeallocationLists.Iterator(pointerNode) {
		if nextDeallocSnapshot.Key >= startSnapshotID && nextDeallocSnapshot.Key <= snapshotID {
			list.Deallocate(
				nextDeallocSnapshot.Value,
				volatilePool,
				persistentPool,
				db.listNodeAssistant,
				listNode,
			)

			continue
		}

		deallocationListValue := deallocationLists.Find(nextDeallocSnapshot.Key, pointerNode)
		listNodeAddress := deallocationListValue.Value(pointerNode)
		newListNodeAddress := listNodeAddress
		list := list.New(list.Config{
			ListRoot:      &newListNodeAddress,
			State:         db.config.State,
			NodeAssistant: db.listNodeAssistant,
		})

		pointerToStore, err := list.Attach(&nextDeallocSnapshot.Value, volatilePool, listNode)
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
				pointerNode,
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
		pointerNode,
	)

	if snapshotID == db.singularityNode.FirstSnapshotID {
		nextSnapshotInfo.PreviousSnapshotID = snapshotInfo.NextSnapshotID
	} else {
		nextSnapshotInfo.PreviousSnapshotID = snapshotInfo.PreviousSnapshotID
	}

	if snapshotInfo.NextSnapshotID < db.singularityNode.LastSnapshotID {
		nextSnapshotInfoValue := db.snapshots.Find(snapshotInfo.NextSnapshotID, pointerNode)
		if err := nextSnapshotInfoValue.Set(
			*nextSnapshotInfo,
			tx,
			volatilePool,
			pointerNode,
		); err != nil {
			return err
		}
	}

	if snapshotID > db.singularityNode.FirstSnapshotID {
		previousSnapshotInfoValue := db.snapshots.Find(snapshotInfo.PreviousSnapshotID, pointerNode)
		if !previousSnapshotInfoValue.Exists(pointerNode) {
			return errors.Errorf("snapshot %d does not exist", snapshotID)
		}

		previousSnapshotInfo := previousSnapshotInfoValue.Value(pointerNode)
		previousSnapshotInfo.NextSnapshotID = snapshotInfo.NextSnapshotID

		if err := previousSnapshotInfoValue.Set(
			previousSnapshotInfo,
			tx,
			volatilePool,
			pointerNode,
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
	pointerNode *space.Node[types.Pointer],
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
			deallocationListValue := db.deallocationLists.Find(snapshotID, pointerNode)
			if deallocationListValue.Exists(pointerNode) {
				pointerToStore, err := db.deallocationListsToCommit[snapshotID].List.Attach(
					lo.ToPtr(deallocationListValue.Value(pointerNode)),
					volatilePool,
					listNode,
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
				pointerNode,
			); err != nil {
				return err
			}
		}
		if sr.PointersToStore > 0 {
			tx.AddStoreRequest(&sr)
		}

		clear(db.deallocationListsToCommit)
	}

	nextSnapshotInfoValue := db.snapshots.Find(db.singularityNode.LastSnapshotID, pointerNode)
	if err := nextSnapshotInfoValue.Set(
		db.snapshotInfo,
		tx,
		volatilePool,
		pointerNode,
	); err != nil {
		return err
	}

	pointer := db.config.State.SingularityNodePointer(snapshotID)
	tx.AddStoreRequest(&pipeline.StoreRequest{
		Store:           [pipeline.StoreCapacity]*types.Pointer{pointer},
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
	pointerNode := s.NewPointerNode()

	for processedCount := uint64(0); ; processedCount++ {
		req, err := pipeReader.Read(ctx)
		if err != nil {
			return err
		}

		if req.Transaction != nil {
			if transferTx, ok := req.Transaction.(*transfer.Tx); ok {
				transferTx.Prepare(s, pointerNode)
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

	pointerNode := db.pointerNodeAssistant.NewNode()
	listNode := db.listNodeAssistant.NewNode()

	for processedCount := uint64(0); ; processedCount++ {
		req, err := pipeReader.Read(ctx)
		if err != nil {
			return err
		}

		if req.Transaction != nil {
			switch tx := req.Transaction.(type) {
			case *transfer.Tx:
				if err := tx.Execute(req, volatilePool, pointerNode); err != nil {
					return err
				}
			case *commitTx:
				// Syncing must be finished just before committing because inside commit we store the results
				// of deallocations.
				<-tx.SyncCh
				if err := db.commit(tx.SnapshotID, req, volatilePool, pointerNode, listNode); err != nil {
					req.CommitCh <- err
					return err
				}
			case *deleteSnapshotTx:
				if err := db.deleteSnapshot(tx.SnapshotID, req, volatilePool, persistentPool, pointerNode,
					listNode); err != nil {
					return err
				}
			case *genesis.Tx:
				if err := tx.Execute(s, req, volatilePool, pointerNode); err != nil {
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
	massPointer := mass.New[types.Pointer](1000)
	persistentPool := db.config.State.NewPersistentPool()

	for processedCount := uint64(0); ; processedCount++ {
		req, err := pipeReader.Read(ctx)
		if err != nil {
			return err
		}

		for sr := req.StoreRequest; sr != nil; sr = sr.Next {
			if sr.PointersToStore == 0 {
				continue
			}

			// FIXME (wojciech): I believe not all the requests require deallocation.
			sr.Deallocate = massPointer.NewSlice(uint64(sr.PointersToStore))
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

		pipeReader.Acknowledge(processedCount+1, req)
	}
}

func (db *DB) processDeallocationRequests(
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
			if sr.DeallocateVolatileAddress != 0 {
				volatilePool.Deallocate(sr.DeallocateVolatileAddress)
			}
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

		// Sync is here in the pipeline because deallocation is the last step required before we may proceed
		// with the commit.
		if req.Type == pipeline.Sync {
			req.SyncCh <- struct{}{}
		}

		pipeReader.Acknowledge(processedCount+1, req)
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
			if r.txRequest.StoreRequest != nil && !r.txRequest.ChecksumProcessed && (r.divider == 0 ||
				(r.read/16)%r.divider == r.mod) {
				r.txRequest.ChecksumProcessed = true
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

func (db *DB) updateChecksums(
	ctx context.Context,
	pipeReader *pipeline.Reader,
	divider uint64,
	mod uint64,
) error {
	var matrix [16][16]*byte
	matrixP := &matrix[0][0]
	checksums := [16]*[32]byte{{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}}
	checksumsP := (**byte)(unsafe.Pointer(&checksums[0]))

	r := &reader{
		pipeReader: pipeReader,
		divider:    divider,
		mod:        mod,
	}

	for {
		matrix = zeroMatrix
		var slots [16]request

		var commitReq request
		minReq := request{
			Count: math.MaxUint64,
		}

		var nilSlots int
	riLoop:
		for ri := range slots {
			req := slots[ri]

			var volatileAddress types.VolatileAddress
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
					if req.Count < minReq.Count {
						minReq = req
					}
				}

				req.PointerIndex--
				p := req.StoreRequest.Store[req.PointerIndex]

				volatileAddress = p.VolatileAddress

				if atomic.LoadUint64(&p.Revision) != req.StoreRequest.RequestedRevision {
					// Pointers are processed from the data node up t the root node. If at any level
					// revision test fails, it doesn't make sense to process parent nodes because revision
					// test will fail there for sure.
					req.PointerIndex = 0
					continue
				}

				break
			}

			slots[ri] = req
			node := db.config.State.Node(volatileAddress)
			for bi := range 16 {
				matrix[ri][bi] = (*byte)(unsafe.Add(node, bi*64))
			}
		}

		if nilSlots == len(slots) {
			r.Acknowledge(commitReq.Count, commitReq.TxRequest, true)
		} else {
			checksum.Blake3(matrixP, checksumsP)
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
				p := sr.Store[i]

				// Volatile address must be copied before verifying revision. Otherwise, the address might be
				// concurrently overwritten by another transaction between revision verification and
				// store write.
				// Persistent address is safe to be used even without atomic, because it is guaranteed that
				// in the same snapshot it is set only once on the first time node is processed by the goroutine
				// allocating persistent addresses,
				volatileAddress := p.VolatileAddress

				if atomic.LoadUint64(&p.Revision) != sr.RequestedRevision {
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
					p.PersistentAddress,
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

	dataNodeAssistant, err := space.NewDataNodeAssistant[K, V](db.config.State)
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

type commitTx struct {
	SnapshotID types.SnapshotID
	SyncCh     <-chan struct{}
}

type deleteSnapshotTx struct {
	SnapshotID types.SnapshotID
}
