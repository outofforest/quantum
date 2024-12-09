package quantum

import (
	"context"
	"fmt"
	"math"
	"sort"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/samber/lo"

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
	State *alloc.State
	Store *persistent.Store
}

// SpaceToCommit represents requested space which might require to be committed.
type SpaceToCommit struct {
	HashMod         *uint64
	Root            *types.Pointer
	SpaceInfoValue  *space.Entry[types.SpaceID, types.Pointer]
	OriginalPointer types.Pointer
}

// FIXME (wojciech): Test if this form of storing deallocation list causes some unacceptable overheads when
// there are many snapshots.
type deallocationKey struct {
	ListSnapshotID types.SnapshotID
	SnapshotID     types.SnapshotID
}

// New creates new database.
func New(config Config) (*DB, error) {
	snapshotInfoNodeAssistant, err := space.NewDataNodeAssistant[types.SnapshotID, types.SnapshotInfo]()
	if err != nil {
		return nil, err
	}

	deallocationNodeAssistant, err := space.NewDataNodeAssistant[deallocationKey, types.PersistentAddress]()
	if err != nil {
		return nil, err
	}

	queue, queueReader := pipeline.New()

	db := &DB{
		config:                    config,
		txRequestFactory:          pipeline.NewTransactionRequestFactory(),
		singularityNode:           photon.FromPointer[types.SingularityNode](config.State.Node(0)),
		snapshotInfoNodeAssistant: snapshotInfoNodeAssistant,
		deallocationNodeAssistant: deallocationNodeAssistant,
		deallocationListsToCommit: map[types.SnapshotID]*list.Pointer{},
		queue:                     queue,
		queueReader:               queueReader,
	}

	// Logical nodes might be deallocated immediately.
	db.snapshots = space.New[types.SnapshotID, types.SnapshotInfo](space.Config[types.SnapshotID, types.SnapshotInfo]{
		SpaceRoot: types.NodeRoot{
			Pointer: &db.singularityNode.SnapshotRoot,
		},
		State:             config.State,
		DataNodeAssistant: snapshotInfoNodeAssistant,
		DeletionCounter:   lo.ToPtr[uint64](0),
		NoSnapshots:       true,
	})

	db.deallocationLists = space.New[deallocationKey, types.PersistentAddress](
		space.Config[deallocationKey, types.PersistentAddress]{
			SpaceRoot: types.NodeRoot{
				Pointer: &db.snapshotInfo.DeallocationRoot,
			},
			State:             config.State,
			DataNodeAssistant: deallocationNodeAssistant,
			DeletionCounter:   lo.ToPtr[uint64](0),
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
	deallocationLists *space.Space[deallocationKey, types.PersistentAddress]

	snapshotInfoNodeAssistant *space.DataNodeAssistant[types.SnapshotID, types.SnapshotInfo]
	deallocationNodeAssistant *space.DataNodeAssistant[deallocationKey, types.PersistentAddress]

	spaceDeletionCounters     [types.NumOfSpaces]uint64
	deallocationListsToCommit map[types.SnapshotID]*list.Pointer

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
	commitCh := make(chan error, 2) // 1 for store and 1 for supervisor
	tx := db.txRequestFactory.New()
	tx.Type = pipeline.Commit
	tx.CommitCh = commitCh
	db.queue.Push(tx)

	if err := <-commitCh; err != nil {
		return err
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
			defer db.config.Store.Close()

			return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
				supervisorReader := db.queueReader
				prepareTxReader := pipeline.CloneReader(db.queueReader)
				prepareTxReaders := make([]*pipeline.Reader, 0, 3)
				for range cap(prepareTxReaders) {
					prepareTxReaders = append(prepareTxReaders, prepareTxReader)
					prepareTxReader = pipeline.NewReader(prepareTxReader)
				}
				executeTxReader := prepareTxReader
				deallocateReader := pipeline.NewReader(executeTxReader)
				prevHashReader := deallocateReader
				dataHashReaders := make([]*pipeline.Reader, 0, 2)
				for range cap(dataHashReaders) {
					nextReader := pipeline.NewReader(prevHashReader)
					dataHashReaders = append(dataHashReaders, nextReader)
					prevHashReader = nextReader
				}
				pointerHashReaders := make([]*pipeline.Reader, 0, 2)
				for range cap(pointerHashReaders) {
					nextReader := pipeline.NewReader(prevHashReader)
					pointerHashReaders = append(pointerHashReaders, nextReader)
					prevHashReader = nextReader
				}
				skipNodesAndCommitReader := pipeline.NewReader(prevHashReader)
				storeNodesReader := pipeline.NewReader(skipNodesAndCommitReader)

				spawn("supervisor", parallel.Exit, func(ctx context.Context) error {
					var lastCommitCh chan<- error
					var processedCount uint64

					for {
						req, err := supervisorReader.Read(ctx)
						if err != nil && lastCommitCh != nil {
							lastCommitCh <- err
							lastCommitCh = nil
						}

						if req != nil {
							processedCount++
							supervisorReader.Acknowledge(processedCount, req)

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
						return db.prepareTransactions(ctx, reader)
					})
				}
				spawn("executeTx", parallel.Fail, func(ctx context.Context) error {
					return db.executeTransactions(ctx, executeTxReader)
				})
				spawn("deallocate", parallel.Fail, func(ctx context.Context) error {
					return db.processDeallocations(ctx, deallocateReader)
				})
				for i, reader := range dataHashReaders {
					spawn(fmt.Sprintf("datahash-%02d", i), parallel.Fail, func(ctx context.Context) error {
						return db.updateDataHashes(ctx, reader, uint64(i))
					})
				}
				for i, reader := range pointerHashReaders {
					spawn(fmt.Sprintf("pointerhash-%02d", i), parallel.Fail, func(ctx context.Context) error {
						return db.updatePointerHashes(ctx, reader, uint64(i))
					})
				}
				spawn("skipNodesAndCommit", parallel.Fail, func(ctx context.Context) error {
					return db.skipNodesAndCommit(ctx, skipNodesAndCommitReader)
				})
				spawn("storeNodes", parallel.Fail, func(ctx context.Context) error {
					return db.storeNodes(ctx, storeNodesReader)
				})

				return nil
			})
		})

		return nil
	})
}

func (db *DB) deleteSnapshot(
	snapshotID types.SnapshotID,
	tx *pipeline.TransactionRequest,
	volatileAllocator *alloc.Allocator[types.VolatileAddress],
	persistentDeallocator *alloc.Deallocator[types.PersistentAddress],
	snapshotHashBuff []byte,
	snapshotHashMatches []uint64,
	deallocationHashBuff []byte,
	deallocationHashMatches []uint64,
	storeReader *persistent.Reader,
	nodeBuff1, nodeBuff2 unsafe.Pointer,
) error {
	if snapshotID == db.snapshotInfo.PreviousSnapshotID {
		return errors.New("deleting latest snapshot is forbidden")
	}

	var snapshotInfoValue space.Entry[types.SnapshotID, types.SnapshotInfo]
	db.snapshots.Find(&snapshotInfoValue, snapshotID, space.StageData, snapshotHashBuff, snapshotHashMatches)

	if exists := snapshotInfoValue.Exists(snapshotHashBuff, snapshotHashMatches); !exists {
		return errors.Errorf("snapshot %d to delete does not exist", snapshotID)
	}
	snapshotInfo := snapshotInfoValue.Value(snapshotHashBuff, snapshotHashMatches)

	snapshotInfoValue.Delete(tx, snapshotHashBuff, snapshotHashMatches)

	var nextSnapshotInfoValue space.Entry[types.SnapshotID, types.SnapshotInfo]
	db.snapshots.Find(&nextSnapshotInfoValue, snapshotInfo.NextSnapshotID, space.StageData, snapshotHashBuff,
		snapshotHashMatches)

	if exists := nextSnapshotInfoValue.Exists(snapshotHashBuff, snapshotHashMatches); !exists {
		return errors.Errorf("next snapshot %d does not exist", snapshotID)
	}
	nextSnapshotInfo := nextSnapshotInfoValue.Value(snapshotHashBuff, snapshotHashMatches)

	deallocationListsRoot := types.NodeRoot{
		Pointer: &snapshotInfo.DeallocationRoot,
	}
	deallocationLists := space.New[deallocationKey, types.PersistentAddress](
		space.Config[deallocationKey, types.PersistentAddress]{
			SpaceRoot:         deallocationListsRoot,
			State:             db.config.State,
			DataNodeAssistant: db.deallocationNodeAssistant,
			DeletionCounter:   lo.ToPtr[uint64](0),
		},
	)

	//nolint:nestif
	if nextSnapshotInfo.DeallocationRoot.VolatileAddress != types.FreeAddress {
		var err error
		for nextDeallocSnapshot := range space.PersistentIteratorAndDeallocator(
			nextSnapshotInfo.DeallocationRoot,
			storeReader,
			db.deallocationNodeAssistant,
			persistentDeallocator,
			nodeBuff1,
			&err,
		) {
			if nextDeallocSnapshot.Key.SnapshotID > snapshotInfo.PreviousSnapshotID &&
				nextDeallocSnapshot.Key.SnapshotID <= snapshotID {
				if err := list.Deallocate(nextDeallocSnapshot.Value, storeReader, persistentDeallocator,
					nodeBuff2); err != nil {
					return err
				}

				continue
			}

			var deallocationListValue space.Entry[deallocationKey, types.PersistentAddress]
			deallocationLists.Find(&deallocationListValue, nextDeallocSnapshot.Key, space.StageData, deallocationHashBuff,
				deallocationHashMatches)
			if err := deallocationListValue.Set(
				tx,
				volatileAllocator,
				nextDeallocSnapshot.Value,
				deallocationHashBuff,
				deallocationHashMatches,
			); err != nil {
				return err
			}
		}
		if err != nil {
			return err
		}
	}

	nextSnapshotInfo.DeallocationRoot = snapshotInfo.DeallocationRoot
	nextSnapshotInfo.PreviousSnapshotID = snapshotInfo.PreviousSnapshotID
	if err := nextSnapshotInfoValue.Set(
		tx,
		volatileAllocator,
		nextSnapshotInfo,
		snapshotHashBuff,
		snapshotHashMatches,
	); err != nil {
		return err
	}

	if snapshotInfo.PreviousSnapshotID > 0 {
		var previousSnapshotInfoValue space.Entry[types.SnapshotID, types.SnapshotInfo]
		db.snapshots.Find(&previousSnapshotInfoValue, snapshotInfo.PreviousSnapshotID, space.StageData,
			snapshotHashBuff, snapshotHashMatches)

		if exists := previousSnapshotInfoValue.Exists(snapshotHashBuff, snapshotHashMatches); !exists {
			return errors.Errorf("previous snapshot %d does not exist", snapshotID)
		}

		previousSnapshotInfo := previousSnapshotInfoValue.Value(snapshotHashBuff, snapshotHashMatches)
		previousSnapshotInfo.NextSnapshotID = snapshotInfo.NextSnapshotID

		if err := previousSnapshotInfoValue.Set(
			tx,
			volatileAllocator,
			previousSnapshotInfo,
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
	volatileAllocator *alloc.Allocator[types.VolatileAddress],
	volatileDeallocator *alloc.Deallocator[types.VolatileAddress],
	persistentAllocator *alloc.Allocator[types.PersistentAddress],
	snapshotHashBuff []byte,
	snapshotHashMatches []uint64,
	deallocationHashBuff []byte,
	deallocationHashMatches []uint64,
) error {
	lastSr := tx.LastStoreRequest
	//nolint:nestif
	if len(db.deallocationListsToCommit) > 0 {
		lists := make([]types.SnapshotID, 0, len(db.deallocationListsToCommit))
		for snapshotID := range db.deallocationListsToCommit {
			lists = append(lists, snapshotID)
		}
		sort.Slice(lists, func(i, j int) bool { return lists[i] < lists[j] })

		var lr *pipeline.ListRequest
		for _, snapshotID := range lists {
			listRoot := db.deallocationListsToCommit[snapshotID]
			var deallocationListValue space.Entry[deallocationKey, types.PersistentAddress]
			db.deallocationLists.Find(&deallocationListValue, deallocationKey{
				ListSnapshotID: db.singularityNode.LastSnapshotID,
				SnapshotID:     snapshotID,
			}, space.StageData, deallocationHashBuff,
				deallocationHashMatches)
			if err := deallocationListValue.Set(
				tx,
				volatileAllocator,
				listRoot.PersistentAddress,
				deallocationHashBuff,
				deallocationHashMatches,
			); err != nil {
				return err
			}

			if lr == nil {
				lr = &pipeline.ListRequest{}
			}
			lr.List[lr.ListsToStore] = *listRoot
			lr.ListsToStore++
			if lr.ListsToStore == pipeline.StoreCapacity {
				tx.AddListRequest(lr)
				lr = nil
			}
		}
		if lr != nil {
			tx.AddListRequest(lr)
		}

		for sr := *lastSr; sr != nil; sr = sr.Next {
			for i := range sr.PointersToStore {
				if sr.Store[i].Pointer.SnapshotID != db.singularityNode.LastSnapshotID {
					persistentAddr, err := persistentAllocator.Allocate()
					if err != nil {
						return err
					}

					sr.Store[i].Pointer.SnapshotID = db.singularityNode.LastSnapshotID
					sr.Store[i].Pointer.PersistentAddress = persistentAddr
				}
				sr.Store[i].Pointer.Revision = uintptr(unsafe.Pointer(sr))
			}
		}

		lastSr = tx.LastStoreRequest

		// It is safe to deallocate volatile nodes here despite they are used to write persistent nodes later.
		// It's because deallocated nodes are never reallocated until current commit is finalized.
		space.DeallocateVolatile(db.snapshotInfo.DeallocationRoot.VolatileAddress, volatileDeallocator, db.config.State)
	}

	var nextSnapshotInfoValue space.Entry[types.SnapshotID, types.SnapshotInfo]
	db.snapshots.Find(&nextSnapshotInfoValue, db.singularityNode.LastSnapshotID, space.StageData, snapshotHashBuff,
		snapshotHashMatches)
	if err := nextSnapshotInfoValue.Set(
		tx,
		volatileAllocator,
		db.snapshotInfo,
		snapshotHashBuff,
		snapshotHashMatches,
	); err != nil {
		return err
	}

	for sr := *lastSr; sr != nil; sr = sr.Next {
		for i := range sr.PointersToStore {
			if sr.Store[i].Pointer.SnapshotID != db.singularityNode.LastSnapshotID {
				persistentAddr, err := persistentAllocator.Allocate()
				if err != nil {
					return err
				}

				sr.Store[i].Pointer.SnapshotID = db.singularityNode.LastSnapshotID
				sr.Store[i].Pointer.PersistentAddress = persistentAddr
			}
			sr.Store[i].Pointer.Revision = uintptr(unsafe.Pointer(sr))
		}
	}

	sr := &pipeline.StoreRequest{}
	sr.Store[0] = db.config.State.SingularityNodeRoot()
	sr.Store[0].Pointer.Revision = uintptr(unsafe.Pointer(sr))
	sr.PointersToStore = 1
	tx.AddStoreRequest(sr)

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

	hashBuff := s.NewHashBuff()
	hashMatches := s.NewHashMatches()

	for processedCount := uint64(0); ; processedCount++ {
		req, err := pipeReader.Read(ctx)
		if err != nil {
			return err
		}

		if req.Transaction != nil {
			if transferTx, ok := req.Transaction.(*transfer.Tx); ok {
				transferTx.Prepare(s, hashBuff, hashMatches)
			}
		}

		pipeReader.Acknowledge(processedCount+1, req)
	}
}

func (db *DB) executeTransactions(ctx context.Context, pipeReader *pipeline.Reader) error {
	storeReader, err := db.config.Store.NewReader()
	if err != nil {
		return err
	}
	defer storeReader.Close()

	volatileAllocator := db.config.State.NewVolatileAllocator()
	persistentDeallocator := db.config.State.NewPersistentDeallocator()

	// We use allocator to allocate this region to get aligned address effortlessly.
	nodeBuff1Address, err := volatileAllocator.Allocate()
	if err != nil {
		return err
	}
	nodeBuff2Address, err := volatileAllocator.Allocate()
	if err != nil {
		return err
	}
	nodeBuff1 := db.config.State.Node(nodeBuff1Address)
	nodeBuff2 := db.config.State.Node(nodeBuff2Address)

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

	for processedCount := uint64(0); ; processedCount++ {
		req, err := pipeReader.Read(ctx)
		if err != nil {
			return err
		}

		if req.Transaction != nil {
			switch tx := req.Transaction.(type) {
			case *transfer.Tx:
				if err := tx.Execute(req, volatileAllocator, hashBuff, hashMatches); err != nil {
					return err
				}
			case *deleteSnapshotTx:
				if err := db.deleteSnapshot(tx.SnapshotID, req, volatileAllocator,
					persistentDeallocator, snapshotHashBuff, snapshotHashMatches, deallocationHashBuff,
					deallocationHashMatches, storeReader, nodeBuff1, nodeBuff2); err != nil {
					return err
				}
			case *genesis.Tx:
				if err := tx.Execute(s, req, volatileAllocator, hashBuff, hashMatches); err != nil {
					return err
				}
			default:
				return errors.New("unknown transaction type")
			}
		}

		pipeReader.Acknowledge(processedCount+1, req)
	}
}

func (db *DB) processDeallocations(ctx context.Context, pipeReader *pipeline.Reader) error {
	volatileAllocator := db.config.State.NewVolatileAllocator()
	persistentAllocator := db.config.State.NewPersistentAllocator()
	persistentDeallocator := db.config.State.NewPersistentDeallocator()

	for processedCount := uint64(0); ; processedCount++ {
		req, err := pipeReader.Read(ctx)
		if err != nil {
			return err
		}

		var lr *pipeline.ListRequest
		for sr := req.StoreRequest; sr != nil; sr = sr.Next {
			if sr.PointersToStore == 0 {
				continue
			}

			for i := range sr.PointersToStore {
				root := sr.Store[i]
				root.Pointer.Revision = uintptr(unsafe.Pointer(sr))

				//nolint:nestif
				if root.Pointer.SnapshotID != db.singularityNode.LastSnapshotID {
					if root.Pointer.PersistentAddress != 0 {
						listNodePointer, err := db.deallocateNode(root.Pointer.SnapshotID, root.Pointer.PersistentAddress,
							volatileAllocator, persistentAllocator, persistentDeallocator, sr.NoSnapshots)
						if err != nil {
							return err
						}

						if listNodePointer.VolatileAddress != types.FreeAddress {
							if lr == nil {
								lr = &pipeline.ListRequest{}
							}
							lr.List[lr.ListsToStore] = listNodePointer
							lr.ListsToStore++
							if lr.ListsToStore == pipeline.StoreCapacity {
								req.AddListRequest(lr)
								lr = nil
							}
						}

						root.Pointer.PersistentAddress = 0
					}
					root.Pointer.SnapshotID = db.singularityNode.LastSnapshotID
				}

				// Singularity node might be stored under address 0 which could cause a mess here.
				// But it doesn't matter, because singularity node is added to the pipeline at later stage.
				if root.Pointer.PersistentAddress == 0 {
					persistentAddress, err := persistentAllocator.Allocate()
					if err != nil {
						return err
					}
					root.Pointer.PersistentAddress = persistentAddress
				}
			}
		}
		if lr != nil {
			req.AddListRequest(lr)
		}

		pipeReader.Acknowledge(processedCount+1, req)
	}
}

func (db *DB) updateDataHashes(
	ctx context.Context,
	pipeReader *pipeline.Reader,
	mod uint64,
) error {
	var matrix [16]*byte
	matrixP := &matrix[0]

	var hashes [16]*byte
	hashesP := &hashes[0]

	var mask uint16
	var slotIndex int

	for processedCount := uint64(0); ; processedCount++ {
		req, err := pipeReader.Read(ctx)
		if err != nil {
			return err
		}

		for sr := req.StoreRequest; sr != nil; sr = sr.Next {
			if sr.PointersToStore == 0 || sr.NoSnapshots {
				continue
			}

			index := sr.PointersToStore - 1
			pointer := sr.Store[index].Pointer

			if uint64(pointer.VolatileAddress)&1 != mod || pointer.Revision != uintptr(unsafe.Pointer(sr)) {
				continue
			}

			mask |= 1 << slotIndex
			matrix[slotIndex] = (*byte)(db.config.State.Node(pointer.VolatileAddress))
			hashes[slotIndex] = &sr.Store[index].Hash[0]

			slotIndex++
			if slotIndex == len(matrix) {
				hash.Blake34096(matrixP, hashesP, mask)

				slotIndex = 0
				mask = 0

				pipeReader.Acknowledge(processedCount, req)
			}
		}

		if req.Type == pipeline.Commit {
			if mask != 0 {
				hash.Blake34096(matrixP, hashesP, mask)

				slotIndex = 0
				mask = 0
			}

			pipeReader.Acknowledge(processedCount+1, req)
		}
	}
}

type request struct {
	TxRequest    *pipeline.TransactionRequest
	Count        uint64
	StoreRequest *pipeline.StoreRequest
	PointerIndex int8
}

type reader struct {
	pipeReader *pipeline.Reader
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

			if r.txRequest.StoreRequest != nil {
				r.storeRequest = r.txRequest.StoreRequest
			}
			r.read++
		}
		for r.storeRequest != nil && (r.storeRequest.PointersToStore < 2 || r.storeRequest.NoSnapshots) {
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
			req.PointerIndex = sr.PointersToStore - 1
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
// - pointer present later in a pipeline must be included in later slot.
func (db *DB) updatePointerHashes(
	ctx context.Context,
	pipeReader *pipeline.Reader,
	mod uint64,
) error {
	r := &reader{
		pipeReader: pipeReader,
	}

	var slots [16]request

	var commitReq request

	var matrix [16]*byte
	matrixP := &matrix[0]

	var hashes [16]*byte
	hashesP := &hashes[0]

	for {
		var mask uint16

		minReq := request{
			Count: math.MaxUint64,
		}

		var nilSlots int

	riLoop:
		for ri := range slots {
			req := slots[ri]

			var pointer *types.Pointer
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
				pointer = req.StoreRequest.Store[req.PointerIndex].Pointer

				if uint64(pointer.VolatileAddress)&1 != mod {
					continue
				}
				if pointer.Revision != uintptr(unsafe.Pointer(req.StoreRequest)) {
					// Pointers are processed from the data node up t the root node. If at any level
					// revision test fails, it doesn't make sense to process parent nodes because revision
					// test will fail there for sure.
					req.PointerIndex = 0
					continue
				}

				if req.Count < minReq.Count {
					minReq = req
				}
				hash = req.StoreRequest.Store[req.PointerIndex].Hash

				break
			}

			slots[ri] = req
			mask |= 1 << ri
			matrix[ri] = (*byte)(db.config.State.Node(pointer.VolatileAddress))
			hashes[ri] = &hash[0]
		}

		if nilSlots == len(slots) {
			r.Acknowledge(commitReq.Count, commitReq.TxRequest, true)
			commitReq = request{}
		} else {
			hash.Blake32048(matrixP, hashesP, mask)
			r.Acknowledge(minReq.Count-1, minReq.TxRequest, false)
		}
	}
}

func (db *DB) skipNodesAndCommit(ctx context.Context, pipeReader *pipeline.Reader) error {
	volatileAllocator := db.config.State.NewVolatileAllocator()
	volatileDeallocator := db.config.State.NewVolatileDeallocator()
	persistentAllocator := db.config.State.NewPersistentAllocator()
	snapshotHashBuff := db.snapshots.NewHashBuff()
	snapshotHashMatches := db.snapshots.NewHashMatches()
	deallocationHashBuff := db.deallocationLists.NewHashBuff()
	deallocationHashMatches := db.deallocationLists.NewHashMatches()

	for processedCount := uint64(0); ; processedCount++ {
		req, err := pipeReader.Read(ctx)
		if err != nil {
			return err
		}

		prevPointer := &req.StoreRequest
		for sr := req.StoreRequest; sr != nil; sr = sr.Next {
			for i := sr.PointersToStore - 1; i >= 0; i-- {
				if sr.Store[i].Pointer.Revision != uintptr(unsafe.Pointer(sr)) {
					sr.PointersLast = i + 1
					break
				}
			}
			if sr.PointersLast == sr.PointersToStore {
				*prevPointer = sr.Next
			} else {
				prevPointer = &sr.Next
			}
		}

		if req.Type == pipeline.Commit {
			if err := db.commit(req, volatileAllocator, volatileDeallocator, persistentAllocator, snapshotHashBuff,
				snapshotHashMatches, deallocationHashBuff, deallocationHashMatches); err != nil {
				return err
			}
		}

		pipeReader.Acknowledge(processedCount+1, req)
	}
}

func (db *DB) storeNodes(ctx context.Context, pipeReader *pipeline.Reader) error {
	storeWriter, err := db.config.Store.NewWriter(db.config.State.Origin(),
		db.config.State.VolatileSize())
	if err != nil {
		return err
	}
	defer storeWriter.Close()

	volatileDeallocator := db.config.State.NewVolatileDeallocator()

	for processedCount := uint64(0); ; processedCount++ {
		req, err := pipeReader.Read(ctx)
		if err != nil {
			return err
		}

		for lr := req.ListRequest; lr != nil; lr = lr.Next {
			for i := range lr.ListsToStore {
				if err := storeWriter.Write(lr.List[i].PersistentAddress, lr.List[i].VolatileAddress); err != nil {
					return err
				}
				volatileDeallocator.Deallocate(lr.List[i].VolatileAddress)
			}
		}

		for sr := req.StoreRequest; sr != nil; sr = sr.Next {
			for i := sr.PointersToStore - 1; i >= sr.PointersLast; i-- {
				pointer := sr.Store[i].Pointer

				if pointer.Revision == uintptr(unsafe.Pointer(sr)) {
					if err := storeWriter.Write(pointer.PersistentAddress, pointer.VolatileAddress); err != nil {
						return err
					}
				}
			}
		}

		if req.Type == pipeline.Commit {
			req.CommitCh <- nil
		}

		pipeReader.Acknowledge(processedCount+1, req)
	}
}

func (db *DB) deallocateNode(
	nodeSnapshotID types.SnapshotID,
	nodeAddress types.PersistentAddress,
	volatileAllocator *alloc.Allocator[types.VolatileAddress],
	persistentAllocator *alloc.Allocator[types.PersistentAddress],
	persistentDeallocator *alloc.Deallocator[types.PersistentAddress],
	immediateDeallocation bool,
) (list.Pointer, error) {
	if nodeSnapshotID > db.snapshotInfo.PreviousSnapshotID || immediateDeallocation {
		persistentDeallocator.Deallocate(nodeAddress)

		return list.Pointer{}, nil
	}

	listRoot := db.deallocationListsToCommit[nodeSnapshotID]
	if listRoot == nil {
		listRoot = &list.Pointer{}
		db.deallocationListsToCommit[nodeSnapshotID] = listRoot
	}

	return list.Add(listRoot, nodeAddress, db.config.State, volatileAllocator, persistentAllocator)
}

func (db *DB) prepareNextSnapshot() error {
	db.snapshotInfo.PreviousSnapshotID = db.singularityNode.LastSnapshotID
	db.singularityNode.LastSnapshotID++
	db.snapshotInfo.NextSnapshotID = db.singularityNode.LastSnapshotID + 1
	db.snapshotInfo.DeallocationRoot = types.Pointer{}

	clear(db.deallocationListsToCommit)

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
		DeletionCounter:   &db.spaceDeletionCounters[spaceID],
	}), nil
}

type deleteSnapshotTx struct {
	SnapshotID types.SnapshotID
}
