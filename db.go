package quantum

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync/atomic"
	"unsafe"

	"github.com/pkg/errors"

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
	waltypes "github.com/outofforest/quantum/wal/types"
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
	Root types.NodeAddress
}

// New creates new database.
func New(config Config) (*DB, error) {
	snapshotInfoNodeAssistant, err := space.NewDataNodeAssistant[types.SnapshotID, types.SnapshotInfo]()
	if err != nil {
		return nil, err
	}

	snapshotToNodeNodeAssistant, err := space.NewDataNodeAssistant[types.SnapshotID, types.NodeAddress]()
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
		deallocationListsToCommit:   map[types.SnapshotID]*ListToCommit{},
		queue:                       queue,
		queueReader:                 queueReader,
	}

	// Logical nodes might be deallocated immediately.
	db.snapshots = space.New[types.SnapshotID, types.SnapshotInfo](space.Config[types.SnapshotID, types.SnapshotInfo]{
		SpaceRoot: types.NodeRoot{
			Pointer: &db.singularityNode.SnapshotRoot,
		},
		State:             config.State,
		DataNodeAssistant: snapshotInfoNodeAssistant,
		NoSnapshots:       true,
	})

	db.deallocationLists = space.New[types.SnapshotID, types.NodeAddress](
		space.Config[types.SnapshotID, types.NodeAddress]{
			SpaceRoot: types.NodeRoot{
				Pointer: &db.snapshotInfo.DeallocationRoot,
			},
			State:             config.State,
			DataNodeAssistant: snapshotToNodeNodeAssistant,
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
	deallocationLists *space.Space[types.SnapshotID, types.NodeAddress]

	snapshotInfoNodeAssistant   *space.DataNodeAssistant[types.SnapshotID, types.SnapshotInfo]
	snapshotToNodeNodeAssistant *space.DataNodeAssistant[types.SnapshotID, types.NodeAddress]

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
				supervisorReader := db.queueReader
				prepareTxReader := pipeline.CloneReader(db.queueReader)
				prepareTxReaders := make([]*pipeline.Reader, 0, 3)
				for range cap(prepareTxReaders) {
					prepareTxReaders = append(prepareTxReaders, prepareTxReader)
					prepareTxReader = pipeline.NewReader(prepareTxReader)
				}
				executeTxReader := prepareTxReader
				deallocateReader := pipeline.NewReader(executeTxReader)
				copyReaders := make([]*pipeline.Reader, 0, 4)
				for range cap(copyReaders) {
					copyReaders = append(copyReaders, pipeline.NewReader(deallocateReader))
				}
				applyWALReader := pipeline.NewReader(copyReaders...)
				prevHashReader := applyWALReader
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

				commitSyncReader := pipeline.NewReader(prevHashReader)

				spawn("supervisor", parallel.Exit, func(ctx context.Context) error {
					var lastSyncCh chan<- struct{}
					var lastCommitCh chan<- error
					var processedCount uint64

					for {
						req, err := supervisorReader.Read(ctx)
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
							supervisorReader.Acknowledge(processedCount, req)

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
						return db.prepareTransactions(ctx, reader)
					})
				}
				spawn("executeTx", parallel.Fail, func(ctx context.Context) error {
					return db.executeTransactions(ctx, executeTxReader)
				})
				spawn("deallocate", parallel.Fail, func(ctx context.Context) error {
					return db.processDeallocations(ctx, deallocateReader)
				})
				for i, reader := range copyReaders {
					spawn(fmt.Sprintf("copy-%02d", i), parallel.Fail, func(ctx context.Context) error {
						return db.copyNodes(ctx, reader, uint64(i))
					})
				}
				spawn("applyWAL", parallel.Fail, func(ctx context.Context) error {
					return db.applyWALChanges(ctx, applyWALReader)
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
				spawn("commitSync", parallel.Fail, func(ctx context.Context) error {
					return db.syncOnCommit(ctx, commitSyncReader)
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
	walRecorder *wal.Recorder,
	allocator *alloc.Allocator,
	deallocator *alloc.Deallocator,
	snapshotHashBuff []byte,
	snapshotHashMatches []uint64,
	deallocationHashBuff []byte,
	deallocationHashMatches []uint64,
) error {
	if snapshotID == db.snapshotInfo.PreviousSnapshotID {
		return errors.New("deleting latest snapshot is forbidden")
	}

	var snapshotInfoValue space.Entry[types.SnapshotID, types.SnapshotInfo]
	err := db.snapshots.Find(&snapshotInfoValue, snapshotID, tx, walRecorder, allocator, snapshotID, space.StageData,
		snapshotHashBuff, snapshotHashMatches)
	if err != nil {
		return err
	}
	exists, err := snapshotInfoValue.Exists(snapshotID, tx, walRecorder, allocator, snapshotHashBuff,
		snapshotHashMatches)
	if err != nil {
		return err
	}
	if !exists {
		return errors.Errorf("snapshot %d to delete does not exist", snapshotID)
	}
	snapshotInfo, err := snapshotInfoValue.Value(snapshotID, tx, walRecorder, allocator, snapshotHashBuff,
		snapshotHashMatches)
	if err != nil {
		return err
	}

	if err := snapshotInfoValue.Delete(snapshotID, tx, walRecorder, allocator, snapshotHashBuff,
		snapshotHashMatches); err != nil {
		return err
	}

	var nextSnapshotInfoValue space.Entry[types.SnapshotID, types.SnapshotInfo]
	if err := db.snapshots.Find(&nextSnapshotInfoValue, snapshotID, tx, walRecorder, allocator,
		snapshotInfo.NextSnapshotID, space.StageData, snapshotHashBuff, snapshotHashMatches); err != nil {
		return err
	}

	exists, err = nextSnapshotInfoValue.Exists(snapshotID, tx, walRecorder, allocator, snapshotHashBuff,
		snapshotHashMatches)
	if err != nil {
		return err
	}
	if !exists {
		return errors.Errorf("next snapshot %d does not exist", snapshotID)
	}
	nextSnapshotInfo, err := nextSnapshotInfoValue.Value(snapshotID, tx, walRecorder, allocator, snapshotHashBuff,
		snapshotHashMatches)
	if err != nil {
		return err
	}

	nextDeallocationListRoot := types.NodeRoot{
		Pointer: &nextSnapshotInfo.DeallocationRoot,
	}
	nextDeallocationLists := space.New[types.SnapshotID, types.NodeAddress](
		space.Config[types.SnapshotID, types.NodeAddress]{
			SpaceRoot:         nextDeallocationListRoot,
			State:             db.config.State,
			DataNodeAssistant: db.snapshotToNodeNodeAssistant,
		},
	)

	deallocationListsRoot := types.NodeRoot{
		Pointer: &snapshotInfo.DeallocationRoot,
	}
	deallocationLists := space.New[types.SnapshotID, types.NodeAddress](
		space.Config[types.SnapshotID, types.NodeAddress]{
			SpaceRoot:         deallocationListsRoot,
			State:             db.config.State,
			DataNodeAssistant: db.snapshotToNodeNodeAssistant,
		},
	)

	for nextDeallocSnapshot := range nextDeallocationLists.Iterator() {
		if nextDeallocSnapshot.Key > snapshotInfo.PreviousSnapshotID && nextDeallocSnapshot.Key <= snapshotID {
			list.Deallocate(
				nextDeallocSnapshot.Value,
				db.config.State,
				deallocator,
			)

			continue
		}

		var deallocationListValue space.Entry[types.SnapshotID, types.NodeAddress]
		err := deallocationLists.Find(&deallocationListValue, snapshotID, tx, walRecorder, allocator,
			nextDeallocSnapshot.Key, space.StageData, deallocationHashBuff, deallocationHashMatches)
		if err != nil {
			return err
		}
		listNodeAddress, err := deallocationListValue.Value(snapshotID, tx, walRecorder, allocator, deallocationHashBuff,
			deallocationHashMatches)
		if err != nil {
			return err
		}
		list := list.New(list.Config{
			Root:  listNodeAddress,
			State: db.config.State,
		})

		listRoot, err := list.Attach(nextDeallocSnapshot.Value, allocator)
		if err != nil {
			return err
		}
		if listRoot != listNodeAddress {
			if err := deallocationListValue.Set(
				snapshotID,
				tx,
				walRecorder,
				allocator,
				listRoot,
				deallocationHashBuff,
				deallocationHashMatches,
			); err != nil {
				return err
			}
		}
	}

	nextSnapshotInfo.DeallocationRoot = snapshotInfo.DeallocationRoot
	nextSnapshotInfo.PreviousSnapshotID = snapshotInfo.PreviousSnapshotID
	if err := nextSnapshotInfoValue.Set(
		snapshotID,
		tx,
		walRecorder,
		allocator,
		nextSnapshotInfo,
		snapshotHashBuff,
		snapshotHashMatches,
	); err != nil {
		return err
	}

	space.Deallocate(
		nextDeallocationListRoot.Pointer,
		deallocator,
		db.config.State,
	)

	//nolint:nestif
	if snapshotInfo.PreviousSnapshotID > 0 {
		var previousSnapshotInfoValue space.Entry[types.SnapshotID, types.SnapshotInfo]
		err := db.snapshots.Find(&previousSnapshotInfoValue, snapshotID, tx, walRecorder, allocator,
			snapshotInfo.PreviousSnapshotID, space.StageData, snapshotHashBuff, snapshotHashMatches)
		if err != nil {
			return err
		}
		exists, err := previousSnapshotInfoValue.Exists(snapshotID,
			tx, walRecorder, allocator, snapshotHashBuff, snapshotHashMatches)
		if err != nil {
			return err
		}
		if !exists {
			return errors.Errorf("previous snapshot %d does not exist", snapshotID)
		}

		previousSnapshotInfo, err := previousSnapshotInfoValue.Value(snapshotID, tx, walRecorder, allocator,
			snapshotHashBuff, snapshotHashMatches)
		if err != nil {
			return err
		}
		previousSnapshotInfo.NextSnapshotID = snapshotInfo.NextSnapshotID

		if err := previousSnapshotInfoValue.Set(
			snapshotID,
			tx,
			walRecorder,
			allocator,
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
	commitSnapshotID types.SnapshotID,
	tx *pipeline.TransactionRequest,
	walRecorder *wal.Recorder,
	allocator *alloc.Allocator,
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

		for _, snapshotID := range lists {
			l := db.deallocationListsToCommit[snapshotID]
			var deallocationListValue space.Entry[types.SnapshotID, types.NodeAddress]
			err := db.deallocationLists.Find(&deallocationListValue, commitSnapshotID, tx, walRecorder, allocator,
				snapshotID, space.StageData, deallocationHashBuff, deallocationHashMatches)
			if err != nil {
				return err
			}
			exists, err := deallocationListValue.Exists(commitSnapshotID, tx, walRecorder, allocator,
				deallocationHashBuff, deallocationHashMatches)
			if err != nil {
				return err
			}
			if exists {
				v, err := deallocationListValue.Value(commitSnapshotID, tx, walRecorder, allocator,
					deallocationHashBuff, deallocationHashMatches)
				if err != nil {
					return err
				}
				listRoot, err := l.List.Attach(v, allocator)
				if err != nil {
					return err
				}
				if listRoot != l.Root {
					l.Root = listRoot
				}
			}
			if err := deallocationListValue.Set(
				commitSnapshotID,
				tx,
				walRecorder,
				allocator,
				l.Root,
				deallocationHashBuff,
				deallocationHashMatches,
			); err != nil {
				return err
			}
		}

		clear(db.deallocationListsToCommit)
	}

	var nextSnapshotInfoValue space.Entry[types.SnapshotID, types.SnapshotInfo]
	err := db.snapshots.Find(&nextSnapshotInfoValue, commitSnapshotID, tx, walRecorder, allocator,
		db.singularityNode.LastSnapshotID, space.StageData, snapshotHashBuff, snapshotHashMatches)
	if err != nil {
		return err
	}
	if err := nextSnapshotInfoValue.Set(
		commitSnapshotID,
		tx,
		walRecorder,
		allocator,
		db.snapshotInfo,
		snapshotHashBuff,
		snapshotHashMatches,
	); err != nil {
		return err
	}

	// FIXME (wojciech): Store singularity node
	// sr := massStoreRequest.New()
	// sr.Store[0] = db.config.State.SingularityNodeRoot()
	// sr.PointersToStore = 1
	// tx.AddStoreRequest(sr)

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

	allocator := db.config.State.NewAllocator()
	walRecorder := wal.NewRecorder(db.config.State, allocator)

	for processedCount := uint64(0); ; processedCount++ {
		req, err := pipeReader.Read(ctx)
		if err != nil {
			return err
		}

		if req.Transaction != nil {
			if transferTx, ok := req.Transaction.(*transfer.Tx); ok {
				if err := transferTx.Prepare(s, db.singularityNode.LastSnapshotID, req, walRecorder, allocator,
					hashBuff, hashMatches); err != nil {
					return err
				}
			}
		}

		pipeReader.Acknowledge(processedCount+1, req)
	}
}

func (db *DB) executeTransactions(ctx context.Context, pipeReader *pipeline.Reader) error {
	allocator := db.config.State.NewAllocator()
	deallocator := db.config.State.NewDeallocator()

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

	walRecorder := wal.NewRecorder(db.config.State, allocator)

	for processedCount := uint64(0); ; processedCount++ {
		req, err := pipeReader.Read(ctx)
		if err != nil {
			return err
		}

		if req.Transaction != nil {
			switch tx := req.Transaction.(type) {
			case *transfer.Tx:
				if err := tx.Execute(db.singularityNode.LastSnapshotID, req, walRecorder, allocator, hashBuff,
					hashMatches); err != nil {
					return err
				}
			case *commitTx:
				// Syncing must be finished just before committing because inside commit we store the results
				// of deallocations.
				<-tx.SyncCh
				if err := db.commit(db.singularityNode.LastSnapshotID, req, walRecorder, allocator, snapshotHashBuff,
					snapshotHashMatches, deallocationHashBuff, deallocationHashMatches); err != nil {
					req.CommitCh <- err
					return err
				}
				walRecorder.Commit(req)
			case *deleteSnapshotTx:
				if err := db.deleteSnapshot(tx.SnapshotID, req, walRecorder, allocator, deallocator, snapshotHashBuff,
					snapshotHashMatches, deallocationHashBuff, deallocationHashMatches); err != nil {
					return err
				}
			case *genesis.Tx:
				if err := tx.Execute(s, db.singularityNode.LastSnapshotID, req, walRecorder, allocator, hashBuff,
					hashMatches); err != nil {
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
	allocator := db.config.State.NewAllocator()
	deallocator := db.config.State.NewDeallocator()
	walRecorder := wal.NewRecorder(db.config.State, allocator)

	var revisionCounter uint32

	for processedCount := uint64(0); ; processedCount++ {
		req, err := pipeReader.Read(ctx)
		if err != nil {
			return err
		}

		for wr := req.WALRequest; wr != nil; wr = wr.Next {
			walNode := waltypes.ProjectNode(db.config.State.Node(wr.NodeAddress))
			var wrIndex uint16

			for wrIndex < waltypes.BlobSize && wal.RecordType(walNode.Blob[wrIndex]) != wal.RecordEnd {
				switch recordType := wal.RecordType(walNode.Blob[wrIndex]); recordType {
				case wal.RecordSet1:
					wrIndex += 10
				case wal.RecordSet8:
					wrIndex += 17
				case wal.RecordSet32:
					wrIndex += 41
				case wal.RecordSet:
					wrIndex += 9
					wrIndex += *(*uint16)(unsafe.Pointer(&walNode.Blob[wrIndex])) + 2
				case wal.RecordImmediateDeallocation, wal.RecordDelayedDeallocation:
					nodeSnapshotID := *(*types.SnapshotID)(unsafe.Pointer(&walNode.Blob[wrIndex+1]))
					oldNodeAddress := *(*types.NodeAddress)(unsafe.Pointer(&walNode.Blob[wrIndex+9]))
					wrIndex += 25

					if err := db.deallocateNode(nodeSnapshotID, oldNodeAddress, allocator, deallocator,
						recordType == wal.RecordImmediateDeallocation); err != nil {
						return err
					}
				default:
					fmt.Printf("%#v\n", walNode.Blob)
					panic("=============")
				}
			}
		}

		for sr := req.StoreRequest; sr != nil; sr = sr.Next {
			revisionCounter++
			sr.RequestedRevision = revisionCounter
			for i := range sr.PointersToStore {
				atomic.StoreUint32(&sr.Store[i].Pointer.Revision, revisionCounter)
			}
		}

		switch req.Type {
		case pipeline.Sync:
			req.SyncCh <- struct{}{}
		case pipeline.Commit:
			walRecorder.Commit(req)
		}

		pipeReader.Acknowledge(processedCount+1, req)
	}
}

func (db *DB) copyNodes(
	ctx context.Context,
	pipeReader *pipeline.Reader,
	mod uint64,
) error {
	for processedCount := uint64(0); ; processedCount++ {
		req, err := pipeReader.Read(ctx)
		if err != nil {
			return err
		}

		if processedCount&3 == mod {
			for wr := req.WALRequest; wr != nil; wr = wr.Next {
				walNode := waltypes.ProjectNode(db.config.State.Node(wr.NodeAddress))
				var wrIndex uint16

				for wrIndex < waltypes.BlobSize && wal.RecordType(walNode.Blob[wrIndex]) != wal.RecordEnd {
					switch recordType := wal.RecordType(walNode.Blob[wrIndex]); recordType {
					case wal.RecordSet1:
						wrIndex += 10
					case wal.RecordSet8:
						wrIndex += 17
					case wal.RecordSet32:
						wrIndex += 41
					case wal.RecordSet:
						wrIndex += 9
						wrIndex += *(*uint16)(unsafe.Pointer(&walNode.Blob[wrIndex])) + 2
					case wal.RecordImmediateDeallocation, wal.RecordDelayedDeallocation:
						oldNodeAddress := *(*types.NodeAddress)(unsafe.Pointer(&walNode.Blob[wrIndex+9]))
						newNodeAddress := *(*types.NodeAddress)(unsafe.Pointer(&walNode.Blob[wrIndex+17]))
						wrIndex += 25

						copy(db.config.State.Bytes(newNodeAddress), db.config.State.Bytes(oldNodeAddress))
					default:
						fmt.Printf("%#v\n", walNode.Blob)
						panic("=============")
					}
				}
			}
		}

		pipeReader.Acknowledge(processedCount+1, req)
	}
}

func (db *DB) applyWALChanges(ctx context.Context, pipeReader *pipeline.Reader) error {
	origin := db.config.State.Origin()

	for processedCount := uint64(0); ; processedCount++ {
		req, err := pipeReader.Read(ctx)
		if err != nil {
			return err
		}

		for wr := req.WALRequest; wr != nil; wr = wr.Next {
			walNode := waltypes.ProjectNode(db.config.State.Node(wr.NodeAddress))
			var wrIndex uint16

			for wrIndex < waltypes.BlobSize && wal.RecordType(walNode.Blob[wrIndex]) != wal.RecordEnd {
				switch recordType := wal.RecordType(walNode.Blob[wrIndex]); recordType {
				case wal.RecordSet1:
					offset := *(*uint64)(unsafe.Pointer(&walNode.Blob[wrIndex+1]))
					*(*byte)(unsafe.Add(origin, offset)) = walNode.Blob[wrIndex+9]
					wrIndex += 10
				case wal.RecordSet8:
					offset := *(*uint64)(unsafe.Pointer(&walNode.Blob[wrIndex+1]))
					copy(unsafe.Slice((*byte)(unsafe.Add(origin, offset)), 8), walNode.Blob[wrIndex+9:])
					wrIndex += 17
				case wal.RecordSet32:
					offset := *(*uint64)(unsafe.Pointer(&walNode.Blob[wrIndex+1]))
					copy(unsafe.Slice((*byte)(unsafe.Add(origin, offset)), 32), walNode.Blob[wrIndex+9:])
					wrIndex += 41
				case wal.RecordSet:
					offset := *(*uint64)(unsafe.Pointer(&walNode.Blob[wrIndex+1]))
					size := *(*uint16)(unsafe.Pointer(&walNode.Blob[wrIndex+9]))
					copy(unsafe.Slice((*byte)(unsafe.Add(origin, offset)), size), walNode.Blob[wrIndex+11:])
					wrIndex += size + 2
				case wal.RecordImmediateDeallocation, wal.RecordDelayedDeallocation:
					wrIndex += 25
				default:
					fmt.Printf("%#v\n", walNode.Blob)
					panic("=============")
				}
			}
		}

		pipeReader.Acknowledge(processedCount+1, req)
	}
}

func (db *DB) updateDataHashes(
	ctx context.Context,
	pipeReader *pipeline.Reader,
	mod uint64,
) error {
	allocator := db.config.State.NewAllocator()
	walRecorder := wal.NewRecorder(db.config.State, allocator)

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

			if uint64(pointer.VolatileAddress)&1 != mod ||
				atomic.LoadUint32(&pointer.Revision) != sr.RequestedRevision {
				continue
			}

			mask |= 1 << slotIndex
			matrix[slotIndex] = (*byte)(db.config.State.Node(pointer.VolatileAddress))

			if index > 0 {
				walHash, err := wal.Reserve(walRecorder, req, sr.Store[index-1].Pointer.PersistentAddress,
					sr.Store[index].Hash)
				if err != nil {
					return err
				}
				hashes[slotIndex] = &walHash[0]
			} else {
				hashes[slotIndex] = &sr.Store[index].Hash[0]
			}

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

			walRecorder.Commit(req)
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
	allocator := db.config.State.NewAllocator()
	walRecorder := wal.NewRecorder(db.config.State, allocator)

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
				if atomic.LoadUint32(&pointer.Revision) != req.StoreRequest.RequestedRevision {
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

			if req.PointerIndex > 0 {
				walHash, err := wal.Reserve(walRecorder, minReq.TxRequest,
					req.StoreRequest.Store[req.PointerIndex-1].Pointer.PersistentAddress, hash)
				if err != nil {
					return err
				}
				hashes[ri] = &walHash[0]
			} else {
				hashes[ri] = &hash[0]
			}
		}

		if nilSlots == len(slots) {
			walRecorder.Commit(commitReq.TxRequest)
			r.Acknowledge(commitReq.Count, commitReq.TxRequest, true)
			commitReq = request{}
		} else {
			hash.Blake32048(matrixP, hashesP, mask)
			r.Acknowledge(minReq.Count-1, minReq.TxRequest, false)
		}
	}
}

func (db *DB) syncOnCommit(ctx context.Context, pipeReader *pipeline.Reader) error {
	for processedCount := uint64(0); ; processedCount++ {
		req, err := pipeReader.Read(ctx)
		if err != nil {
			return err
		}

		if req.Type == pipeline.Commit {
			for _, store := range db.config.Stores {
				err := store.Sync()
				req.CommitCh <- err
				if err != nil {
					return err
				}
			}
		}

		pipeReader.Acknowledge(processedCount+1, req)
	}
}

func (db *DB) deallocateNode(
	nodeSnapshotID types.SnapshotID,
	nodeAddress types.NodeAddress,
	allocator *alloc.Allocator,
	deallocator *alloc.Deallocator,
	immediateDeallocation bool,
) error {
	if nodeSnapshotID > db.snapshotInfo.PreviousSnapshotID || immediateDeallocation {
		deallocator.Deallocate(nodeAddress)

		return nil
	}

	l, exists := db.deallocationListsToCommit[nodeSnapshotID]
	if !exists {
		l = &ListToCommit{
			List: list.New(list.Config{
				State: db.config.State,
			}),
		}
		db.deallocationListsToCommit[nodeSnapshotID] = l
	}

	listRoot, err := l.List.Add(
		nodeAddress,
		allocator,
	)
	if err != nil {
		return err
	}

	l.Root = listRoot

	return nil
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
	}), nil
}

type commitTx struct {
	SyncCh <-chan struct{}
}

type deleteSnapshotTx struct {
	SnapshotID types.SnapshotID
}
