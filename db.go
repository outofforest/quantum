package quantum

import (
	"context"
	"fmt"
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
func New(config Config) *DB {
	queue, queueReader := pipeline.New()

	db := &DB{
		config:           config,
		txRequestFactory: pipeline.NewTransactionRequestFactory(),
		singularityNode:  photon.FromPointer[types.SingularityNode](config.State.Node(0)),
		queue:            queue,
		queueReader:      queueReader,
	}

	db.prepareNextSnapshot()
	return db
}

// DB represents the database.
type DB struct {
	config           Config
	txRequestFactory *pipeline.TransactionRequestFactory
	singularityNode  *types.SingularityNode
	snapshotInfo     types.SnapshotInfo

	spaceDeletionCounters [types.NumOfSpaces]uint64

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
	db.prepareNextSnapshot()

	return nil
}

// Close tells that there will be no more operations done.
func (db *DB) Close() {
	tx := db.txRequestFactory.New()
	tx.Type = pipeline.Close
	db.queue.Push(tx)
}

// Run runs db goroutines.
func (db *DB) Run(ctx context.Context) error {
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
		prevHashReader := executeTxReader
		dataHashReaders := make([]*pipeline.Reader, 0, 2)
		for range cap(dataHashReaders) {
			nextReader := pipeline.NewReader(prevHashReader)
			dataHashReaders = append(dataHashReaders, nextReader)
			prevHashReader = nextReader
		}
		pointerHashReader := pipeline.NewReader(prevHashReader)
		storeNodesReader := pipeline.NewReader(pointerHashReader)

		spawn("supervisor", parallel.Exit, func(ctx context.Context) error {
			var lastCommitCh chan<- error

			for {
				req, readCount, err := supervisorReader.Read(ctx)
				if err != nil && lastCommitCh != nil {
					lastCommitCh <- err
					lastCommitCh = nil
				}

				if req != nil {
					supervisorReader.Acknowledge(readCount, req.Type)

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
			spawn(fmt.Sprintf("pipe01PrepareTx-%02d", i), parallel.Fail, func(ctx context.Context) error {
				balanceSpace, err := GetSpace[txtypes.Account, txtypes.Amount](spaces.Balances, db)
				if err != nil {
					return err
				}

				return reader.Run(ctx, pipe01PrepareTransactions(balanceSpace))
			})
		}
		spawn("pipe02ExecuteTx", parallel.Fail, func(ctx context.Context) error {
			snapshotInfoNodeAssistant, err := space.NewDataNodeAssistant[types.SnapshotID, types.SnapshotInfo]()
			if err != nil {
				return err
			}

			snapshotSpace := space.New[types.SnapshotID, types.SnapshotInfo](space.Config[types.SnapshotID, types.SnapshotInfo]{
				SpaceRoot: types.NodeRoot{
					Pointer: &db.singularityNode.SnapshotRoot,
				},
				State:             db.config.State,
				DataNodeAssistant: snapshotInfoNodeAssistant,
				DeletionCounter:   lo.ToPtr[uint64](0),
				NoSnapshots:       true,
			})

			deallocationNodeAssistant, err := space.NewDataNodeAssistant[deallocationKey, types.ListRoot]()
			if err != nil {
				return err
			}

			deallocationListSpace := space.New[deallocationKey, types.ListRoot](
				space.Config[deallocationKey, types.ListRoot]{
					SpaceRoot: types.NodeRoot{
						Pointer: &db.snapshotInfo.DeallocationRoot,
					},
					State:             db.config.State,
					DataNodeAssistant: deallocationNodeAssistant,
					DeletionCounter:   lo.ToPtr[uint64](0),
					NoSnapshots:       true,
				},
			)

			balanceSpace, err := GetSpace[txtypes.Account, txtypes.Amount](spaces.Balances, db)
			if err != nil {
				return err
			}

			return executeTxReader.Run(ctx, pipe02ExecuteTransactions(db.config.State,
				&db.singularityNode.LastSnapshotID, &db.snapshotInfo, snapshotSpace, deallocationListSpace,
				deallocationNodeAssistant, balanceSpace))
		})
		for i, reader := range dataHashReaders {
			spawn(fmt.Sprintf("pipe03DataHash-%02d", i), parallel.Fail, func(ctx context.Context) error {
				return reader.Run(ctx, pipe03UpdateDataHashes(db.config.State, uint64(len(dataHashReaders)-1),
					uint64(i)))
			})
		}
		spawn("pipe04PointerHash", parallel.Fail, func(ctx context.Context) error {
			return pipe04UpdatePointerHashes(ctx, db.config.State, pointerHashReader)
		})
		spawn("pipe05StoreNodes", parallel.Fail, func(ctx context.Context) error {
			storeWriter, err := db.config.Store.NewWriter(db.config.State.Origin(),
				db.config.State.VolatileSize())
			if err != nil {
				return err
			}
			defer storeWriter.Close()

			return storeNodesReader.Run(ctx, pipe05StoreNodes(storeWriter))
		})

		return nil
	})
}

func (db *DB) prepareNextSnapshot() {
	db.singularityNode.LastSnapshotID++
	db.snapshotInfo.PreviousSnapshotID = db.singularityNode.LastSnapshotID - 1
	db.snapshotInfo.NextSnapshotID = db.singularityNode.LastSnapshotID + 1
	db.snapshotInfo.DeallocationRoot = types.Pointer{}
}

func pipe01PrepareTransactions(balanceSpace *space.Space[txtypes.Account, txtypes.Amount]) pipeline.TxFunc {
	return func(tx *pipeline.TransactionRequest, readCount uint64) (uint64, error) {
		if tx.Transaction == nil {
			return readCount, nil
		}

		if transferTx, ok := tx.Transaction.(*transfer.Tx); ok {
			transferTx.Prepare(balanceSpace)
		}

		return readCount, nil
	}
}

func pipe02ExecuteTransactions(
	state *alloc.State,
	snapshotID *types.SnapshotID,
	snapshotInfo *types.SnapshotInfo,
	snapshotSpace *space.Space[types.SnapshotID, types.SnapshotInfo],
	deallocationListSpace *space.Space[deallocationKey, types.ListRoot],
	deallocationNodeAssistant *space.DataNodeAssistant[deallocationKey, types.ListRoot],
	balanceSpace *space.Space[txtypes.Account, txtypes.Amount],
) pipeline.TxFunc {
	volatileAllocator := state.NewVolatileAllocator()
	volatileDeallocator := state.NewVolatileDeallocator()
	persistentAllocator := state.NewPersistentAllocator()
	persistentDeallocator := state.NewPersistentDeallocator()

	deallocationListsToCommit := map[types.SnapshotID]*types.ListRoot{}

	return func(tx *pipeline.TransactionRequest, readCount uint64) (uint64, error) {
		if tx.Transaction != nil {
			switch t := tx.Transaction.(type) {
			case *transfer.Tx:
				if err := t.Execute(balanceSpace, tx, volatileAllocator); err != nil {
					return 0, err
				}
			case *deleteSnapshotTx:
				if err := deleteSnapshot(*snapshotID, t.SnapshotID, state, tx, volatileAllocator,
					volatileDeallocator, persistentDeallocator, snapshotSpace, deallocationNodeAssistant); err != nil {
					return 0, err
				}
			case *genesis.Tx:
				if err := t.Execute(balanceSpace, tx, volatileAllocator); err != nil {
					return 0, err
				}
			default:
				return 0, errors.New("unknown transaction type")
			}
		}

		var lr *pipeline.ListRequest
		for sr := tx.StoreRequest; sr != nil; sr = sr.Next {
			if sr.PointersToStore == 0 {
				continue
			}

			for i := range sr.PointersToStore {
				root := sr.Store[i]
				root.Pointer.Revision = uintptr(unsafe.Pointer(sr))

				//nolint:nestif
				if root.Pointer.SnapshotID != *snapshotID {
					if root.Pointer.PersistentAddress != 0 {
						listNodePointer, err := deallocateNode(state, root.Pointer.SnapshotID, *snapshotID,
							root.Pointer.PersistentAddress, deallocationListsToCommit, volatileAllocator,
							persistentAllocator, persistentDeallocator, sr.NoSnapshots)
						if err != nil {
							return 0, err
						}

						if listNodePointer.VolatileAddress != types.FreeAddress {
							if lr == nil {
								lr = &pipeline.ListRequest{}
							}
							lr.List[lr.ListsToStore] = listNodePointer
							lr.ListsToStore++
							if lr.ListsToStore == pipeline.StoreCapacity {
								tx.AddListRequest(lr)
								lr = nil
							}
						}

						root.Pointer.PersistentAddress = 0
					}
					root.Pointer.SnapshotID = *snapshotID
				}

				// Singularity node might be stored under address 0 which could cause a mess here.
				// But it doesn't matter, because singularity node is added to the pipeline at later stage.
				if root.Pointer.PersistentAddress == 0 {
					persistentAddress, err := persistentAllocator.Allocate()
					if err != nil {
						return 0, err
					}
					root.Pointer.PersistentAddress = persistentAddress
				}
			}
		}
		if lr != nil {
			tx.AddListRequest(lr)
		}

		if tx.Type == pipeline.Commit {
			if err := commit(*snapshotID, *snapshotInfo, tx, state, deallocationListsToCommit, volatileAllocator,
				persistentAllocator, persistentDeallocator, snapshotSpace, deallocationListSpace); err != nil {
				return 0, err
			}
		}

		return readCount, nil
	}
}

func pipe03UpdateDataHashes(state *alloc.State, modMask, mod uint64) pipeline.TxFunc {
	var matrix [16]*byte
	matrixP := &matrix[0]

	var hashes [16]*byte
	hashesP := &hashes[0]

	var mask uint16
	var slotIndex int

	return func(tx *pipeline.TransactionRequest, readCount uint64) (uint64, error) {
		var ackCount uint64

		for sr := tx.StoreRequest; sr != nil; sr = sr.Next {
			if sr.PointersToStore == 0 || sr.NoSnapshots {
				continue
			}

			index := sr.PointersToStore - 1
			pointer := sr.Store[index].Pointer
			volatileAddress := sr.Store[index].VolatileAddress

			if uint64(volatileAddress)&modMask != mod || pointer.Revision != uintptr(unsafe.Pointer(sr)) {
				continue
			}

			mask |= 1 << slotIndex
			matrix[slotIndex] = (*byte)(state.Node(volatileAddress))
			hashes[slotIndex] = &sr.Store[index].Hash[0]

			slotIndex++
			if slotIndex == len(matrix) {
				hash.Blake34096(matrixP, hashesP, mask)

				slotIndex = 0
				mask = 0

				ackCount = readCount - 1
			}
		}

		if tx.Type == pipeline.Commit {
			if mask != 0 {
				hash.Blake34096(matrixP, hashesP, mask)

				slotIndex = 0
				mask = 0
			}

			ackCount = readCount
		}

		return ackCount, nil
	}
}

type reader struct {
	pipeReader *pipeline.Reader

	txRequest    *pipeline.TransactionRequest
	StoreRequest *pipeline.StoreRequest
	Count        uint64
}

func (r *reader) Read(ctx context.Context) error {
	if r.StoreRequest != nil {
		r.StoreRequest = r.StoreRequest.Next
	}

loop:
	for {
		for r.StoreRequest == nil {
			var err error
			r.txRequest, r.Count, err = r.pipeReader.Read(ctx)
			if err != nil {
				return err
			}

			r.StoreRequest = r.txRequest.StoreRequest
		}

		for r.StoreRequest.PointersToStore < 2 || r.StoreRequest.NoSnapshots {
			r.StoreRequest = r.StoreRequest.Next
			if r.StoreRequest == nil {
				if r.txRequest.Type != pipeline.Commit {
					continue loop
				}
				return nil
			}
		}

		return nil
	}
}

type slot struct {
	r *reader

	StoreRequest *pipeline.StoreRequest
	Count        uint64
	PointerIndex int8
}

func (s *slot) Read(ctx context.Context) error {
	if err := s.r.Read(ctx); err != nil {
		return err
	}
	s.StoreRequest = s.r.StoreRequest
	s.Count = s.r.Count
	if s.StoreRequest == nil {
		s.PointerIndex = 0
	} else {
		s.PointerIndex = s.StoreRequest.PointersToStore - 1 // -1 to skip the data node
	}

	return nil
}

func pipe04UpdatePointerHashes(
	ctx context.Context,
	state *alloc.State,
	pipeReader *pipeline.Reader,
) error {
	r := &reader{
		pipeReader: pipeReader,
	}

	var slots [16]*slot
	for i := range slots {
		slots[i] = &slot{r: r}
	}

	var commitSlot *slot

	var matrix [16]*byte
	matrixP := &matrix[0]

	var hashes [16]*byte
	hashesP := &hashes[0]

	n2 := 0
	for {
		n := n2
		n2 = 0
		for i, s := range slots[:n] {
			if s.PointerIndex == 0 {
				continue
			}

			s.PointerIndex--
			if s.StoreRequest.Store[s.PointerIndex].Pointer.Revision != uintptr(unsafe.Pointer(s.StoreRequest)) {
				// Pointers are processed from the data node up t the root node. If at any level
				// revision test fails, it doesn't make sense to process parent nodes because revision
				// test will fail there for sure.
				continue
			}

			slots[n2], slots[i] = s, slots[n2]
			n2++
		}

	riLoop:
		for _, s := range slots[n2:] {
			for {
				if err := s.Read(ctx); err != nil {
					return err
				}

				if s.StoreRequest == nil {
					commitSlot = s
					break riLoop
				}

				s.PointerIndex--
				if s.StoreRequest.Store[s.PointerIndex].Pointer.Revision != uintptr(unsafe.Pointer(s.StoreRequest)) {
					// Pointers are processed from the data node up to the root node. If at any level
					// revision test fails, it doesn't make sense to process parent nodes because revision
					// test will fail there for sure.
					continue
				}

				n2++

				break
			}
		}

		var minSlot *slot
		if n2 > 0 {
			minSlot = slots[0]
			var mask uint16

			for ri, r := range slots[:n2] {
				if r.Count < minSlot.Count {
					minSlot = r
				}

				mask |= 1 << ri
				matrix[ri] = (*byte)(state.Node(r.StoreRequest.Store[r.PointerIndex].VolatileAddress))
				hashes[ri] = &r.StoreRequest.Store[r.PointerIndex].Hash[0]
			}

			hash.Blake32048(matrixP, hashesP, mask)
		}
		if commitSlot == nil {
			pipeReader.Acknowledge(minSlot.Count-1, pipeline.None)
		} else {
			pipeReader.Acknowledge(commitSlot.Count, pipeline.Commit)
			commitSlot = nil
			n2 = 0
		}
	}
}

func pipe05StoreNodes(storeWriter *persistent.Writer) pipeline.TxFunc {
	return func(tx *pipeline.TransactionRequest, readCount uint64) (uint64, error) {
		for lr := tx.ListRequest; lr != nil; lr = lr.Next {
			for i := range lr.ListsToStore {
				if err := storeWriter.Write(lr.List[i].PersistentAddress, lr.List[i].VolatileAddress); err != nil {
					return 0, err
				}
			}
		}

		for sr := tx.StoreRequest; sr != nil; sr = sr.Next {
			for i := sr.PointersToStore - 1; i >= 0; i-- {
				pointer := sr.Store[i].Pointer

				if pointer.Revision == uintptr(unsafe.Pointer(sr)) {
					if err := storeWriter.Write(pointer.PersistentAddress, sr.Store[i].VolatileAddress); err != nil {
						return 0, err
					}
				}
			}
		}

		if tx.Type == pipeline.Commit {
			tx.CommitCh <- nil
		}

		return readCount, nil
	}
}

func deleteSnapshot(
	snapshotID types.SnapshotID,
	deleteSnapshotID types.SnapshotID,
	state *alloc.State,
	tx *pipeline.TransactionRequest,
	volatileAllocator *alloc.Allocator[types.VolatileAddress],
	volatileDeallocator *alloc.Deallocator[types.VolatileAddress],
	persistentDeallocator *alloc.Deallocator[types.PersistentAddress],
	snapshotSpace *space.Space[types.SnapshotID, types.SnapshotInfo],
	deallocationNodeAssistant *space.DataNodeAssistant[deallocationKey, types.ListRoot],
) error {
	if deleteSnapshotID == snapshotID-1 {
		return errors.New("deleting latest persistent snapshot is forbidden")
	}

	var snapshotInfoValue space.Entry[types.SnapshotID, types.SnapshotInfo]
	snapshotSpace.Find(&snapshotInfoValue, deleteSnapshotID, space.StageData)

	if exists := snapshotSpace.KeyExists(&snapshotInfoValue); !exists {
		return errors.Errorf("snapshot %d to delete does not exist", deleteSnapshotID)
	}
	snapshotInfo := snapshotSpace.ReadKey(&snapshotInfoValue)

	snapshotSpace.DeleteKey(&snapshotInfoValue, tx)

	var nextSnapshotInfoValue space.Entry[types.SnapshotID, types.SnapshotInfo]
	snapshotSpace.Find(&nextSnapshotInfoValue, snapshotInfo.NextSnapshotID, space.StageData)

	if exists := snapshotSpace.KeyExists(&nextSnapshotInfoValue); !exists {
		return errors.Errorf("next snapshot %d does not exist", deleteSnapshotID)
	}
	nextSnapshotInfo := snapshotSpace.ReadKey(&nextSnapshotInfoValue)

	deallocationListsRoot := types.NodeRoot{
		Pointer: &snapshotInfo.DeallocationRoot,
	}

	deallocationLists := space.New[deallocationKey, types.ListRoot](
		space.Config[deallocationKey, types.ListRoot]{
			SpaceRoot:         deallocationListsRoot,
			State:             state,
			DataNodeAssistant: deallocationNodeAssistant,
			DeletionCounter:   lo.ToPtr[uint64](0),
			NoSnapshots:       true,
		},
	)

	for nextDeallocSnapshot := range space.IteratorAndDeallocator(
		nextSnapshotInfo.DeallocationRoot,
		state,
		deallocationNodeAssistant,
		volatileDeallocator,
		persistentDeallocator,
	) {
		if nextDeallocSnapshot.Key.SnapshotID > snapshotInfo.PreviousSnapshotID &&
			nextDeallocSnapshot.Key.SnapshotID <= deleteSnapshotID {
			if err := list.Deallocate(nextDeallocSnapshot.Value, state, volatileDeallocator,
				persistentDeallocator); err != nil {
				return err
			}

			continue
		}

		var deallocationListValue space.Entry[deallocationKey, types.ListRoot]
		deallocationLists.Find(&deallocationListValue, nextDeallocSnapshot.Key, space.StageData)
		if err := deallocationLists.SetKey(&deallocationListValue, tx, volatileAllocator,
			nextDeallocSnapshot.Value); err != nil {
			return err
		}
	}

	nextSnapshotInfo.DeallocationRoot = snapshotInfo.DeallocationRoot
	nextSnapshotInfo.PreviousSnapshotID = snapshotInfo.PreviousSnapshotID
	if err := snapshotSpace.SetKey(&nextSnapshotInfoValue, tx, volatileAllocator, nextSnapshotInfo); err != nil {
		return err
	}

	if snapshotInfo.PreviousSnapshotID > 0 {
		var previousSnapshotInfoValue space.Entry[types.SnapshotID, types.SnapshotInfo]
		snapshotSpace.Find(&previousSnapshotInfoValue, snapshotInfo.PreviousSnapshotID, space.StageData)

		if exists := snapshotSpace.KeyExists(&previousSnapshotInfoValue); !exists {
			return errors.Errorf("previous snapshot %d does not exist", deleteSnapshotID)
		}

		previousSnapshotInfo := snapshotSpace.ReadKey(&previousSnapshotInfoValue)
		previousSnapshotInfo.NextSnapshotID = snapshotInfo.NextSnapshotID

		if err := snapshotSpace.SetKey(&previousSnapshotInfoValue, tx, volatileAllocator, previousSnapshotInfo); err != nil {
			return err
		}
	}

	return nil
}

func commit(
	snapshotID types.SnapshotID,
	snapshotInfo types.SnapshotInfo,
	tx *pipeline.TransactionRequest,
	state *alloc.State,
	deallocationListsToCommit map[types.SnapshotID]*types.ListRoot,
	volatileAllocator *alloc.Allocator[types.VolatileAddress],
	persistentAllocator *alloc.Allocator[types.PersistentAddress],
	persistentDeallocator *alloc.Deallocator[types.PersistentAddress],
	snapshotSpace *space.Space[types.SnapshotID, types.SnapshotInfo],
	deallocationListSpace *space.Space[deallocationKey, types.ListRoot],
) error {
	// Deallocations done in this function are done after standard deallocation are processed by the transaction
	// execution goroutine. Deallocations done here don't go to deallocation lists but are deallocated immediately.

	//nolint:nestif
	if len(deallocationListsToCommit) > 0 {
		lastSr := tx.LastStoreRequest

		lists := make([]types.SnapshotID, 0, len(deallocationListsToCommit))
		for deallocSnapshotID := range deallocationListsToCommit {
			lists = append(lists, deallocSnapshotID)
		}
		sort.Slice(lists, func(i, j int) bool { return lists[i] < lists[j] })

		var lr *pipeline.ListRequest
		for _, deallocSnapshotID := range lists {
			listRoot := deallocationListsToCommit[deallocSnapshotID]

			var deallocationListValue space.Entry[deallocationKey, types.ListRoot]
			deallocationListSpace.Find(&deallocationListValue, deallocationKey{
				ListSnapshotID: snapshotID,
				SnapshotID:     deallocSnapshotID,
			}, space.StageData)
			if err := deallocationListSpace.SetKey(&deallocationListValue, tx, volatileAllocator, *listRoot); err != nil {
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

		clear(deallocationListsToCommit)

		for sr := *lastSr; sr != nil; sr = sr.Next {
			for i := range sr.PointersToStore {
				if sr.Store[i].Pointer.SnapshotID == 0 {
					persistentAddr, err := persistentAllocator.Allocate()
					if err != nil {
						return err
					}

					sr.Store[i].Pointer.SnapshotID = snapshotID
					sr.Store[i].Pointer.PersistentAddress = persistentAddr
				}
				sr.Store[i].Pointer.Revision = uintptr(unsafe.Pointer(sr))
			}
		}
	}

	lastSr := tx.LastStoreRequest

	var nextSnapshotInfoValue space.Entry[types.SnapshotID, types.SnapshotInfo]
	snapshotSpace.Find(&nextSnapshotInfoValue, snapshotID, space.StageData)
	if err := snapshotSpace.SetKey(&nextSnapshotInfoValue, tx, volatileAllocator, snapshotInfo); err != nil {
		return err
	}

	for sr := *lastSr; sr != nil; sr = sr.Next {
		for i := range sr.PointersToStore {
			if sr.Store[i].Pointer.SnapshotID != snapshotID {
				if sr.Store[i].Pointer.PersistentAddress != 0 {
					persistentDeallocator.Deallocate(sr.Store[i].Pointer.PersistentAddress)
				}
				persistentAddr, err := persistentAllocator.Allocate()
				if err != nil {
					return err
				}

				sr.Store[i].Pointer.SnapshotID = snapshotID
				sr.Store[i].Pointer.PersistentAddress = persistentAddr
			}
			sr.Store[i].Pointer.Revision = uintptr(unsafe.Pointer(sr))
		}
	}

	sr := &pipeline.StoreRequest{
		PointersToStore: 1,
		NoSnapshots:     true,
	}
	sr.Store[0] = state.SingularityNodeRoot(snapshotID)
	sr.Store[0].Pointer.Revision = uintptr(unsafe.Pointer(sr))
	tx.AddStoreRequest(sr)

	return nil
}

func deallocateNode(
	state *alloc.State,
	snapshotID types.SnapshotID,
	nodeSnapshotID types.SnapshotID,
	nodeAddress types.PersistentAddress,
	deallocationListsToCommit map[types.SnapshotID]*types.ListRoot,
	volatileAllocator *alloc.Allocator[types.VolatileAddress],
	persistentAllocator *alloc.Allocator[types.PersistentAddress],
	persistentDeallocator *alloc.Deallocator[types.PersistentAddress],
	immediateDeallocation bool,
) (types.ListRoot, error) {
	// Latest persistent snapshot cannot be deleted, so there is no gap between that snapshot and the pending one.
	// It means the condition here don't need to include snapshot IDs greater than the previous snapshot ID.
	if nodeSnapshotID == snapshotID || immediateDeallocation {
		persistentDeallocator.Deallocate(nodeAddress)

		return types.ListRoot{}, nil
	}

	listRoot := deallocationListsToCommit[nodeSnapshotID]
	if listRoot == nil {
		listRoot = &types.ListRoot{}
		deallocationListsToCommit[nodeSnapshotID] = listRoot
	}

	return list.Add(listRoot, nodeAddress, state, volatileAllocator, persistentAllocator)
}

// GetSpace retrieves space from snapshot.
func GetSpace[K, V comparable](spaceID types.SpaceID, db *DB) (*space.Space[K, V], error) {
	if spaceID >= types.NumOfSpaces {
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
