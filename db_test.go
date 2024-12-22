package quantum

import (
	"testing"
	"unsafe"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/outofforest/quantum/list"
	"github.com/outofforest/quantum/pipeline"
	"github.com/outofforest/quantum/space"
	"github.com/outofforest/quantum/state"
	txtypes "github.com/outofforest/quantum/tx/types"
	"github.com/outofforest/quantum/types"
)

const stateSize = 100 * types.NodeLength

func TestImmediateDeallocation(t *testing.T) {
	requireT := require.New(t)

	appState := state.NewForTest(t, 10*types.NodeLength)
	volatileAllocator := appState.NewVolatileAllocator()
	persistentAllocator := appState.NewPersistentAllocator()
	persistentDeallocator := appState.NewPersistentDeallocator()
	deallocationListsToCommit := map[types.SnapshotID]*types.ListRoot{}

	persistentAddress, err := persistentAllocator.Allocate()
	requireT.NoError(err)

	for {
		if _, err := persistentAllocator.Allocate(); err != nil {
			break
		}
	}

	listRoot, err := deallocateNode(appState, 10, 1, persistentAddress,
		deallocationListsToCommit, volatileAllocator, persistentAllocator, persistentDeallocator, true)
	requireT.NoError(err)
	requireT.Zero(listRoot.VolatileAddress)
	requireT.Zero(listRoot.PersistentAddress)
	requireT.Empty(deallocationListsToCommit)

	persistentDeallocator.Deallocate(0x00)
	appState.Commit()

	_, err = persistentAllocator.Allocate()
	requireT.NoError(err)

	deallocatedPersistentAddress, err := persistentAllocator.Allocate()
	requireT.NoError(err)

	requireT.Equal(persistentAddress, deallocatedPersistentAddress)
}

func TestSameSnapshotDeallocation(t *testing.T) {
	requireT := require.New(t)

	appState := state.NewForTest(t, 10*types.NodeLength)
	volatileAllocator := appState.NewVolatileAllocator()
	persistentAllocator := appState.NewPersistentAllocator()
	persistentDeallocator := appState.NewPersistentDeallocator()
	deallocationListsToCommit := map[types.SnapshotID]*types.ListRoot{}

	persistentAddress, err := persistentAllocator.Allocate()
	requireT.NoError(err)

	for {
		if _, err := persistentAllocator.Allocate(); err != nil {
			break
		}
	}

	listRoot, err := deallocateNode(appState, 10, 10, persistentAddress,
		deallocationListsToCommit, volatileAllocator, persistentAllocator, persistentDeallocator, false)
	requireT.NoError(err)
	requireT.Zero(listRoot.VolatileAddress)
	requireT.Zero(listRoot.PersistentAddress)
	requireT.Empty(deallocationListsToCommit)

	persistentDeallocator.Deallocate(0x00)
	appState.Commit()

	_, err = persistentAllocator.Allocate()
	requireT.NoError(err)

	deallocatedPersistentAddress, err := persistentAllocator.Allocate()
	requireT.NoError(err)

	requireT.Equal(persistentAddress, deallocatedPersistentAddress)
}

func TestDelayedDeallocation(t *testing.T) {
	requireT := require.New(t)

	const (
		nodeSnapshotID1 = 1
		nodeSnapshotID2 = 2
	)

	appState := state.NewForTest(t, 10*types.NodeLength)
	volatileAllocator := appState.NewVolatileAllocator()
	volatileDeallocator := appState.NewVolatileDeallocator()
	persistentAllocator := appState.NewPersistentAllocator()
	persistentDeallocator := appState.NewPersistentDeallocator()
	deallocationListsToCommit := map[types.SnapshotID]*types.ListRoot{}

	persistentAddress1, err := persistentAllocator.Allocate()
	requireT.NoError(err)
	persistentAddress2, err := persistentAllocator.Allocate()
	requireT.NoError(err)
	persistentAddress3, err := persistentAllocator.Allocate()
	requireT.NoError(err)

	listRoot, err := deallocateNode(appState, 10, nodeSnapshotID1, persistentAddress1,
		deallocationListsToCommit, volatileAllocator, persistentAllocator, persistentDeallocator, false)
	requireT.NoError(err)
	requireT.Zero(listRoot.VolatileAddress)
	requireT.Zero(listRoot.PersistentAddress)
	requireT.Len(deallocationListsToCommit, 1)
	requireT.NotNil(deallocationListsToCommit[nodeSnapshotID1])
	requireT.NotZero(deallocationListsToCommit[nodeSnapshotID1].VolatileAddress)
	requireT.NotZero(deallocationListsToCommit[nodeSnapshotID1].PersistentAddress)

	previousListRoot := *deallocationListsToCommit[nodeSnapshotID1]

	listRoot, err = deallocateNode(appState, 10, nodeSnapshotID1, persistentAddress2,
		deallocationListsToCommit, volatileAllocator, persistentAllocator, persistentDeallocator, false)
	requireT.NoError(err)
	requireT.Zero(listRoot.VolatileAddress)
	requireT.Zero(listRoot.PersistentAddress)
	requireT.Len(deallocationListsToCommit, 1)
	requireT.NotNil(deallocationListsToCommit[nodeSnapshotID1])
	requireT.Equal(previousListRoot, *deallocationListsToCommit[nodeSnapshotID1])

	listRoot, err = deallocateNode(appState, 10, nodeSnapshotID2, persistentAddress3,
		deallocationListsToCommit, volatileAllocator, persistentAllocator, persistentDeallocator, false)
	requireT.NoError(err)
	requireT.Zero(listRoot.VolatileAddress)
	requireT.Zero(listRoot.PersistentAddress)
	requireT.Len(deallocationListsToCommit, 2)
	requireT.NotNil(deallocationListsToCommit[nodeSnapshotID2])
	requireT.NotEqual(previousListRoot, *deallocationListsToCommit[nodeSnapshotID2])

	for {
		if _, err := persistentAllocator.Allocate(); err != nil {
			break
		}
	}

	persistentDeallocator.Deallocate(0x00)
	appState.Commit()

	_, err = persistentAllocator.Allocate()
	requireT.NoError(err)

	_, err = persistentAllocator.Allocate()
	requireT.Error(err)

	requireT.NoError(list.Deallocate(*deallocationListsToCommit[nodeSnapshotID1], appState, volatileDeallocator,
		persistentDeallocator))

	persistentDeallocator.Deallocate(0x00)
	appState.Commit()

	_, err = persistentAllocator.Allocate()
	requireT.NoError(err)

	listRootPersistentAddress, err := persistentAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(listRootPersistentAddress, deallocationListsToCommit[nodeSnapshotID1].PersistentAddress)

	deallocatedPersistentAddress1, err := persistentAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(persistentAddress1, deallocatedPersistentAddress1)

	deallocatedPersistentAddress2, err := persistentAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(persistentAddress2, deallocatedPersistentAddress2)

	requireT.NoError(list.Deallocate(*deallocationListsToCommit[nodeSnapshotID2], appState, volatileDeallocator,
		persistentDeallocator))

	persistentDeallocator.Deallocate(0x00)
	appState.Commit()

	_, err = persistentAllocator.Allocate()
	requireT.NoError(err)

	listRootPersistentAddress, err = persistentAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(listRootPersistentAddress, deallocationListsToCommit[nodeSnapshotID2].PersistentAddress)

	deallocatedPersistentAddress3, err := persistentAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(persistentAddress3, deallocatedPersistentAddress3)
}

func TestCommitWithoutDeallocationLists(t *testing.T) {
	requireT := require.New(t)

	const (
		snapshotID1 types.SnapshotID = 1
		snapshotID2 types.SnapshotID = 2
	)

	appState := state.NewForTest(t, 10*types.NodeLength)
	txFactory := pipeline.NewTransactionRequestFactory()
	volatileAllocator := appState.NewVolatileAllocator()
	persistentAllocator := appState.NewPersistentAllocator()
	persistentDeallocator := appState.NewPersistentDeallocator()
	deallocationListsToCommit := map[types.SnapshotID]*types.ListRoot{}

	snapshotInfoNodeAssistant, err := space.NewDataNodeAssistant[types.SnapshotID, types.SnapshotInfo]()
	requireT.NoError(err)

	var snapshotRoot, deallocationRoot types.Pointer
	snapshotInfo1 := types.SnapshotInfo{
		PreviousSnapshotID: 0,
		NextSnapshotID:     snapshotID2,
	}
	snapshotInfo2 := types.SnapshotInfo{
		PreviousSnapshotID: snapshotID1,
		NextSnapshotID:     3,
	}

	snapshotSpace := space.New[types.SnapshotID, types.SnapshotInfo](space.Config[types.SnapshotID, types.SnapshotInfo]{
		SpaceRoot: types.NodeRoot{
			Pointer: &snapshotRoot,
		},
		State:             appState,
		DataNodeAssistant: snapshotInfoNodeAssistant,
		DeletionCounter:   lo.ToPtr[uint64](0),
		NoSnapshots:       true,
	})

	deallocationNodeAssistant, err := space.NewDataNodeAssistant[deallocationKey, types.ListRoot]()
	requireT.NoError(err)

	deallocationListSpace := space.New[deallocationKey, types.ListRoot](
		space.Config[deallocationKey, types.ListRoot]{
			SpaceRoot: types.NodeRoot{
				Pointer: &deallocationRoot,
			},
			State:             appState,
			DataNodeAssistant: deallocationNodeAssistant,
			DeletionCounter:   lo.ToPtr[uint64](0),
			NoSnapshots:       true,
		},
	)

	tx := txFactory.New()
	requireT.NoError(commit(snapshotID1, snapshotInfo1, tx, appState, deallocationListsToCommit, volatileAllocator,
		persistentAllocator, persistentDeallocator, snapshotSpace, deallocationListSpace))
	requireT.Zero(deallocationRoot)
	requireT.NotZero(snapshotRoot.VolatileAddress)
	requireT.NotZero(snapshotRoot.PersistentAddress)
	requireT.NotZero(snapshotRoot.SnapshotID)
	requireT.NotZero(snapshotRoot.Revision)
	sInfo1, exists := snapshotSpace.Query(snapshotID1)
	requireT.True(exists)
	requireT.Equal(snapshotInfo1, sInfo1)
	requireT.Nil(tx.ListRequest)
	requireT.Nil(*tx.LastListRequest)
	requireT.Empty(deallocationListsToCommit)

	// snapshot space

	sr := tx.StoreRequest
	requireT.NotNil(sr)
	requireT.True(sr.NoSnapshots)
	requireT.Equal(int8(1), sr.PointersToStore)
	requireT.Nil(sr.Store[0].Hash)
	requireT.NotZero(sr.Store[0].VolatileAddress)
	requireT.NotZero(sr.Store[0].Pointer.VolatileAddress)
	requireT.NotZero(sr.Store[0].Pointer.PersistentAddress)
	requireT.Equal(snapshotRoot, *sr.Store[0].Pointer)
	requireT.Equal(snapshotRoot.VolatileAddress, sr.Store[0].VolatileAddress)
	requireT.Equal(uintptr(unsafe.Pointer(sr)), sr.Store[0].Pointer.Revision)
	requireT.Equal(snapshotID1, sr.Store[0].Pointer.SnapshotID)

	// commit

	sr = sr.Next
	requireT.NotNil(sr)
	requireT.Nil(sr.Next)
	requireT.True(sr.NoSnapshots)
	requireT.Equal(int8(1), sr.PointersToStore)
	requireT.Nil(sr.Store[0].Hash)
	requireT.Zero(sr.Store[0].VolatileAddress)
	requireT.Zero(sr.Store[0].Pointer.VolatileAddress)
	requireT.Equal(types.PersistentAddress(2), sr.Store[0].Pointer.PersistentAddress)
	requireT.Equal(uintptr(unsafe.Pointer(sr)), sr.Store[0].Pointer.Revision)
	requireT.Equal(snapshotID1, sr.Store[0].Pointer.SnapshotID)

	previousSnapshotRoot := snapshotRoot

	tx = txFactory.New()
	requireT.NoError(commit(snapshotID2, snapshotInfo2, tx, appState, deallocationListsToCommit, volatileAllocator,
		persistentAllocator, persistentDeallocator, snapshotSpace, deallocationListSpace))
	requireT.Zero(deallocationRoot)
	requireT.NotZero(snapshotRoot.VolatileAddress)
	requireT.NotZero(snapshotRoot.PersistentAddress)
	requireT.NotZero(snapshotRoot.SnapshotID)
	requireT.NotZero(snapshotRoot.Revision)
	requireT.Equal(previousSnapshotRoot.VolatileAddress, snapshotRoot.VolatileAddress)
	requireT.NotEqual(previousSnapshotRoot.PersistentAddress, snapshotRoot.PersistentAddress)
	sInfo1, exists = snapshotSpace.Query(snapshotID1)
	requireT.True(exists)
	requireT.Equal(snapshotInfo1, sInfo1)
	sInfo2, exists := snapshotSpace.Query(snapshotID2)
	requireT.True(exists)
	requireT.Equal(snapshotInfo2, sInfo2)
	requireT.Nil(tx.ListRequest)
	requireT.Nil(*tx.LastListRequest)
	requireT.Empty(deallocationListsToCommit)

	// snapshot space

	sr = tx.StoreRequest
	requireT.NotNil(sr)
	requireT.True(sr.NoSnapshots)
	requireT.Equal(int8(1), sr.PointersToStore)
	requireT.Nil(sr.Store[0].Hash)
	requireT.NotZero(sr.Store[0].VolatileAddress)
	requireT.NotZero(sr.Store[0].Pointer.VolatileAddress)
	requireT.NotZero(sr.Store[0].Pointer.PersistentAddress)
	requireT.Equal(snapshotRoot, *sr.Store[0].Pointer)
	requireT.Equal(snapshotRoot.VolatileAddress, sr.Store[0].VolatileAddress)
	requireT.Equal(uintptr(unsafe.Pointer(sr)), sr.Store[0].Pointer.Revision)
	requireT.Equal(snapshotID2, sr.Store[0].Pointer.SnapshotID)

	// commit

	sr = sr.Next
	requireT.NotNil(sr)
	requireT.Nil(sr.Next)
	requireT.True(sr.NoSnapshots)
	requireT.Equal(int8(1), sr.PointersToStore)
	requireT.Nil(sr.Store[0].Hash)
	requireT.Zero(sr.Store[0].VolatileAddress)
	requireT.Zero(sr.Store[0].Pointer.VolatileAddress)
	requireT.Equal(types.PersistentAddress(4), sr.Store[0].Pointer.PersistentAddress)
	requireT.Equal(uintptr(unsafe.Pointer(sr)), sr.Store[0].Pointer.Revision)
	requireT.Equal(snapshotID2, sr.Store[0].Pointer.SnapshotID)

	persistentDeallocator.Deallocate(0x00)
	appState.Commit()

	var oldSnapshotRootFound bool
	for {
		persistentAddress, err := persistentAllocator.Allocate()
		if err != nil {
			break
		}
		requireT.NotZero(persistentAddress)
		requireT.NotEqual(snapshotRoot.PersistentAddress, persistentAddress)
		if persistentAddress == previousSnapshotRoot.PersistentAddress {
			requireT.False(oldSnapshotRootFound)
			oldSnapshotRootFound = true
		}
	}
	requireT.True(oldSnapshotRootFound)
}

func TestCommitWithDeallocationLists(t *testing.T) {
	requireT := require.New(t)

	const (
		snapshotID             types.SnapshotID = 1
		numOfDeallocationLists                  = 5*pipeline.StoreCapacity - 1
	)

	appState := state.NewForTest(t, 10*types.NodeLength)
	txFactory := pipeline.NewTransactionRequestFactory()
	volatileAllocator := appState.NewVolatileAllocator()
	persistentAllocator := appState.NewPersistentAllocator()
	persistentDeallocator := appState.NewPersistentDeallocator()

	snapshotInfoNodeAssistant, err := space.NewDataNodeAssistant[types.SnapshotID, types.SnapshotInfo]()
	requireT.NoError(err)

	var snapshotRoot, deallocationRoot types.Pointer
	snapshotInfo := types.SnapshotInfo{
		PreviousSnapshotID: 0,
		NextSnapshotID:     2,
	}

	deallocationListsToCommit := map[types.SnapshotID]*types.ListRoot{}
	for i := range types.SnapshotID(numOfDeallocationLists) {
		deallocationListsToCommit[i] = &types.ListRoot{
			VolatileAddress:   types.VolatileAddress(i),
			PersistentAddress: types.PersistentAddress(i),
		}
	}

	snapshotSpace := space.New[types.SnapshotID, types.SnapshotInfo](space.Config[types.SnapshotID, types.SnapshotInfo]{
		SpaceRoot: types.NodeRoot{
			Pointer: &snapshotRoot,
		},
		State:             appState,
		DataNodeAssistant: snapshotInfoNodeAssistant,
		DeletionCounter:   lo.ToPtr[uint64](0),
		NoSnapshots:       true,
	})

	deallocationNodeAssistant, err := space.NewDataNodeAssistant[deallocationKey, types.ListRoot]()
	requireT.NoError(err)

	deallocationListSpace := space.New[deallocationKey, types.ListRoot](
		space.Config[deallocationKey, types.ListRoot]{
			SpaceRoot: types.NodeRoot{
				Pointer: &deallocationRoot,
			},
			State:             appState,
			DataNodeAssistant: deallocationNodeAssistant,
			DeletionCounter:   lo.ToPtr[uint64](0),
			NoSnapshots:       true,
		},
	)

	tx := txFactory.New()
	requireT.NoError(commit(snapshotID, snapshotInfo, tx, appState, deallocationListsToCommit, volatileAllocator,
		persistentAllocator, persistentDeallocator, snapshotSpace, deallocationListSpace))
	requireT.NotZero(deallocationRoot.VolatileAddress)
	requireT.NotZero(deallocationRoot.PersistentAddress)
	requireT.Equal(snapshotID, deallocationRoot.SnapshotID)
	//	requireT.Equal(uintptr(unsafe.Pointer(tx.StoreRequest)), deallocationRoot.Revision)
	requireT.NotNil(tx.ListRequest)
	requireT.Nil(*tx.LastListRequest)
	requireT.Empty(deallocationListsToCommit)

	// deallocation space

	sr := tx.StoreRequest
	for i := range numOfDeallocationLists {
		listRoot, exists := deallocationListSpace.Query(deallocationKey{
			ListSnapshotID: snapshotID,
			SnapshotID:     types.SnapshotID(i),
		})
		requireT.True(exists)
		requireT.Equal(types.VolatileAddress(i), listRoot.VolatileAddress)
		requireT.Equal(types.PersistentAddress(i), listRoot.PersistentAddress)

		requireT.NotNil(sr)
		requireT.True(sr.NoSnapshots)
		requireT.Equal(int8(1), sr.PointersToStore)
		requireT.Nil(sr.Store[0].Hash)
		requireT.NotZero(sr.Store[0].VolatileAddress)
		requireT.NotZero(sr.Store[0].Pointer.VolatileAddress)
		requireT.NotZero(sr.Store[0].Pointer.PersistentAddress)
		requireT.Equal(deallocationRoot, *sr.Store[0].Pointer)
		requireT.Equal(deallocationRoot.VolatileAddress, sr.Store[0].VolatileAddress)
		requireT.Equal(snapshotID, sr.Store[0].Pointer.SnapshotID)
		if i == numOfDeallocationLists-1 {
			requireT.Equal(uintptr(unsafe.Pointer(sr)), sr.Store[0].Pointer.Revision)
		}
		sr = sr.Next
	}

	// snapshot space

	requireT.NotNil(sr)
	requireT.True(sr.NoSnapshots)
	requireT.Equal(int8(1), sr.PointersToStore)
	requireT.Nil(sr.Store[0].Hash)
	requireT.NotZero(sr.Store[0].VolatileAddress)
	requireT.NotZero(sr.Store[0].Pointer.VolatileAddress)
	requireT.NotZero(sr.Store[0].Pointer.PersistentAddress)
	requireT.Equal(snapshotRoot, *sr.Store[0].Pointer)
	requireT.Equal(snapshotRoot.VolatileAddress, sr.Store[0].VolatileAddress)
	requireT.Equal(uintptr(unsafe.Pointer(sr)), sr.Store[0].Pointer.Revision)
	requireT.Equal(snapshotID, sr.Store[0].Pointer.SnapshotID)

	// commit

	sr = sr.Next
	requireT.NotNil(sr)
	requireT.Nil(sr.Next)
	requireT.True(sr.NoSnapshots)
	requireT.Equal(int8(1), sr.PointersToStore)
	requireT.Nil(sr.Store[0].Hash)
	requireT.Zero(sr.Store[0].VolatileAddress)
	requireT.Zero(sr.Store[0].Pointer.VolatileAddress)
	requireT.Equal(types.PersistentAddress(2), sr.Store[0].Pointer.PersistentAddress)
	requireT.Equal(uintptr(unsafe.Pointer(sr)), sr.Store[0].Pointer.Revision)
	requireT.Equal(snapshotID, sr.Store[0].Pointer.SnapshotID)

	var listRootIndex uint64
	for lr := tx.ListRequest; lr != nil; lr = lr.Next {
		if lr.Next == nil {
			requireT.Equal(int8(pipeline.StoreCapacity-1), lr.ListsToStore)
		} else {
			requireT.Equal(int8(pipeline.StoreCapacity), lr.ListsToStore)
		}
		for i := range lr.ListsToStore {
			listRoot := lr.List[i]
			requireT.Equal(types.VolatileAddress(listRootIndex), listRoot.VolatileAddress)
			requireT.Equal(types.PersistentAddress(listRootIndex), listRoot.PersistentAddress)
			listRootIndex++
		}
	}
	requireT.Equal(uint64(numOfDeallocationLists), listRootIndex)
}

func TestPipe01PrepareTransactionsDoesNothingIfTransactionIsNil(t *testing.T) {
	requireT := require.New(t)

	s := newSpace(t)
	txFactory := pipeline.NewTransactionRequestFactory()

	txFunc := pipe01PrepareTransaction(s)
	tx := txFactory.New()

	const readCount uint64 = 10
	rCount, err := txFunc(tx, readCount)
	requireT.NoError(err)
	requireT.Equal(readCount, rCount)

	requireT.Nil(tx.Transaction)
	requireT.Nil(tx.StoreRequest)
	requireT.Nil(tx.ListRequest)
}

func newSpace(t *testing.T) *space.Space[txtypes.Account, txtypes.Amount] {
	appState := state.NewForTest(t, stateSize)

	dataNodeAssistant, err := space.NewDataNodeAssistant[txtypes.Account, txtypes.Amount]()
	require.NoError(t, err)

	return space.New[txtypes.Account, txtypes.Amount](space.Config[txtypes.Account, txtypes.Amount]{
		SpaceRoot: types.NodeRoot{
			Pointer: &types.Pointer{},
			Hash:    &types.Hash{},
		},
		State:             appState,
		DataNodeAssistant: dataNodeAssistant,
		DeletionCounter:   lo.ToPtr[uint64](0),
		NoSnapshots:       false,
	})
}
