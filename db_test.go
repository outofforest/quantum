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

func TestDeletingLatestSnapshotFails(t *testing.T) {
	requireT := require.New(t)

	const (
		snapshotID       types.SnapshotID = 10
		deleteSnapshotID types.SnapshotID = 9
	)

	appState := state.NewForTest(t, 10*types.NodeLength)
	txFactory := pipeline.NewTransactionRequestFactory()
	volatileAllocator := appState.NewVolatileAllocator()
	volatileDeallocator := appState.NewVolatileDeallocator()
	persistentDeallocator := appState.NewPersistentDeallocator()

	deallocationNodeAssistant, err := space.NewDataNodeAssistant[deallocationKey, types.ListRoot]()
	requireT.NoError(err)

	snapshotInfoNodeAssistant, err := space.NewDataNodeAssistant[types.SnapshotID, types.SnapshotInfo]()
	requireT.NoError(err)

	var snapshotRoot types.Pointer

	snapshotSpace := space.New[types.SnapshotID, types.SnapshotInfo](space.Config[types.SnapshotID, types.SnapshotInfo]{
		SpaceRoot: types.NodeRoot{
			Pointer: &snapshotRoot,
		},
		State:             appState,
		DataNodeAssistant: snapshotInfoNodeAssistant,
		DeletionCounter:   lo.ToPtr[uint64](0),
		NoSnapshots:       true,
	})
	var snapshotEntry space.Entry[types.SnapshotID, types.SnapshotInfo]
	snapshotSpace.Find(&snapshotEntry, deleteSnapshotID, space.StageData)
	requireT.NoError(snapshotSpace.SetKey(&snapshotEntry, txFactory.New(), volatileAllocator, types.SnapshotInfo{
		PreviousSnapshotID: deleteSnapshotID - 1,
		NextSnapshotID:     deleteSnapshotID + 1,
	}))

	tx := txFactory.New()
	requireT.Error(deleteSnapshot(snapshotID, deleteSnapshotID, appState, tx, volatileAllocator, volatileDeallocator,
		persistentDeallocator, snapshotSpace, deallocationNodeAssistant))
	requireT.Nil(tx.StoreRequest)
	requireT.Nil(tx.ListRequest)
}

func TestDeletingNonExistingSnapshotFails(t *testing.T) {
	requireT := require.New(t)

	const (
		snapshotID       types.SnapshotID = 10
		deleteSnapshotID types.SnapshotID = 8
	)

	appState := state.NewForTest(t, 10*types.NodeLength)
	txFactory := pipeline.NewTransactionRequestFactory()
	volatileAllocator := appState.NewVolatileAllocator()
	volatileDeallocator := appState.NewVolatileDeallocator()
	persistentDeallocator := appState.NewPersistentDeallocator()

	deallocationNodeAssistant, err := space.NewDataNodeAssistant[deallocationKey, types.ListRoot]()
	requireT.NoError(err)

	snapshotInfoNodeAssistant, err := space.NewDataNodeAssistant[types.SnapshotID, types.SnapshotInfo]()
	requireT.NoError(err)

	var snapshotRoot types.Pointer

	snapshotSpace := space.New[types.SnapshotID, types.SnapshotInfo](space.Config[types.SnapshotID, types.SnapshotInfo]{
		SpaceRoot: types.NodeRoot{
			Pointer: &snapshotRoot,
		},
		State:             appState,
		DataNodeAssistant: snapshotInfoNodeAssistant,
		DeletionCounter:   lo.ToPtr[uint64](0),
		NoSnapshots:       true,
	})
	var snapshotEntry space.Entry[types.SnapshotID, types.SnapshotInfo]
	snapshotSpace.Find(&snapshotEntry, deleteSnapshotID+1, space.StageData)
	requireT.NoError(snapshotSpace.SetKey(&snapshotEntry, txFactory.New(), volatileAllocator, types.SnapshotInfo{
		PreviousSnapshotID: deleteSnapshotID,
		NextSnapshotID:     deleteSnapshotID + 2,
	}))

	tx := txFactory.New()
	requireT.Error(deleteSnapshot(snapshotID, deleteSnapshotID, appState, tx, volatileAllocator, volatileDeallocator,
		persistentDeallocator, snapshotSpace, deallocationNodeAssistant))
	requireT.Nil(tx.StoreRequest)
	requireT.Nil(tx.ListRequest)
}

func TestDeletingSnapshotFailsIfPreviousSnapshotDosNotExist(t *testing.T) {
	requireT := require.New(t)

	const (
		snapshotID       types.SnapshotID = 10
		deleteSnapshotID types.SnapshotID = 8
	)

	appState := state.NewForTest(t, 20*types.NodeLength)
	txFactory := pipeline.NewTransactionRequestFactory()
	volatileAllocator := appState.NewVolatileAllocator()
	volatileDeallocator := appState.NewVolatileDeallocator()
	persistentAllocator := appState.NewPersistentAllocator()
	persistentDeallocator := appState.NewPersistentDeallocator()

	deallocationNodeAssistant, err := space.NewDataNodeAssistant[deallocationKey, types.ListRoot]()
	requireT.NoError(err)

	snapshotInfoNodeAssistant, err := space.NewDataNodeAssistant[types.SnapshotID, types.SnapshotInfo]()
	requireT.NoError(err)

	var snapshotRoot types.Pointer

	snapshotSpace := space.New[types.SnapshotID, types.SnapshotInfo](space.Config[types.SnapshotID, types.SnapshotInfo]{
		SpaceRoot: types.NodeRoot{
			Pointer: &snapshotRoot,
		},
		State:             appState,
		DataNodeAssistant: snapshotInfoNodeAssistant,
		DeletionCounter:   lo.ToPtr[uint64](0),
		NoSnapshots:       true,
	})

	tx := txFactory.New()
	var snapshotEntry1 space.Entry[types.SnapshotID, types.SnapshotInfo]
	snapshotSpace.Find(&snapshotEntry1, deleteSnapshotID, space.StageData)
	requireT.NoError(snapshotSpace.SetKey(&snapshotEntry1, tx, volatileAllocator, types.SnapshotInfo{
		PreviousSnapshotID: deleteSnapshotID - 1,
		NextSnapshotID:     deleteSnapshotID + 1,
	}))

	var snapshotEntry2 space.Entry[types.SnapshotID, types.SnapshotInfo]
	snapshotSpace.Find(&snapshotEntry2, deleteSnapshotID+1, space.StageData)
	requireT.NoError(snapshotSpace.SetKey(&snapshotEntry2, tx, volatileAllocator, types.SnapshotInfo{
		PreviousSnapshotID: deleteSnapshotID,
		NextSnapshotID:     deleteSnapshotID + 2,
	}))

	for {
		if _, err := volatileAllocator.Allocate(); err != nil {
			break
		}
	}
	for {
		if _, err := persistentAllocator.Allocate(); err != nil {
			break
		}
	}

	tx = txFactory.New()
	requireT.Error(deleteSnapshot(snapshotID, deleteSnapshotID, appState, tx, volatileAllocator, volatileDeallocator,
		persistentDeallocator, snapshotSpace, deallocationNodeAssistant))
}

func TestDeletingSnapshotFailsIfNextSnapshotDosNotExist(t *testing.T) {
	requireT := require.New(t)

	const (
		snapshotID       types.SnapshotID = 10
		deleteSnapshotID types.SnapshotID = 8
	)

	appState := state.NewForTest(t, 20*types.NodeLength)
	txFactory := pipeline.NewTransactionRequestFactory()
	volatileAllocator := appState.NewVolatileAllocator()
	volatileDeallocator := appState.NewVolatileDeallocator()
	persistentAllocator := appState.NewPersistentAllocator()
	persistentDeallocator := appState.NewPersistentDeallocator()

	deallocationNodeAssistant, err := space.NewDataNodeAssistant[deallocationKey, types.ListRoot]()
	requireT.NoError(err)

	snapshotInfoNodeAssistant, err := space.NewDataNodeAssistant[types.SnapshotID, types.SnapshotInfo]()
	requireT.NoError(err)

	var snapshotRoot types.Pointer

	snapshotSpace := space.New[types.SnapshotID, types.SnapshotInfo](space.Config[types.SnapshotID, types.SnapshotInfo]{
		SpaceRoot: types.NodeRoot{
			Pointer: &snapshotRoot,
		},
		State:             appState,
		DataNodeAssistant: snapshotInfoNodeAssistant,
		DeletionCounter:   lo.ToPtr[uint64](0),
		NoSnapshots:       true,
	})

	tx := txFactory.New()
	var snapshotEntry1 space.Entry[types.SnapshotID, types.SnapshotInfo]
	snapshotSpace.Find(&snapshotEntry1, deleteSnapshotID, space.StageData)
	requireT.NoError(snapshotSpace.SetKey(&snapshotEntry1, tx, volatileAllocator, types.SnapshotInfo{
		PreviousSnapshotID: deleteSnapshotID - 1,
		NextSnapshotID:     deleteSnapshotID + 1,
	}))

	var snapshotEntry2 space.Entry[types.SnapshotID, types.SnapshotInfo]
	snapshotSpace.Find(&snapshotEntry2, deleteSnapshotID-1, space.StageData)
	requireT.NoError(snapshotSpace.SetKey(&snapshotEntry2, tx, volatileAllocator, types.SnapshotInfo{
		PreviousSnapshotID: deleteSnapshotID - 2,
		NextSnapshotID:     deleteSnapshotID,
	}))

	for {
		if _, err := volatileAllocator.Allocate(); err != nil {
			break
		}
	}
	for {
		if _, err := persistentAllocator.Allocate(); err != nil {
			break
		}
	}

	tx = txFactory.New()
	requireT.Error(deleteSnapshot(snapshotID, deleteSnapshotID, appState, tx, volatileAllocator, volatileDeallocator,
		persistentDeallocator, snapshotSpace, deallocationNodeAssistant))
}

func TestDeletingSnapshotWithoutDeallocationLists(t *testing.T) {
	requireT := require.New(t)

	const (
		snapshotID       types.SnapshotID = 10
		deleteSnapshotID types.SnapshotID = 8
	)

	appState := state.NewForTest(t, 10*types.NodeLength)
	txFactory := pipeline.NewTransactionRequestFactory()
	volatileAllocator := appState.NewVolatileAllocator()
	volatileDeallocator := appState.NewVolatileDeallocator()
	persistentAllocator := appState.NewPersistentAllocator()
	persistentDeallocator := appState.NewPersistentDeallocator()

	deallocationNodeAssistant, err := space.NewDataNodeAssistant[deallocationKey, types.ListRoot]()
	requireT.NoError(err)

	snapshotInfoNodeAssistant, err := space.NewDataNodeAssistant[types.SnapshotID, types.SnapshotInfo]()
	requireT.NoError(err)

	var snapshotRoot types.Pointer

	snapshotSpace := space.New[types.SnapshotID, types.SnapshotInfo](space.Config[types.SnapshotID, types.SnapshotInfo]{
		SpaceRoot: types.NodeRoot{
			Pointer: &snapshotRoot,
		},
		State:             appState,
		DataNodeAssistant: snapshotInfoNodeAssistant,
		DeletionCounter:   lo.ToPtr[uint64](0),
		NoSnapshots:       true,
	})

	var snapshotEntry1 space.Entry[types.SnapshotID, types.SnapshotInfo]
	snapshotSpace.Find(&snapshotEntry1, deleteSnapshotID-1, space.StageData)
	requireT.NoError(snapshotSpace.SetKey(&snapshotEntry1, txFactory.New(), volatileAllocator, types.SnapshotInfo{
		PreviousSnapshotID: 0,
		NextSnapshotID:     deleteSnapshotID,
	}))

	var snapshotEntry2 space.Entry[types.SnapshotID, types.SnapshotInfo]
	snapshotSpace.Find(&snapshotEntry2, deleteSnapshotID, space.StageData)
	requireT.NoError(snapshotSpace.SetKey(&snapshotEntry2, txFactory.New(), volatileAllocator, types.SnapshotInfo{
		PreviousSnapshotID: deleteSnapshotID - 1,
		NextSnapshotID:     deleteSnapshotID + 1,
	}))

	var snapshotEntry3 space.Entry[types.SnapshotID, types.SnapshotInfo]
	snapshotSpace.Find(&snapshotEntry3, deleteSnapshotID+1, space.StageData)
	requireT.NoError(snapshotSpace.SetKey(&snapshotEntry3, txFactory.New(), volatileAllocator, types.SnapshotInfo{
		PreviousSnapshotID: deleteSnapshotID,
		NextSnapshotID:     deleteSnapshotID + 2,
	}))

	for {
		if _, err := volatileAllocator.Allocate(); err != nil {
			break
		}
	}
	for {
		if _, err := persistentAllocator.Allocate(); err != nil {
			break
		}
	}

	tx := txFactory.New()
	requireT.NoError(deleteSnapshot(snapshotID, deleteSnapshotID, appState, tx, volatileAllocator, volatileDeallocator,
		persistentDeallocator, snapshotSpace, deallocationNodeAssistant))

	appState.Commit()

	_, err = volatileAllocator.Allocate()
	requireT.Error(err)

	_, err = persistentAllocator.Allocate()
	requireT.Error(err)

	_, exists := snapshotSpace.Query(deleteSnapshotID)
	requireT.False(exists)

	sInfo, exists := snapshotSpace.Query(deleteSnapshotID - 1)
	requireT.True(exists)
	requireT.Equal(deleteSnapshotID+1, sInfo.NextSnapshotID)
	requireT.Equal(types.SnapshotID(0), sInfo.PreviousSnapshotID)

	sInfo, exists = snapshotSpace.Query(deleteSnapshotID + 1)
	requireT.True(exists)
	requireT.Equal(deleteSnapshotID+2, sInfo.NextSnapshotID)
	requireT.Equal(deleteSnapshotID-1, sInfo.PreviousSnapshotID)
	requireT.Zero(sInfo.DeallocationRoot)

	requireT.Nil(tx.ListRequest)

	sr := tx.StoreRequest
	for range 3 {
		requireT.NotNil(sr)
		requireT.True(sr.NoSnapshots)
		requireT.Equal(int8(1), sr.PointersToStore)
		requireT.Equal(snapshotRoot, *sr.Store[0].Pointer)
		requireT.Equal(snapshotRoot.VolatileAddress, sr.Store[0].VolatileAddress)
		requireT.Zero(snapshotRoot.PersistentAddress)
		requireT.Zero(snapshotRoot.SnapshotID)
		requireT.Zero(snapshotRoot.Revision)
		sr = sr.Next
	}
	requireT.Nil(sr)
}

func TestDeletingSnapshotWithDeallocationListsWhichShouldNotBeDeallocated(t *testing.T) {
	requireT := require.New(t)

	const (
		snapshotID         types.SnapshotID = 10
		nextSnapshotID     types.SnapshotID = 9
		deleteSnapshotID   types.SnapshotID = 6
		previousSnapshotID types.SnapshotID = 3
	)

	appState := state.NewForTest(t, 20*types.NodeLength)
	txFactory := pipeline.NewTransactionRequestFactory()
	volatileAllocator := appState.NewVolatileAllocator()
	volatileDeallocator := appState.NewVolatileDeallocator()
	persistentAllocator := appState.NewPersistentAllocator()
	persistentDeallocator := appState.NewPersistentDeallocator()

	deallocationNodeAssistant, err := space.NewDataNodeAssistant[deallocationKey, types.ListRoot]()
	requireT.NoError(err)

	snapshotInfoNodeAssistant, err := space.NewDataNodeAssistant[types.SnapshotID, types.SnapshotInfo]()
	requireT.NoError(err)

	var snapshotRoot types.Pointer

	snapshotSpace := space.New[types.SnapshotID, types.SnapshotInfo](space.Config[types.SnapshotID, types.SnapshotInfo]{
		SpaceRoot: types.NodeRoot{
			Pointer: &snapshotRoot,
		},
		State:             appState,
		DataNodeAssistant: snapshotInfoNodeAssistant,
		DeletionCounter:   lo.ToPtr[uint64](0),
		NoSnapshots:       true,
	})

	tx := txFactory.New()
	var snapshotEntry1 space.Entry[types.SnapshotID, types.SnapshotInfo]
	snapshotSpace.Find(&snapshotEntry1, previousSnapshotID, space.StageData)
	requireT.NoError(snapshotSpace.SetKey(&snapshotEntry1, tx, volatileAllocator, types.SnapshotInfo{
		PreviousSnapshotID: 0,
		NextSnapshotID:     deleteSnapshotID,
	}))

	var snapshotEntry2 space.Entry[types.SnapshotID, types.SnapshotInfo]
	snapshotSpace.Find(&snapshotEntry2, deleteSnapshotID, space.StageData)
	requireT.NoError(snapshotSpace.SetKey(&snapshotEntry2, tx, volatileAllocator, types.SnapshotInfo{
		PreviousSnapshotID: previousSnapshotID,
		NextSnapshotID:     nextSnapshotID,
	}))

	var deallocationRoot types.Pointer

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

	var list1, list2, list3 types.ListRoot
	_, err = list.Add(&list1, 500, appState, volatileAllocator, persistentAllocator)
	requireT.NoError(err)
	_, err = list.Add(&list1, 501, appState, volatileAllocator, persistentAllocator)
	requireT.NoError(err)
	_, err = list.Add(&list2, 600, appState, volatileAllocator, persistentAllocator)
	requireT.NoError(err)
	_, err = list.Add(&list2, 601, appState, volatileAllocator, persistentAllocator)
	requireT.NoError(err)
	_, err = list.Add(&list3, 700, appState, volatileAllocator, persistentAllocator)
	requireT.NoError(err)
	_, err = list.Add(&list3, 701, appState, volatileAllocator, persistentAllocator)
	requireT.NoError(err)

	list1PersistentAddress, err := persistentAllocator.Allocate()
	requireT.NoError(err)

	list2PersistentAddress, err := persistentAllocator.Allocate()
	requireT.NoError(err)

	list3PersistentAddress, err := persistentAllocator.Allocate()
	requireT.NoError(err)

	list1.PersistentAddress = list1PersistentAddress
	list2.PersistentAddress = list2PersistentAddress
	list3.PersistentAddress = list3PersistentAddress

	list1Key := deallocationKey{
		ListSnapshotID: deleteSnapshotID,
		SnapshotID:     previousSnapshotID,
	}
	var deallocEntry1 space.Entry[deallocationKey, types.ListRoot]
	deallocationListSpace.Find(&deallocEntry1, list1Key, space.StageData)
	requireT.NoError(deallocationListSpace.SetKey(&deallocEntry1, tx, volatileAllocator, list1))

	list2Key := deallocationKey{
		ListSnapshotID: previousSnapshotID,
		SnapshotID:     previousSnapshotID - 1,
	}
	var deallocEntry2 space.Entry[deallocationKey, types.ListRoot]
	deallocationListSpace.Find(&deallocEntry2, list2Key, space.StageData)
	requireT.NoError(deallocationListSpace.SetKey(&deallocEntry2, tx, volatileAllocator, list2))

	list3Key := deallocationKey{
		ListSnapshotID: previousSnapshotID,
		SnapshotID:     previousSnapshotID - 2,
	}
	var deallocEntry3 space.Entry[deallocationKey, types.ListRoot]
	deallocationListSpace.Find(&deallocEntry3, list3Key, space.StageData)
	requireT.NoError(deallocationListSpace.SetKey(&deallocEntry3, tx, volatileAllocator, list3))

	deallocationRootPersistentAdress, err := persistentAllocator.Allocate()
	requireT.NoError(err)
	deallocationRoot.PersistentAddress = deallocationRootPersistentAdress
	deallocationRoot.SnapshotID = nextSnapshotID

	var snapshotEntry3 space.Entry[types.SnapshotID, types.SnapshotInfo]
	snapshotSpace.Find(&snapshotEntry3, nextSnapshotID, space.StageData)
	requireT.NoError(snapshotSpace.SetKey(&snapshotEntry3, txFactory.New(), volatileAllocator, types.SnapshotInfo{
		PreviousSnapshotID: deleteSnapshotID,
		NextSnapshotID:     nextSnapshotID + 1,
		DeallocationRoot:   deallocationRoot,
	}))

	for {
		if _, err := volatileAllocator.Allocate(); err != nil {
			break
		}
	}
	for {
		if _, err := persistentAllocator.Allocate(); err != nil {
			break
		}
	}

	volatileDeallocator.Deallocate(0x100)
	appState.Commit()

	tx = txFactory.New()
	requireT.NoError(deleteSnapshot(snapshotID, deleteSnapshotID, appState, tx, volatileAllocator, volatileDeallocator,
		persistentDeallocator, snapshotSpace, deallocationNodeAssistant))

	volatileDeallocator.Deallocate(0x101)
	persistentDeallocator.Deallocate(0x101)
	appState.Commit()

	_, err = volatileAllocator.Allocate()
	requireT.NoError(err)

	volatileAddress, err := volatileAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(deallocationRoot.VolatileAddress, volatileAddress)

	_, err = volatileAllocator.Allocate()
	requireT.Error(err)

	_, err = persistentAllocator.Allocate()
	requireT.NoError(err)

	persistentAddress, err := persistentAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(deallocationRoot.PersistentAddress, persistentAddress)

	_, err = persistentAllocator.Allocate()
	requireT.Error(err)

	_, exists := snapshotSpace.Query(deleteSnapshotID)
	requireT.False(exists)

	sInfo, exists := snapshotSpace.Query(previousSnapshotID)
	requireT.True(exists)
	requireT.Equal(nextSnapshotID, sInfo.NextSnapshotID)
	requireT.Equal(types.SnapshotID(0), sInfo.PreviousSnapshotID)

	sInfo, exists = snapshotSpace.Query(nextSnapshotID)
	requireT.True(exists)
	requireT.Equal(nextSnapshotID+1, sInfo.NextSnapshotID)
	requireT.Equal(previousSnapshotID, sInfo.PreviousSnapshotID)
	requireT.NotZero(sInfo.DeallocationRoot.VolatileAddress)

	deallocationListSpace = space.New[deallocationKey, types.ListRoot](
		space.Config[deallocationKey, types.ListRoot]{
			SpaceRoot: types.NodeRoot{
				Pointer: lo.ToPtr(sInfo.DeallocationRoot),
			},
			State:             appState,
			DataNodeAssistant: deallocationNodeAssistant,
			DeletionCounter:   lo.ToPtr[uint64](0),
			NoSnapshots:       true,
		},
	)

	listRoot, exists := deallocationListSpace.Query(list1Key)
	requireT.True(exists)
	requireT.Equal(list1, listRoot)

	listRoot, exists = deallocationListSpace.Query(list2Key)
	requireT.True(exists)
	requireT.Equal(list2, listRoot)

	listRoot, exists = deallocationListSpace.Query(list3Key)
	requireT.True(exists)
	requireT.Equal(list3, listRoot)

	requireT.Nil(tx.ListRequest)

	sr := tx.StoreRequest

	for range 3 {
		requireT.NotNil(sr)
		requireT.True(sr.NoSnapshots)
		requireT.Equal(int8(1), sr.PointersToStore)
		requireT.Equal(sInfo.DeallocationRoot, *sr.Store[0].Pointer)
		requireT.Equal(sInfo.DeallocationRoot.VolatileAddress, sr.Store[0].VolatileAddress)
		requireT.Zero(snapshotRoot.PersistentAddress)
		requireT.Zero(snapshotRoot.SnapshotID)
		requireT.Zero(snapshotRoot.Revision)
		sr = sr.Next
	}

	for range 3 {
		requireT.NotNil(sr)
		requireT.True(sr.NoSnapshots)
		requireT.Equal(int8(1), sr.PointersToStore)
		requireT.Equal(snapshotRoot, *sr.Store[0].Pointer)
		requireT.Equal(snapshotRoot.VolatileAddress, sr.Store[0].VolatileAddress)
		requireT.Zero(snapshotRoot.PersistentAddress)
		requireT.Zero(snapshotRoot.SnapshotID)
		requireT.Zero(snapshotRoot.Revision)
		sr = sr.Next
	}
	requireT.Nil(sr)
}

func TestDeletingSnapshotWithDeallocationListsWhichShouldBeDeallocated(t *testing.T) {
	requireT := require.New(t)

	const (
		snapshotID         types.SnapshotID = 10
		nextSnapshotID     types.SnapshotID = 9
		deleteSnapshotID   types.SnapshotID = 6
		previousSnapshotID types.SnapshotID = 3
	)

	appState := state.NewForTest(t, 20*types.NodeLength)
	txFactory := pipeline.NewTransactionRequestFactory()
	volatileAllocator := appState.NewVolatileAllocator()
	volatileDeallocator := appState.NewVolatileDeallocator()
	persistentAllocator := appState.NewPersistentAllocator()
	persistentDeallocator := appState.NewPersistentDeallocator()

	deallocationNodeAssistant, err := space.NewDataNodeAssistant[deallocationKey, types.ListRoot]()
	requireT.NoError(err)

	snapshotInfoNodeAssistant, err := space.NewDataNodeAssistant[types.SnapshotID, types.SnapshotInfo]()
	requireT.NoError(err)

	var snapshotRoot types.Pointer

	snapshotSpace := space.New[types.SnapshotID, types.SnapshotInfo](space.Config[types.SnapshotID, types.SnapshotInfo]{
		SpaceRoot: types.NodeRoot{
			Pointer: &snapshotRoot,
		},
		State:             appState,
		DataNodeAssistant: snapshotInfoNodeAssistant,
		DeletionCounter:   lo.ToPtr[uint64](0),
		NoSnapshots:       true,
	})

	tx := txFactory.New()
	var snapshotEntry1 space.Entry[types.SnapshotID, types.SnapshotInfo]
	snapshotSpace.Find(&snapshotEntry1, previousSnapshotID, space.StageData)
	requireT.NoError(snapshotSpace.SetKey(&snapshotEntry1, tx, volatileAllocator, types.SnapshotInfo{
		PreviousSnapshotID: 0,
		NextSnapshotID:     deleteSnapshotID,
	}))

	var snapshotEntry2 space.Entry[types.SnapshotID, types.SnapshotInfo]
	snapshotSpace.Find(&snapshotEntry2, deleteSnapshotID, space.StageData)
	requireT.NoError(snapshotSpace.SetKey(&snapshotEntry2, tx, volatileAllocator, types.SnapshotInfo{
		PreviousSnapshotID: previousSnapshotID,
		NextSnapshotID:     nextSnapshotID,
	}))

	var deallocationRoot types.Pointer

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

	var list1, list2, list3 types.ListRoot
	_, err = list.Add(&list1, 500, appState, volatileAllocator, persistentAllocator)
	requireT.NoError(err)
	_, err = list.Add(&list1, 501, appState, volatileAllocator, persistentAllocator)
	requireT.NoError(err)
	_, err = list.Add(&list2, 600, appState, volatileAllocator, persistentAllocator)
	requireT.NoError(err)
	_, err = list.Add(&list2, 601, appState, volatileAllocator, persistentAllocator)
	requireT.NoError(err)
	_, err = list.Add(&list3, 700, appState, volatileAllocator, persistentAllocator)
	requireT.NoError(err)
	_, err = list.Add(&list3, 701, appState, volatileAllocator, persistentAllocator)
	requireT.NoError(err)

	list1PersistentAddress, err := persistentAllocator.Allocate()
	requireT.NoError(err)

	list2PersistentAddress, err := persistentAllocator.Allocate()
	requireT.NoError(err)

	list3PersistentAddress, err := persistentAllocator.Allocate()
	requireT.NoError(err)

	list1.PersistentAddress = list1PersistentAddress
	list2.PersistentAddress = list2PersistentAddress
	list3.PersistentAddress = list3PersistentAddress

	list1Key := deallocationKey{
		ListSnapshotID: deleteSnapshotID + 1,
		SnapshotID:     deleteSnapshotID,
	}
	var deallocEntry1 space.Entry[deallocationKey, types.ListRoot]
	deallocationListSpace.Find(&deallocEntry1, list1Key, space.StageData)
	requireT.NoError(deallocationListSpace.SetKey(&deallocEntry1, tx, volatileAllocator, list1))

	list2Key := deallocationKey{
		ListSnapshotID: nextSnapshotID,
		SnapshotID:     previousSnapshotID + 1,
	}
	var deallocEntry2 space.Entry[deallocationKey, types.ListRoot]
	deallocationListSpace.Find(&deallocEntry2, list2Key, space.StageData)
	requireT.NoError(deallocationListSpace.SetKey(&deallocEntry2, tx, volatileAllocator, list2))

	list3Key := deallocationKey{
		ListSnapshotID: nextSnapshotID,
		SnapshotID:     previousSnapshotID + 2,
	}
	var deallocEntry3 space.Entry[deallocationKey, types.ListRoot]
	deallocationListSpace.Find(&deallocEntry3, list3Key, space.StageData)
	requireT.NoError(deallocationListSpace.SetKey(&deallocEntry3, tx, volatileAllocator, list3))

	deallocationRootPersistentAdress, err := persistentAllocator.Allocate()
	requireT.NoError(err)
	deallocationRoot.PersistentAddress = deallocationRootPersistentAdress
	deallocationRoot.SnapshotID = nextSnapshotID

	var snapshotEntry3 space.Entry[types.SnapshotID, types.SnapshotInfo]
	snapshotSpace.Find(&snapshotEntry3, nextSnapshotID, space.StageData)
	requireT.NoError(snapshotSpace.SetKey(&snapshotEntry3, txFactory.New(), volatileAllocator, types.SnapshotInfo{
		PreviousSnapshotID: deleteSnapshotID,
		NextSnapshotID:     nextSnapshotID + 1,
		DeallocationRoot:   deallocationRoot,
	}))

	for {
		if _, err := volatileAllocator.Allocate(); err != nil {
			break
		}
	}
	for {
		if _, err := persistentAllocator.Allocate(); err != nil {
			break
		}
	}

	tx = txFactory.New()
	requireT.NoError(deleteSnapshot(snapshotID, deleteSnapshotID, appState, tx, volatileAllocator, volatileDeallocator,
		persistentDeallocator, snapshotSpace, deallocationNodeAssistant))

	volatileDeallocator.Deallocate(0x100)
	persistentDeallocator.Deallocate(0x100)
	appState.Commit()

	_, err = volatileAllocator.Allocate()
	requireT.NoError(err)

	_, err = persistentAllocator.Allocate()
	requireT.NoError(err)

	volatileAddress, err := volatileAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(deallocationRoot.VolatileAddress, volatileAddress)

	persistentAddress, err := persistentAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(deallocationRoot.PersistentAddress, persistentAddress)

	volatileAddress, err = volatileAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(list1.VolatileAddress, volatileAddress)

	persistentAddress, err = persistentAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(list1.PersistentAddress, persistentAddress)

	persistentAddress, err = persistentAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(types.PersistentAddress(500), persistentAddress)

	persistentAddress, err = persistentAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(types.PersistentAddress(501), persistentAddress)

	volatileAddress, err = volatileAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(list2.VolatileAddress, volatileAddress)

	persistentAddress, err = persistentAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(list2.PersistentAddress, persistentAddress)

	persistentAddress, err = persistentAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(types.PersistentAddress(600), persistentAddress)

	persistentAddress, err = persistentAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(types.PersistentAddress(601), persistentAddress)

	volatileAddress, err = volatileAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(list3.VolatileAddress, volatileAddress)

	persistentAddress, err = persistentAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(list3.PersistentAddress, persistentAddress)

	persistentAddress, err = persistentAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(types.PersistentAddress(700), persistentAddress)

	persistentAddress, err = persistentAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(types.PersistentAddress(701), persistentAddress)

	_, err = volatileAllocator.Allocate()
	requireT.Error(err)

	_, err = persistentAllocator.Allocate()
	requireT.Error(err)

	_, exists := snapshotSpace.Query(deleteSnapshotID)
	requireT.False(exists)

	sInfo, exists := snapshotSpace.Query(previousSnapshotID)
	requireT.True(exists)
	requireT.Equal(nextSnapshotID, sInfo.NextSnapshotID)
	requireT.Equal(types.SnapshotID(0), sInfo.PreviousSnapshotID)

	sInfo, exists = snapshotSpace.Query(nextSnapshotID)
	requireT.True(exists)
	requireT.Equal(nextSnapshotID+1, sInfo.NextSnapshotID)
	requireT.Equal(previousSnapshotID, sInfo.PreviousSnapshotID)
	requireT.Zero(sInfo.DeallocationRoot)

	requireT.Nil(tx.ListRequest)

	sr := tx.StoreRequest

	for range 3 {
		requireT.NotNil(sr)
		requireT.True(sr.NoSnapshots)
		requireT.Equal(int8(1), sr.PointersToStore)
		requireT.Equal(snapshotRoot, *sr.Store[0].Pointer)
		requireT.Equal(snapshotRoot.VolatileAddress, sr.Store[0].VolatileAddress)
		requireT.Zero(snapshotRoot.PersistentAddress)
		requireT.Zero(snapshotRoot.SnapshotID)
		requireT.Zero(snapshotRoot.Revision)
		sr = sr.Next
	}
	requireT.Nil(sr)
}

func TestDeletingTheOldestSnapshot(t *testing.T) {
	requireT := require.New(t)

	const (
		snapshotID       types.SnapshotID = 10
		nextSnapshotID   types.SnapshotID = 9
		deleteSnapshotID types.SnapshotID = 3
	)

	appState := state.NewForTest(t, 20*types.NodeLength)
	txFactory := pipeline.NewTransactionRequestFactory()
	volatileAllocator := appState.NewVolatileAllocator()
	volatileDeallocator := appState.NewVolatileDeallocator()
	persistentAllocator := appState.NewPersistentAllocator()
	persistentDeallocator := appState.NewPersistentDeallocator()

	deallocationNodeAssistant, err := space.NewDataNodeAssistant[deallocationKey, types.ListRoot]()
	requireT.NoError(err)

	snapshotInfoNodeAssistant, err := space.NewDataNodeAssistant[types.SnapshotID, types.SnapshotInfo]()
	requireT.NoError(err)

	var snapshotRoot types.Pointer

	snapshotSpace := space.New[types.SnapshotID, types.SnapshotInfo](space.Config[types.SnapshotID, types.SnapshotInfo]{
		SpaceRoot: types.NodeRoot{
			Pointer: &snapshotRoot,
		},
		State:             appState,
		DataNodeAssistant: snapshotInfoNodeAssistant,
		DeletionCounter:   lo.ToPtr[uint64](0),
		NoSnapshots:       true,
	})

	tx := txFactory.New()
	var snapshotEntry1 space.Entry[types.SnapshotID, types.SnapshotInfo]
	snapshotSpace.Find(&snapshotEntry1, deleteSnapshotID, space.StageData)
	requireT.NoError(snapshotSpace.SetKey(&snapshotEntry1, tx, volatileAllocator, types.SnapshotInfo{
		PreviousSnapshotID: 0,
		NextSnapshotID:     nextSnapshotID,
	}))

	var deallocationRoot types.Pointer

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

	var list1, list2, list3 types.ListRoot
	_, err = list.Add(&list1, 500, appState, volatileAllocator, persistentAllocator)
	requireT.NoError(err)
	_, err = list.Add(&list1, 501, appState, volatileAllocator, persistentAllocator)
	requireT.NoError(err)
	_, err = list.Add(&list2, 600, appState, volatileAllocator, persistentAllocator)
	requireT.NoError(err)
	_, err = list.Add(&list2, 601, appState, volatileAllocator, persistentAllocator)
	requireT.NoError(err)
	_, err = list.Add(&list3, 700, appState, volatileAllocator, persistentAllocator)
	requireT.NoError(err)
	_, err = list.Add(&list3, 701, appState, volatileAllocator, persistentAllocator)
	requireT.NoError(err)

	list1PersistentAddress, err := persistentAllocator.Allocate()
	requireT.NoError(err)

	list2PersistentAddress, err := persistentAllocator.Allocate()
	requireT.NoError(err)

	list3PersistentAddress, err := persistentAllocator.Allocate()
	requireT.NoError(err)

	list1.PersistentAddress = list1PersistentAddress
	list2.PersistentAddress = list2PersistentAddress
	list3.PersistentAddress = list3PersistentAddress

	list1Key := deallocationKey{
		ListSnapshotID: nextSnapshotID,
		SnapshotID:     deleteSnapshotID - 2,
	}
	var deallocEntry1 space.Entry[deallocationKey, types.ListRoot]
	deallocationListSpace.Find(&deallocEntry1, list1Key, space.StageData)
	requireT.NoError(deallocationListSpace.SetKey(&deallocEntry1, tx, volatileAllocator, list1))

	list2Key := deallocationKey{
		ListSnapshotID: nextSnapshotID,
		SnapshotID:     deleteSnapshotID - 1,
	}
	var deallocEntry2 space.Entry[deallocationKey, types.ListRoot]
	deallocationListSpace.Find(&deallocEntry2, list2Key, space.StageData)
	requireT.NoError(deallocationListSpace.SetKey(&deallocEntry2, tx, volatileAllocator, list2))

	list3Key := deallocationKey{
		ListSnapshotID: nextSnapshotID,
		SnapshotID:     deleteSnapshotID,
	}
	var deallocEntry3 space.Entry[deallocationKey, types.ListRoot]
	deallocationListSpace.Find(&deallocEntry3, list3Key, space.StageData)
	requireT.NoError(deallocationListSpace.SetKey(&deallocEntry3, tx, volatileAllocator, list3))

	deallocationRootPersistentAdress, err := persistentAllocator.Allocate()
	requireT.NoError(err)
	deallocationRoot.PersistentAddress = deallocationRootPersistentAdress
	deallocationRoot.SnapshotID = nextSnapshotID

	var snapshotEntry2 space.Entry[types.SnapshotID, types.SnapshotInfo]
	snapshotSpace.Find(&snapshotEntry2, nextSnapshotID, space.StageData)
	requireT.NoError(snapshotSpace.SetKey(&snapshotEntry2, tx, volatileAllocator, types.SnapshotInfo{
		PreviousSnapshotID: deleteSnapshotID,
		NextSnapshotID:     nextSnapshotID + 1,
		DeallocationRoot:   deallocationRoot,
	}))

	for {
		if _, err := volatileAllocator.Allocate(); err != nil {
			break
		}
	}
	for {
		if _, err := persistentAllocator.Allocate(); err != nil {
			break
		}
	}

	tx = txFactory.New()
	requireT.NoError(deleteSnapshot(snapshotID, deleteSnapshotID, appState, tx, volatileAllocator, volatileDeallocator,
		persistentDeallocator, snapshotSpace, deallocationNodeAssistant))

	volatileDeallocator.Deallocate(0x100)
	persistentDeallocator.Deallocate(0x100)
	appState.Commit()

	_, err = volatileAllocator.Allocate()
	requireT.NoError(err)

	_, err = persistentAllocator.Allocate()
	requireT.NoError(err)

	volatileAddress, err := volatileAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(deallocationRoot.VolatileAddress, volatileAddress)

	persistentAddress, err := persistentAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(deallocationRoot.PersistentAddress, persistentAddress)

	volatileAddress, err = volatileAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(list1.VolatileAddress, volatileAddress)

	persistentAddress, err = persistentAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(list1.PersistentAddress, persistentAddress)

	persistentAddress, err = persistentAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(types.PersistentAddress(500), persistentAddress)

	persistentAddress, err = persistentAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(types.PersistentAddress(501), persistentAddress)

	volatileAddress, err = volatileAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(list2.VolatileAddress, volatileAddress)

	persistentAddress, err = persistentAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(list2.PersistentAddress, persistentAddress)

	persistentAddress, err = persistentAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(types.PersistentAddress(600), persistentAddress)

	persistentAddress, err = persistentAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(types.PersistentAddress(601), persistentAddress)

	volatileAddress, err = volatileAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(list3.VolatileAddress, volatileAddress)

	persistentAddress, err = persistentAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(list3.PersistentAddress, persistentAddress)

	persistentAddress, err = persistentAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(types.PersistentAddress(700), persistentAddress)

	persistentAddress, err = persistentAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(types.PersistentAddress(701), persistentAddress)

	_, err = volatileAllocator.Allocate()
	requireT.Error(err)

	_, err = persistentAllocator.Allocate()
	requireT.Error(err)

	_, exists := snapshotSpace.Query(deleteSnapshotID)
	requireT.False(exists)

	sInfo, exists := snapshotSpace.Query(nextSnapshotID)
	requireT.True(exists)
	requireT.Equal(snapshotID, sInfo.NextSnapshotID)
	requireT.Equal(types.SnapshotID(0), sInfo.PreviousSnapshotID)
	requireT.Zero(sInfo.DeallocationRoot)

	requireT.Nil(tx.ListRequest)

	sr := tx.StoreRequest

	for range 2 {
		requireT.NotNil(sr)
		requireT.True(sr.NoSnapshots)
		requireT.Equal(int8(1), sr.PointersToStore)
		requireT.Equal(snapshotRoot, *sr.Store[0].Pointer)
		requireT.Equal(snapshotRoot.VolatileAddress, sr.Store[0].VolatileAddress)
		requireT.Zero(snapshotRoot.PersistentAddress)
		requireT.Zero(snapshotRoot.SnapshotID)
		requireT.Zero(snapshotRoot.Revision)
		sr = sr.Next
	}
	requireT.Nil(sr)
}

func TestDominoSnapshotDeletion(t *testing.T) {
	requireT := require.New(t)

	const (
		snapshotID types.SnapshotID = 5
		snapshot4  types.SnapshotID = 4
		snapshot3  types.SnapshotID = 3
		snapshot2  types.SnapshotID = 2
		snapshot1  types.SnapshotID = 1
	)

	appState := state.NewForTest(t, 20*types.NodeLength)
	txFactory := pipeline.NewTransactionRequestFactory()
	volatileAllocator := appState.NewVolatileAllocator()
	volatileDeallocator := appState.NewVolatileDeallocator()
	persistentAllocator := appState.NewPersistentAllocator()
	persistentDeallocator := appState.NewPersistentDeallocator()

	deallocationNodeAssistant, err := space.NewDataNodeAssistant[deallocationKey, types.ListRoot]()
	requireT.NoError(err)

	snapshotInfoNodeAssistant, err := space.NewDataNodeAssistant[types.SnapshotID, types.SnapshotInfo]()
	requireT.NoError(err)

	var snapshotRoot types.Pointer

	snapshotSpace := space.New[types.SnapshotID, types.SnapshotInfo](space.Config[types.SnapshotID, types.SnapshotInfo]{
		SpaceRoot: types.NodeRoot{
			Pointer: &snapshotRoot,
		},
		State:             appState,
		DataNodeAssistant: snapshotInfoNodeAssistant,
		DeletionCounter:   lo.ToPtr[uint64](0),
		NoSnapshots:       true,
	})

	tx := txFactory.New()

	// snapshot 1

	var snapshotEntry1 space.Entry[types.SnapshotID, types.SnapshotInfo]
	snapshotSpace.Find(&snapshotEntry1, snapshot1, space.StageData)
	requireT.NoError(snapshotSpace.SetKey(&snapshotEntry1, tx, volatileAllocator, types.SnapshotInfo{
		PreviousSnapshotID: 0,
		NextSnapshotID:     snapshot2,
	}))

	// snapshot 2

	var deallocationRoot2 types.Pointer

	deallocationListSpace := space.New[deallocationKey, types.ListRoot](
		space.Config[deallocationKey, types.ListRoot]{
			SpaceRoot: types.NodeRoot{
				Pointer: &deallocationRoot2,
			},
			State:             appState,
			DataNodeAssistant: deallocationNodeAssistant,
			DeletionCounter:   lo.ToPtr[uint64](0),
			NoSnapshots:       true,
		},
	)

	var list21 types.ListRoot
	_, err = list.Add(&list21, 211, appState, volatileAllocator, persistentAllocator)
	requireT.NoError(err)

	list21PersistentAddress, err := persistentAllocator.Allocate()
	requireT.NoError(err)

	list21.PersistentAddress = list21PersistentAddress

	list21Key := deallocationKey{
		ListSnapshotID: snapshot2,
		SnapshotID:     snapshot1,
	}
	var deallocEntry21 space.Entry[deallocationKey, types.ListRoot]
	deallocationListSpace.Find(&deallocEntry21, list21Key, space.StageData)
	requireT.NoError(deallocationListSpace.SetKey(&deallocEntry21, tx, volatileAllocator, list21))

	deallocationRoot2PersistentAddress, err := persistentAllocator.Allocate()
	requireT.NoError(err)
	deallocationRoot2.PersistentAddress = deallocationRoot2PersistentAddress

	var snapshotEntry2 space.Entry[types.SnapshotID, types.SnapshotInfo]
	snapshotSpace.Find(&snapshotEntry2, snapshot2, space.StageData)
	requireT.NoError(snapshotSpace.SetKey(&snapshotEntry2, tx, volatileAllocator, types.SnapshotInfo{
		PreviousSnapshotID: snapshot1,
		NextSnapshotID:     snapshot3,
		DeallocationRoot:   deallocationRoot2,
	}))

	// snapshot 3

	var deallocationRoot3 types.Pointer

	deallocationListSpace = space.New[deallocationKey, types.ListRoot](
		space.Config[deallocationKey, types.ListRoot]{
			SpaceRoot: types.NodeRoot{
				Pointer: &deallocationRoot3,
			},
			State:             appState,
			DataNodeAssistant: deallocationNodeAssistant,
			DeletionCounter:   lo.ToPtr[uint64](0),
			NoSnapshots:       true,
		},
	)

	var list31 types.ListRoot
	_, err = list.Add(&list31, 311, appState, volatileAllocator, persistentAllocator)
	requireT.NoError(err)

	list31PersistentAddress, err := persistentAllocator.Allocate()
	requireT.NoError(err)

	list31.PersistentAddress = list31PersistentAddress

	list31Key := deallocationKey{
		ListSnapshotID: snapshot3,
		SnapshotID:     snapshot1,
	}
	var deallocEntry31 space.Entry[deallocationKey, types.ListRoot]
	deallocationListSpace.Find(&deallocEntry31, list31Key, space.StageData)
	requireT.NoError(deallocationListSpace.SetKey(&deallocEntry31, tx, volatileAllocator, list31))

	var list32 types.ListRoot
	_, err = list.Add(&list32, 321, appState, volatileAllocator, persistentAllocator)
	requireT.NoError(err)

	list32PersistentAddress, err := persistentAllocator.Allocate()
	requireT.NoError(err)

	list32.PersistentAddress = list32PersistentAddress

	list32Key := deallocationKey{
		ListSnapshotID: snapshot3,
		SnapshotID:     snapshot2,
	}
	var deallocEntry32 space.Entry[deallocationKey, types.ListRoot]
	deallocationListSpace.Find(&deallocEntry32, list32Key, space.StageData)
	requireT.NoError(deallocationListSpace.SetKey(&deallocEntry32, tx, volatileAllocator, list32))

	deallocationRoot3PersistentAddress, err := persistentAllocator.Allocate()
	requireT.NoError(err)
	deallocationRoot3.PersistentAddress = deallocationRoot3PersistentAddress

	var snapshotEntry3 space.Entry[types.SnapshotID, types.SnapshotInfo]
	snapshotSpace.Find(&snapshotEntry3, snapshot3, space.StageData)
	requireT.NoError(snapshotSpace.SetKey(&snapshotEntry3, tx, volatileAllocator, types.SnapshotInfo{
		PreviousSnapshotID: snapshot2,
		NextSnapshotID:     snapshot4,
		DeallocationRoot:   deallocationRoot3,
	}))

	// snapshot 4

	var deallocationRoot4 types.Pointer

	deallocationListSpace = space.New[deallocationKey, types.ListRoot](
		space.Config[deallocationKey, types.ListRoot]{
			SpaceRoot: types.NodeRoot{
				Pointer: &deallocationRoot4,
			},
			State:             appState,
			DataNodeAssistant: deallocationNodeAssistant,
			DeletionCounter:   lo.ToPtr[uint64](0),
			NoSnapshots:       true,
		},
	)

	var list41 types.ListRoot
	_, err = list.Add(&list41, 411, appState, volatileAllocator, persistentAllocator)
	requireT.NoError(err)

	list41PersistentAddress, err := persistentAllocator.Allocate()
	requireT.NoError(err)

	list41.PersistentAddress = list41PersistentAddress

	list41Key := deallocationKey{
		ListSnapshotID: snapshot4,
		SnapshotID:     snapshot1,
	}
	var deallocEntry41 space.Entry[deallocationKey, types.ListRoot]
	deallocationListSpace.Find(&deallocEntry41, list41Key, space.StageData)
	requireT.NoError(deallocationListSpace.SetKey(&deallocEntry41, tx, volatileAllocator, list41))

	var list42 types.ListRoot
	_, err = list.Add(&list42, 421, appState, volatileAllocator, persistentAllocator)
	requireT.NoError(err)

	list42PersistentAddress, err := persistentAllocator.Allocate()
	requireT.NoError(err)

	list42.PersistentAddress = list42PersistentAddress

	list42Key := deallocationKey{
		ListSnapshotID: snapshot4,
		SnapshotID:     snapshot2,
	}
	var deallocEntry42 space.Entry[deallocationKey, types.ListRoot]
	deallocationListSpace.Find(&deallocEntry42, list42Key, space.StageData)
	requireT.NoError(deallocationListSpace.SetKey(&deallocEntry42, tx, volatileAllocator, list42))

	var list43 types.ListRoot
	_, err = list.Add(&list43, 431, appState, volatileAllocator, persistentAllocator)
	requireT.NoError(err)

	list43PersistentAddress, err := persistentAllocator.Allocate()
	requireT.NoError(err)

	list43.PersistentAddress = list43PersistentAddress

	list43Key := deallocationKey{
		ListSnapshotID: snapshot4,
		SnapshotID:     snapshot3,
	}
	var deallocEntry43 space.Entry[deallocationKey, types.ListRoot]
	deallocationListSpace.Find(&deallocEntry43, list43Key, space.StageData)
	requireT.NoError(deallocationListSpace.SetKey(&deallocEntry43, tx, volatileAllocator, list43))

	deallocationRoot4PersistentAddress, err := persistentAllocator.Allocate()
	requireT.NoError(err)
	deallocationRoot4.PersistentAddress = deallocationRoot4PersistentAddress

	var snapshotEntry4 space.Entry[types.SnapshotID, types.SnapshotInfo]
	snapshotSpace.Find(&snapshotEntry4, snapshot4, space.StageData)
	requireT.NoError(snapshotSpace.SetKey(&snapshotEntry4, tx, volatileAllocator, types.SnapshotInfo{
		PreviousSnapshotID: snapshot3,
		NextSnapshotID:     snapshotID,
		DeallocationRoot:   deallocationRoot4,
	}))

	for {
		if _, err := persistentAllocator.Allocate(); err != nil {
			break
		}
	}

	tx = txFactory.New()

	// delete 2

	requireT.NoError(deleteSnapshot(snapshotID, snapshot2, appState, tx, volatileAllocator, volatileDeallocator,
		persistentDeallocator, snapshotSpace, deallocationNodeAssistant))

	persistentDeallocator.Deallocate(0x100)
	appState.Commit()

	_, err = persistentAllocator.Allocate()
	requireT.NoError(err)

	persistentAddress, err := persistentAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(deallocationRoot3.PersistentAddress, persistentAddress)

	persistentAddress, err = persistentAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(list32.PersistentAddress, persistentAddress)

	persistentAddress, err = persistentAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(types.PersistentAddress(321), persistentAddress)

	_, err = persistentAllocator.Allocate()
	requireT.Error(err)

	// delete 1

	sInfo3, exists := snapshotSpace.Query(snapshot3)
	requireT.True(exists)

	requireT.NoError(deleteSnapshot(snapshotID, snapshot1, appState, tx, volatileAllocator, volatileDeallocator,
		persistentDeallocator, snapshotSpace, deallocationNodeAssistant))

	persistentDeallocator.Deallocate(0x200)
	appState.Commit()

	_, err = persistentAllocator.Allocate()
	requireT.NoError(err)

	persistentAddress, err = persistentAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(sInfo3.DeallocationRoot.PersistentAddress, persistentAddress)

	persistentAddress, err = persistentAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(list21.PersistentAddress, persistentAddress)

	persistentAddress, err = persistentAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(types.PersistentAddress(211), persistentAddress)

	persistentAddress, err = persistentAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(list31.PersistentAddress, persistentAddress)

	persistentAddress, err = persistentAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(types.PersistentAddress(311), persistentAddress)

	_, err = persistentAllocator.Allocate()
	requireT.Error(err)

	// delete 3

	requireT.NoError(deleteSnapshot(snapshotID, snapshot3, appState, tx, volatileAllocator, volatileDeallocator,
		persistentDeallocator, snapshotSpace, deallocationNodeAssistant))

	persistentDeallocator.Deallocate(0x300)
	appState.Commit()

	_, err = persistentAllocator.Allocate()
	requireT.NoError(err)

	persistentAddress, err = persistentAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(deallocationRoot4.PersistentAddress, persistentAddress)

	persistentAddress, err = persistentAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(list41.PersistentAddress, persistentAddress)

	persistentAddress, err = persistentAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(types.PersistentAddress(411), persistentAddress)

	persistentAddress, err = persistentAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(list42.PersistentAddress, persistentAddress)

	persistentAddress, err = persistentAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(types.PersistentAddress(421), persistentAddress)

	persistentAddress, err = persistentAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(list43.PersistentAddress, persistentAddress)

	persistentAddress, err = persistentAllocator.Allocate()
	requireT.NoError(err)
	requireT.Equal(types.PersistentAddress(431), persistentAddress)

	_, err = persistentAllocator.Allocate()
	requireT.Error(err)
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
