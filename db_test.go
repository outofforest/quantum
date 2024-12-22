package quantum

import (
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

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

	listRoot, err := deallocateNode(appState, 10, 1, persistentAddress,
		deallocationListsToCommit, volatileAllocator, persistentAllocator, persistentDeallocator, true)
	requireT.NoError(err)
	requireT.Zero(listRoot.VolatileAddress)
	requireT.Zero(listRoot.PersistentAddress)
	requireT.Empty(deallocationListsToCommit)

	for {
		if _, err := persistentAllocator.Allocate(); err != nil {
			break
		}
	}

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

	listRoot, err := deallocateNode(appState, 10, 10, persistentAddress,
		deallocationListsToCommit, volatileAllocator, persistentAllocator, persistentDeallocator, false)
	requireT.NoError(err)
	requireT.Zero(listRoot.VolatileAddress)
	requireT.Zero(listRoot.PersistentAddress)
	requireT.Empty(deallocationListsToCommit)

	for {
		if _, err := persistentAllocator.Allocate(); err != nil {
			break
		}
	}

	persistentDeallocator.Deallocate(0x00)
	appState.Commit()

	_, err = persistentAllocator.Allocate()
	requireT.NoError(err)

	deallocatedPersistentAddress, err := persistentAllocator.Allocate()
	requireT.NoError(err)

	requireT.Equal(persistentAddress, deallocatedPersistentAddress)
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
