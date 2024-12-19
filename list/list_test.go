package list

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/types"
)

const stateSize = (numOfAddresses + 6) * types.NodeLength

func TestList(t *testing.T) {
	requireT := require.New(t)

	state := alloc.NewForTest(t, stateSize)
	volatileAllocator := state.NewVolatileAllocator()
	persistentAllocator := state.NewPersistentAllocator()

	var root types.ListRoot

	var lNodeToStore types.ListRoot

	expectedPersistentAddresses := []types.PersistentAddress{}

	for range 2 * numOfAddresses {
		persistentAddress, err := persistentAllocator.Allocate()
		requireT.NoError(err)

		expectedPersistentAddresses = append(expectedPersistentAddresses, persistentAddress)

		nodeToStore, err := Add(&root, persistentAddress, state, volatileAllocator, persistentAllocator)
		requireT.NoError(err)

		if nodeToStore.VolatileAddress != 0 {
			requireT.Zero(lNodeToStore.VolatileAddress)
			lNodeToStore = nodeToStore
		}
	}

	requireT.NotZero(root.VolatileAddress)
	requireT.NotZero(root.PersistentAddress)
	requireT.NotZero(lNodeToStore.VolatileAddress)
	requireT.NotZero(lNodeToStore.PersistentAddress)
	requireT.NotEqual(root.VolatileAddress, lNodeToStore.VolatileAddress)
	requireT.NotEqual(root.PersistentAddress, lNodeToStore.PersistentAddress)

	expectedPersistentAddresses = append(expectedPersistentAddresses, root.PersistentAddress,
		lNodeToStore.PersistentAddress)
	persistentAddresses := []types.PersistentAddress{
		root.PersistentAddress,
		lNodeToStore.PersistentAddress,
	}

	n := projectNode(state.Node(root.VolatileAddress))
	requireT.Equal(lNodeToStore.VolatileAddress, n.Next.VolatileAddress)
	requireT.Equal(lNodeToStore.PersistentAddress, n.Next.PersistentAddress)

	for i := range n.NumOfPointerAddresses {
		persistentAddresses = append(persistentAddresses, n.Slots[i])
	}

	n = projectNode(state.Node(lNodeToStore.VolatileAddress))
	requireT.Zero(n.Next.VolatileAddress)
	requireT.Zero(n.Next.PersistentAddress)

	for i := range n.NumOfPointerAddresses {
		persistentAddresses = append(persistentAddresses, n.Slots[i])
	}

	sort.Slice(expectedPersistentAddresses, func(i, j int) bool {
		return expectedPersistentAddresses[i] < expectedPersistentAddresses[j]
	})
	sort.Slice(persistentAddresses, func(i, j int) bool {
		return persistentAddresses[i] < persistentAddresses[j]
	})

	requireT.Equal(expectedPersistentAddresses, persistentAddresses)

	for {
		_, err := volatileAllocator.Allocate()
		if err != nil {
			break
		}
	}
	for {
		_, err := persistentAllocator.Allocate()
		if err != nil {
			break
		}
	}

	volatileDeallocator := state.NewVolatileDeallocator()
	persistentDeallocator := state.NewPersistentDeallocator()
	requireT.NoError(Deallocate(root, state, volatileDeallocator, persistentDeallocator))

	volatileDeallocator.Deallocate(0x00)
	persistentDeallocator.Deallocate(0x00)

	state.Commit()

	_, err := volatileAllocator.Allocate()
	requireT.NoError(err)
	_, err = persistentAllocator.Allocate()
	requireT.NoError(err)

	deallocatedVolatile := []types.VolatileAddress{}
	deallocatedPersistent := []types.PersistentAddress{}

	for {
		address, err := volatileAllocator.Allocate()
		if err != nil {
			break
		}
		deallocatedVolatile = append(deallocatedVolatile, address)
	}
	for {
		address, err := persistentAllocator.Allocate()
		if err != nil {
			break
		}
		deallocatedPersistent = append(deallocatedPersistent, address)
	}

	sort.Slice(deallocatedVolatile, func(i, j int) bool {
		return deallocatedVolatile[i] < deallocatedVolatile[j]
	})
	sort.Slice(deallocatedPersistent, func(i, j int) bool {
		return deallocatedPersistent[i] < deallocatedPersistent[j]
	})

	requireT.Equal(expectedPersistentAddresses, deallocatedPersistent)
	requireT.Equal([]types.VolatileAddress{lNodeToStore.VolatileAddress, root.VolatileAddress}, deallocatedVolatile)
}
