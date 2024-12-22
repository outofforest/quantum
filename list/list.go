package list

import (
	"github.com/outofforest/quantum/state"
	"github.com/outofforest/quantum/types"
)

// Add adds address to the list.
func Add(
	listRoot *types.ListRoot,
	nodeAddress types.PersistentAddress,
	appState *state.State,
	volatileAllocator *state.Allocator[types.VolatileAddress],
	persistentAllocator *state.Allocator[types.PersistentAddress],
) (types.ListRoot, error) {
	if listRoot.VolatileAddress == types.FreeAddress {
		var err error
		listRoot.VolatileAddress, err = volatileAllocator.Allocate()
		if err != nil {
			return types.ListRoot{}, err
		}
		listRoot.PersistentAddress, err = persistentAllocator.Allocate()
		if err != nil {
			return types.ListRoot{}, err
		}
		n := projectNode(appState.Node(listRoot.VolatileAddress))

		n.Slots[0] = nodeAddress
		n.NumOfPointerAddresses = 1
		// This is needed because list nodes are not zeroed.
		n.Next = types.ListRoot{}

		return types.ListRoot{}, nil
	}

	n := projectNode(appState.Node(listRoot.VolatileAddress))
	if n.NumOfPointerAddresses < numOfAddresses {
		n.Slots[n.NumOfPointerAddresses] = nodeAddress
		n.NumOfPointerAddresses++

		return types.ListRoot{}, nil
	}

	oldRoot := *listRoot

	var err error
	listRoot.VolatileAddress, err = volatileAllocator.Allocate()
	if err != nil {
		return types.ListRoot{}, err
	}

	listRoot.PersistentAddress, err = persistentAllocator.Allocate()
	if err != nil {
		return types.ListRoot{}, err
	}
	n = projectNode(appState.Node(listRoot.VolatileAddress))

	n.Slots[0] = nodeAddress
	n.NumOfPointerAddresses = 1
	n.Next = oldRoot

	return oldRoot, nil
}

// Deallocate deallocates nodes referenced by the list.
func Deallocate(
	listRoot types.ListRoot,
	appState *state.State,
	volatileDeallocator *state.Deallocator[types.VolatileAddress],
	persistentDeallocator *state.Deallocator[types.PersistentAddress],
) error {
	for {
		// It is safe to do deallocations here because deallocated nodes are not reallocated until commit is finalized.
		volatileDeallocator.Deallocate(listRoot.VolatileAddress)
		persistentDeallocator.Deallocate(listRoot.PersistentAddress)

		n := projectNode(appState.Node(listRoot.VolatileAddress))
		for i := range n.NumOfPointerAddresses {
			persistentDeallocator.Deallocate(n.Slots[i])
		}

		if n.Next.VolatileAddress == types.FreeAddress {
			return nil
		}
		listRoot = n.Next
	}
}
