package list

import (
	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/types"
)

// Pointer represents pointer to the list root.
type Pointer struct {
	VolatileAddress   types.VolatileAddress
	PersistentAddress types.PersistentAddress
}

// Add adds address to the list.
func Add(
	listRoot *Pointer,
	nodeAddress types.PersistentAddress,
	state *alloc.State,
	volatileAllocator *alloc.Allocator[types.VolatileAddress],
	persistentAllocator *alloc.Allocator[types.PersistentAddress],
) (Pointer, error) {
	if listRoot.VolatileAddress == types.FreeAddress {
		var err error
		listRoot.VolatileAddress, err = volatileAllocator.Allocate()
		if err != nil {
			return Pointer{}, err
		}
		listRoot.PersistentAddress, err = persistentAllocator.Allocate()
		if err != nil {
			return Pointer{}, err
		}
		node := ProjectNode(state.Node(listRoot.VolatileAddress))

		node.Slots[0] = nodeAddress
		node.NumOfPointerAddresses = 1
		// This is needed because list nodes are not zeroed.
		node.Next = Pointer{}

		return Pointer{}, nil
	}

	node := ProjectNode(state.Node(listRoot.VolatileAddress))
	if node.NumOfPointerAddresses < NumOfAddresses {
		node.Slots[node.NumOfPointerAddresses] = nodeAddress
		node.NumOfPointerAddresses++

		return Pointer{}, nil
	}

	oldRoot := *listRoot

	var err error
	listRoot.VolatileAddress, err = volatileAllocator.Allocate()
	if err != nil {
		return Pointer{}, err
	}

	listRoot.PersistentAddress, err = persistentAllocator.Allocate()
	if err != nil {
		return Pointer{}, err
	}
	node = ProjectNode(state.Node(listRoot.VolatileAddress))

	node.Slots[0] = nodeAddress
	node.NumOfPointerAddresses = 1
	node.Next = oldRoot

	return oldRoot, nil
}

// Deallocate deallocates nodes referenced by the list.
func Deallocate(
	listRoot Pointer,
	state *alloc.State,
	volatileDeallocator *alloc.Deallocator[types.VolatileAddress],
	persistentDeallocator *alloc.Deallocator[types.PersistentAddress],
) error {
	for {
		// It is safe to do deallocations here because deallocated nodes are not reallocated until commit is finalized.
		volatileDeallocator.Deallocate(listRoot.VolatileAddress)
		persistentDeallocator.Deallocate(listRoot.PersistentAddress)

		node := ProjectNode(state.Node(listRoot.VolatileAddress))
		for i := range node.NumOfPointerAddresses {
			persistentDeallocator.Deallocate(node.Slots[i])
		}

		if node.Next.VolatileAddress == types.FreeAddress {
			return nil
		}
		listRoot = node.Next
	}
}
