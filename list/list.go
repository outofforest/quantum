package list

import (
	"unsafe"

	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/persistent"
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
		node.Next = 0

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
	node.Next = oldRoot.PersistentAddress

	return oldRoot, nil
}

// Deallocate deallocates nodes referenced by the list.
func Deallocate(
	listRoot types.PersistentAddress,
	store *persistent.FileStore,
	deallocator *alloc.Deallocator[types.PersistentAddress],
	nodeBuff unsafe.Pointer,
) error {
	for {
		if listRoot == 0 {
			return nil
		}

		if err := store.Read(listRoot, nodeBuff); err != nil {
			return err
		}

		node := ProjectNode(nodeBuff)
		for i := range node.NumOfPointerAddresses {
			deallocator.Deallocate(node.Slots[i])
		}

		deallocator.Deallocate(listRoot)
		listRoot = node.Next
	}
}
