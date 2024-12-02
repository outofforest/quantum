package list

import (
	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/types"
)

// StoreAddress adds address to the list.
func StoreAddress(
	listTail *types.NodeAddress,
	nodeAddress types.NodeAddress,
	state *alloc.State,
	allocator *alloc.Allocator,
) (bool, error) {
	if *listTail == 0 {
		var err error
		*listTail, err = allocator.Allocate()
		if err != nil {
			return false, err
		}
		node := ProjectNode(state.Node(*listTail))

		node.Slots[0] = nodeAddress
		node.NumOfAddresses = 1
		// This is needed because list nodes are not zeroed.
		node.Next = 0

		return true, nil
	}

	node := ProjectNode(state.Node(*listTail))
	if node.NumOfAddresses < NumOfAddresses {
		node.Slots[node.NumOfAddresses] = nodeAddress
		node.NumOfAddresses++

		return false, nil
	}

	var err error
	*listTail, err = allocator.Allocate()
	if err != nil {
		return false, err
	}
	node.Next = *listTail
	node = ProjectNode(state.Node(*listTail))

	node.Slots[0] = nodeAddress
	node.NumOfAddresses = 1
	// This is needed because list nodes are not zeroed.
	node.Next = 0

	return true, nil
}

// Merge merges two lists.
func Merge(
	list1Front, list1Tail *types.NodeAddress,
	list2Front, list2Tail types.NodeAddress,
	state *alloc.State,
) {
	if list2Front == 0 {
		return
	}
	if *list1Front == 0 {
		*list1Front = list2Front
	} else {
		node := ProjectNode(state.Node(*list1Tail))
		node.Next = list2Front
	}

	*list1Tail = list2Tail
}

// Iterator iterates over addresses in the list.
func Iterator(listFront types.NodeAddress, state *alloc.State) func(func(types.NodeAddress) bool) {
	return func(yield func(types.NodeAddress) bool) {
		for {
			if listFront == 0 {
				return
			}

			node := ProjectNode(state.Node(listFront))
			for i := range node.NumOfAddresses {
				if !yield(node.Slots[i]) {
					return
				}
			}

			listFront = node.Next
		}
	}
}
