package list

import (
	"sort"

	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/types"
)

// Add adds address to the list.
func Add(
	listRoot *types.NodeAddress,
	nodeAddress types.NodeAddress,
	state *alloc.State,
	allocator *alloc.Allocator,
) error {
	if *listRoot == 0 {
		var err error
		*listRoot, err = allocator.Allocate()
		if err != nil {
			return err
		}
		node := ProjectNode(state.Node(*listRoot))

		node.Slots[0] = nodeAddress
		node.NumOfPointerAddresses = 1
		// This is needed because list nodes are not zeroed.
		node.NumOfSideListAddresses = 0

		return nil
	}

	node := ProjectNode(state.Node(*listRoot))
	if node.NumOfPointerAddresses+node.NumOfSideListAddresses < NumOfAddresses {
		node.Slots[node.NumOfPointerAddresses] = nodeAddress
		node.NumOfPointerAddresses++

		return nil
	}

	newNodeAddress, err := allocator.Allocate()
	if err != nil {
		return err
	}
	node = ProjectNode(state.Node(newNodeAddress))

	node.Slots[0] = nodeAddress
	node.Slots[NumOfAddresses-1] = *listRoot
	node.NumOfPointerAddresses = 1
	node.NumOfSideListAddresses = 1

	*listRoot = newNodeAddress

	return nil
}

// Attach attaches another list.
func Attach(
	listRoot *types.NodeAddress,
	listAddress types.NodeAddress,
	state *alloc.State,
	allocator *alloc.Allocator,
) error {
	if *listRoot == 0 {
		var err error
		*listRoot, err = allocator.Allocate()
		if err != nil {
			return err
		}
		node := ProjectNode(state.Node(*listRoot))

		node.Slots[NumOfAddresses-1] = listAddress
		node.NumOfSideListAddresses = 1
		// This is needed because list nodes are not zeroed.
		node.NumOfPointerAddresses = 0

		return nil
	}

	node := ProjectNode(state.Node(*listRoot))
	if node.NumOfPointerAddresses+node.NumOfSideListAddresses < NumOfAddresses {
		node.NumOfSideListAddresses++
		node.Slots[NumOfAddresses-node.NumOfSideListAddresses] = listAddress

		return nil
	}

	newNodeAddress, err := allocator.Allocate()
	if err != nil {
		return err
	}
	node = ProjectNode(state.Node(newNodeAddress))

	node.Slots[NumOfAddresses-2] = *listRoot
	node.Slots[NumOfAddresses-1] = listAddress
	node.NumOfSideListAddresses = 2
	// This is needed because list nodes are not zeroed.
	node.NumOfPointerAddresses = 0

	*listRoot = newNodeAddress

	return nil
}

// Iterator iterates over items in the list.
func Iterator(listRoot types.NodeAddress, state *alloc.State) func(func(types.NodeAddress) bool) {
	return func(yield func(types.NodeAddress) bool) {
		if listRoot == 0 {
			return
		}

		stack := []types.NodeAddress{listRoot}
		for {
			if len(stack) == 0 {
				return
			}

			volatileAddress := stack[len(stack)-1]
			stack = stack[:len(stack)-1]

			node := ProjectNode(state.Node(volatileAddress))
			for i := range node.NumOfPointerAddresses {
				if !yield(node.Slots[i]) {
					return
				}
			}

			for i, j := uint16(0), NumOfAddresses-1; i < node.NumOfSideListAddresses; i, j = i+1, j-1 {
				stack = append(stack, node.Slots[j])
			}
		}
	}
}

// Nodes returns list of nodes used by the list.
func Nodes(listRoot types.NodeAddress, state *alloc.State) []types.NodeAddress {
	if listRoot == 0 {
		return nil
	}

	nodes := []types.NodeAddress{}
	stack := []types.NodeAddress{listRoot}

	for {
		if len(stack) == 0 {
			sort.Slice(nodes, func(i, j int) bool {
				return nodes[i] < nodes[j]
			})

			return nodes
		}

		volatileAddress := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		nodes = append(nodes, volatileAddress)

		node := ProjectNode(state.Node(volatileAddress))
		for i, j := uint16(0), NumOfAddresses-1; i < node.NumOfSideListAddresses; i, j = i+1, j-1 {
			stack = append(stack, node.Slots[j])
		}
	}
}

// Deallocate deallocates nodes referenced by the list.
func Deallocate(
	listRoot types.NodeAddress,
	state *alloc.State,
	deallocator *alloc.Deallocator,
) {
	if listRoot == 0 {
		return
	}

	const maxStackSize = 5

	stack := [maxStackSize]types.NodeAddress{listRoot}
	stackLen := 1

	for {
		if stackLen == 0 {
			return
		}

		stackLen--
		p := stack[stackLen]
		node := ProjectNode(state.Node(p))

		for i := range node.NumOfPointerAddresses {
			deallocator.Deallocate(node.Slots[i])
		}

		for i, j := uint16(0), NumOfAddresses-1; i < node.NumOfSideListAddresses; i, j = i+1, j-1 {
			if stackLen < maxStackSize {
				stack[stackLen] = node.Slots[j]
				stackLen++

				continue
			}

			Deallocate(
				node.Slots[j],
				state,
				deallocator,
			)
		}

		deallocator.Deallocate(p)
	}
}
