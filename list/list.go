package list

import (
	"sort"

	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/types"
)

// Config stores list configuration.
type Config struct {
	Root  types.NodeAddress
	State *alloc.State
}

// New creates new list.
func New(config Config) *List {
	return &List{
		config: config,
	}
}

// List represents the list of node addresses.
type List struct {
	config Config
}

// Add adds address to the list.
func (l *List) Add(nodeAddress types.NodeAddress, allocator *alloc.Allocator) (types.NodeAddress, error) {
	if l.config.Root == 0 {
		newNodeAddress, err := allocator.Allocate()
		if err != nil {
			return 0, err
		}
		node := ProjectNode(l.config.State.Node(newNodeAddress))

		node.Slots[0] = nodeAddress
		node.NumOfPointerAddresses = 1
		// This is needed because list nodes are not zeroed.
		node.NumOfSideListAddresses = 0

		l.config.Root = newNodeAddress

		return l.config.Root, nil
	}

	node := ProjectNode(l.config.State.Node(l.config.Root))
	if node.NumOfPointerAddresses+node.NumOfSideListAddresses < NumOfAddresses {
		node.Slots[node.NumOfPointerAddresses] = nodeAddress
		node.NumOfPointerAddresses++

		return l.config.Root, nil
	}

	newNodeAddress, err := allocator.Allocate()
	if err != nil {
		return 0, err
	}
	node = ProjectNode(l.config.State.Node(newNodeAddress))

	node.Slots[0] = nodeAddress
	node.Slots[NumOfAddresses-1] = l.config.Root
	node.NumOfPointerAddresses = 1
	node.NumOfSideListAddresses = 1

	l.config.Root = newNodeAddress

	return l.config.Root, nil
}

// Attach attaches another list.
func (l *List) Attach(listAddress types.NodeAddress, allocator *alloc.Allocator) (types.NodeAddress, error) {
	if l.config.Root == 0 {
		newNodeAddress, err := allocator.Allocate()
		if err != nil {
			return 0, err
		}
		node := ProjectNode(l.config.State.Node(newNodeAddress))

		node.Slots[NumOfAddresses-1] = listAddress
		node.NumOfSideListAddresses = 1
		// This is needed because list nodes are not zeroed.
		node.NumOfPointerAddresses = 0

		l.config.Root = newNodeAddress

		return l.config.Root, nil
	}

	node := ProjectNode(l.config.State.Node(l.config.Root))
	if node.NumOfPointerAddresses+node.NumOfSideListAddresses < NumOfAddresses {
		node.NumOfSideListAddresses++
		node.Slots[NumOfAddresses-node.NumOfSideListAddresses] = listAddress

		return l.config.Root, nil
	}

	newNodeAddress, err := allocator.Allocate()
	if err != nil {
		return 0, err
	}
	node = ProjectNode(l.config.State.Node(newNodeAddress))

	node.Slots[NumOfAddresses-2] = l.config.Root
	node.Slots[NumOfAddresses-1] = listAddress
	node.NumOfSideListAddresses = 2
	// This is needed because list nodes are not zeroed.
	node.NumOfPointerAddresses = 0

	l.config.Root = newNodeAddress

	return l.config.Root, nil
}

// Iterator iterates over items in the list.
func (l *List) Iterator() func(func(types.NodeAddress) bool) {
	return func(yield func(types.NodeAddress) bool) {
		if l.config.Root == 0 {
			return
		}

		stack := []types.NodeAddress{l.config.Root}
		for {
			if len(stack) == 0 {
				return
			}

			volatileAddress := stack[len(stack)-1]
			stack = stack[:len(stack)-1]

			node := ProjectNode(l.config.State.Node(volatileAddress))
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
func (l *List) Nodes() []types.NodeAddress {
	if l.config.Root == 0 {
		return nil
	}

	nodes := []types.NodeAddress{}
	stack := []types.NodeAddress{l.config.Root}

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

		node := ProjectNode(l.config.State.Node(volatileAddress))
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
