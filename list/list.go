package list

import (
	"sort"

	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/types"
)

// Config stores list configuration.
type Config struct {
	Root  *types.Pointer
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
func (l *List) Add(nodeAddress types.NodeAddress, allocator *alloc.Allocator) (*types.Pointer, error) {
	if l.config.Root.State == types.StateFree {
		newNodeAddress, err := allocator.Allocate()
		if err != nil {
			return nil, err
		}
		node := ProjectNode(l.config.State.Node(newNodeAddress))

		node.Slots[0] = nodeAddress
		node.NumOfPointerAddresses = 1
		// This is needed because list nodes are not zeroed.
		node.NumOfSideListAddresses = 0

		l.config.Root.VolatileAddress = newNodeAddress
		l.config.Root.State = types.StateData

		return l.config.Root, nil
	}

	node := ProjectNode(l.config.State.Node(l.config.Root.VolatileAddress))
	if node.NumOfPointerAddresses+node.NumOfSideListAddresses < NumOfAddresses {
		node.Slots[node.NumOfPointerAddresses] = nodeAddress
		node.NumOfPointerAddresses++

		return l.config.Root, nil
	}

	newNodeAddress, err := allocator.Allocate()
	if err != nil {
		return nil, err
	}
	node = ProjectNode(l.config.State.Node(newNodeAddress))

	node.Slots[0] = nodeAddress
	node.Slots[NumOfAddresses-2] = l.config.Root.VolatileAddress
	node.Slots[NumOfAddresses-1] = l.config.Root.PersistentAddress
	node.NumOfPointerAddresses = 1
	node.NumOfSideListAddresses = 2

	l.config.Root = &types.Pointer{
		VolatileAddress: newNodeAddress,
		State:           types.StateData,
	}

	return l.config.Root, nil
}

// Attach attaches another list.
func (l *List) Attach(pointer *types.Pointer, allocator *alloc.Allocator) (*types.Pointer, error) {
	if l.config.Root.State == types.StateFree {
		*l.config.Root = *pointer

		//nolint:nilnil
		return nil, nil
	}

	node := ProjectNode(l.config.State.Node(l.config.Root.VolatileAddress))
	if node.NumOfPointerAddresses+node.NumOfSideListAddresses < NumOfAddresses-1 {
		node.NumOfSideListAddresses += 2
		node.Slots[NumOfAddresses-node.NumOfSideListAddresses] = pointer.VolatileAddress
		node.Slots[NumOfAddresses-node.NumOfSideListAddresses+1] = pointer.PersistentAddress

		return l.config.Root, nil
	}

	newNodeAddress, err := allocator.Allocate()
	if err != nil {
		return nil, err
	}
	node = ProjectNode(l.config.State.Node(newNodeAddress))

	node.Slots[NumOfAddresses-4] = l.config.Root.VolatileAddress
	node.Slots[NumOfAddresses-3] = l.config.Root.PersistentAddress
	node.Slots[NumOfAddresses-2] = pointer.VolatileAddress
	node.Slots[NumOfAddresses-1] = pointer.PersistentAddress
	node.NumOfSideListAddresses = 4
	// This is needed because list nodes are not zeroed.
	node.NumOfPointerAddresses = 0

	l.config.Root = &types.Pointer{
		VolatileAddress: newNodeAddress,
		State:           types.StateData,
	}

	return l.config.Root, nil
}

// Iterator iterates over items in the list.
func (l *List) Iterator() func(func(types.NodeAddress) bool) {
	return func(yield func(types.NodeAddress) bool) {
		if l.config.Root.VolatileAddress == 0 {
			return
		}

		stack := []types.NodeAddress{l.config.Root.VolatileAddress}
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

			for i, j := uint16(0), NumOfAddresses-2; i < node.NumOfSideListAddresses; i, j = i+2, j-2 {
				stack = append(stack, node.Slots[j])
			}
		}
	}
}

// Nodes returns list of nodes used by the list.
func (l *List) Nodes() []types.NodeAddress {
	if l.config.Root.VolatileAddress == 0 {
		return nil
	}

	nodes := []types.NodeAddress{}
	stack := []types.NodeAddress{l.config.Root.VolatileAddress}

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
		for i, j := uint16(0), NumOfAddresses-2; i < node.NumOfSideListAddresses; i, j = i+2, j-2 {
			stack = append(stack, node.Slots[j])
		}
	}
}

// Deallocate deallocates nodes referenced by the list.
func Deallocate(
	listRoot types.Pointer,
	state *alloc.State,
	deallocator *alloc.Deallocator,
) {
	if listRoot.State == types.StateFree {
		return
	}

	const maxStackSize = 5

	stack := [maxStackSize]types.Pointer{listRoot}
	stackLen := 1

	for {
		if stackLen == 0 {
			return
		}

		stackLen--
		p := stack[stackLen]
		node := ProjectNode(state.Node(p.VolatileAddress))

		for i := range node.NumOfPointerAddresses {
			// We don't deallocate from volatile pool here, because those nodes are still used by next revisions.
			deallocator.Deallocate(node.Slots[i])
		}

		for i, j := uint16(0), NumOfAddresses-2; i < node.NumOfSideListAddresses; i, j = i+2, j-2 {
			if stackLen < maxStackSize {
				stack[stackLen].VolatileAddress = node.Slots[j]
				stack[stackLen].PersistentAddress = node.Slots[j+1]
				stackLen++

				continue
			}

			Deallocate(
				types.Pointer{
					VolatileAddress:   node.Slots[j],
					PersistentAddress: node.Slots[j+1],
				},
				state,
				deallocator,
			)
		}

		deallocator.Deallocate(p.VolatileAddress)
		deallocator.Deallocate(p.PersistentAddress)
	}
}
