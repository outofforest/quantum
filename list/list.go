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
func (l *List) Add(
	pointer *types.Pointer,
	volatilePool *alloc.Pool[types.VolatileAddress],
) (*types.Pointer, error) {
	if l.config.Root.State == types.StateFree {
		newNodeAddress, err := volatilePool.Allocate()
		if err != nil {
			return nil, err
		}
		node := ProjectNode(l.config.State.Node(newNodeAddress))

		node.Slots[0] = uint64(pointer.PersistentAddress)
		node.NumOfPointerSlots = 1

		l.config.Root.VolatileAddress = newNodeAddress
		l.config.Root.State = types.StateData

		return l.config.Root, nil
	}

	node := ProjectNode(l.config.State.Node(l.config.Root.VolatileAddress))
	if node.NumOfPointerSlots+node.NumOfSideListSlots < NumOfSlots {
		node.Slots[node.NumOfPointerSlots] = uint64(pointer.PersistentAddress)
		node.NumOfPointerSlots++

		return l.config.Root, nil
	}

	newNodeAddress, err := volatilePool.Allocate()
	if err != nil {
		return nil, err
	}
	node = ProjectNode(l.config.State.Node(newNodeAddress))

	node.Slots[0] = uint64(pointer.PersistentAddress)
	node.Slots[NumOfSlots-2] = uint64(l.config.Root.VolatileAddress)
	node.Slots[NumOfSlots-1] = uint64(l.config.Root.PersistentAddress)
	node.NumOfPointerSlots = 1
	node.NumOfSideListSlots = 2

	l.config.Root = &types.Pointer{
		VolatileAddress: newNodeAddress,
		State:           types.StateData,
	}

	return l.config.Root, nil
}

// Attach attaches another list.
func (l *List) Attach(
	pointer *types.Pointer,
	volatilePool *alloc.Pool[types.VolatileAddress],
) (*types.Pointer, error) {
	if l.config.Root.State == types.StateFree {
		*l.config.Root = *pointer

		//nolint:nilnil
		return nil, nil
	}

	node := ProjectNode(l.config.State.Node(l.config.Root.VolatileAddress))
	if node.NumOfPointerSlots+node.NumOfSideListSlots < NumOfSlots-1 {
		node.NumOfSideListSlots += 2
		node.Slots[NumOfSlots-node.NumOfSideListSlots] = uint64(pointer.VolatileAddress)
		node.Slots[NumOfSlots-node.NumOfSideListSlots+1] = uint64(pointer.PersistentAddress)

		return l.config.Root, nil
	}

	newNodeAddress, err := volatilePool.Allocate()
	if err != nil {
		return nil, err
	}
	node = ProjectNode(l.config.State.Node(newNodeAddress))

	node.Slots[NumOfSlots-4] = uint64(l.config.Root.VolatileAddress)
	node.Slots[NumOfSlots-3] = uint64(l.config.Root.PersistentAddress)
	node.Slots[NumOfSlots-2] = uint64(pointer.VolatileAddress)
	node.Slots[NumOfSlots-1] = uint64(pointer.PersistentAddress)
	node.NumOfSideListSlots = 4

	l.config.Root = &types.Pointer{
		VolatileAddress: newNodeAddress,
		State:           types.StateData,
	}

	return l.config.Root, nil
}

// Iterator iterates over items in the list.
func (l *List) Iterator() func(func(types.VolatileAddress) bool) {
	return func(yield func(types.VolatileAddress) bool) {
		if l.config.Root.VolatileAddress == 0 {
			return
		}

		stack := []types.VolatileAddress{l.config.Root.VolatileAddress}
		for {
			if len(stack) == 0 {
				return
			}

			volatileAddress := stack[len(stack)-1]
			stack = stack[:len(stack)-1]

			node := ProjectNode(l.config.State.Node(volatileAddress))
			for i := range node.NumOfPointerSlots {
				if !yield(types.VolatileAddress(node.Slots[i])) {
					return
				}
			}

			for i, j := uint16(0), NumOfSlots-2; i < node.NumOfSideListSlots; i, j = i+2, j-2 {
				stack = append(stack, types.VolatileAddress(node.Slots[j]))
			}
		}
	}
}

// Nodes returns list of nodes used by the list.
func (l *List) Nodes() []types.VolatileAddress {
	if l.config.Root.VolatileAddress == 0 {
		return nil
	}

	nodes := []types.VolatileAddress{}
	stack := []types.VolatileAddress{l.config.Root.VolatileAddress}

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
		for i, j := uint16(0), NumOfSlots-2; i < node.NumOfSideListSlots; i, j = i+2, j-2 {
			stack = append(stack, types.VolatileAddress(node.Slots[j]))
		}
	}
}

// Deallocate deallocates nodes referenced by the list.
func Deallocate(
	listRoot types.Pointer,
	state *alloc.State,
	volatilePool *alloc.Pool[types.VolatileAddress],
	persistentPool *alloc.Pool[types.PersistentAddress],
) {
	if listRoot.VolatileAddress == 0 {
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

		for i := range node.NumOfPointerSlots {
			// We don't deallocate from volatile pool here, because those nodes are still used by next revisions.
			persistentPool.Deallocate(types.PersistentAddress(node.Slots[i]))
		}

		for i, j := uint16(0), NumOfSlots-2; i < node.NumOfSideListSlots; i, j = i+2, j-2 {
			if stackLen < maxStackSize {
				stack[stackLen].VolatileAddress = types.VolatileAddress(node.Slots[j])
				stack[stackLen].PersistentAddress = types.PersistentAddress(node.Slots[j+1])
				stackLen++

				continue
			}

			Deallocate(
				types.Pointer{
					VolatileAddress:   types.VolatileAddress(node.Slots[j]),
					PersistentAddress: types.PersistentAddress(node.Slots[j+1]),
				},
				state,
				volatilePool,
				persistentPool,
			)
		}

		volatilePool.Deallocate(p.VolatileAddress)
		persistentPool.Deallocate(p.PersistentAddress)
	}
}
