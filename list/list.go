package list

import (
	"sort"

	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/types"
)

// Config stores list configuration.
type Config struct {
	Root          *types.Pointer
	State         *alloc.State
	NodeAssistant *NodeAssistant
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
	node *Node,
) (*types.Pointer, error) {
	if l.config.Root.State == types.StateFree {
		newNodeAddress, err := volatilePool.Allocate()
		if err != nil {
			return nil, err
		}
		l.config.NodeAssistant.Project(newNodeAddress, node)

		node.Pointers[0] = *pointer
		node.Header.NumOfPointers = 1

		l.config.Root.VolatileAddress = newNodeAddress
		l.config.Root.State = types.StateData

		return l.config.Root, nil
	}

	l.config.NodeAssistant.Project(l.config.Root.VolatileAddress, node)
	if node.Header.NumOfPointers+node.Header.NumOfSideLists < uint64(len(node.Pointers)) {
		node.Pointers[node.Header.NumOfPointers] = *pointer
		node.Header.NumOfPointers++

		return l.config.Root, nil
	}

	newNodeAddress, err := volatilePool.Allocate()
	if err != nil {
		return nil, err
	}
	l.config.NodeAssistant.Project(newNodeAddress, node)

	node.Pointers[0] = *pointer
	node.Pointers[len(node.Pointers)-1] = *l.config.Root
	node.Header.NumOfPointers = 1
	node.Header.NumOfSideLists = 1

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
	node *Node,
) (*types.Pointer, error) {
	if l.config.Root.State == types.StateFree {
		*l.config.Root = *pointer

		//nolint:nilnil
		return nil, nil
	}

	l.config.NodeAssistant.Project(l.config.Root.VolatileAddress, node)
	if node.Header.NumOfPointers+node.Header.NumOfSideLists < uint64(len(node.Pointers)) {
		node.Header.NumOfSideLists++
		node.Pointers[uint64(len(node.Pointers))-node.Header.NumOfSideLists] = *pointer

		return l.config.Root, nil
	}

	newNodeAddress, err := volatilePool.Allocate()
	if err != nil {
		return nil, err
	}
	l.config.NodeAssistant.Project(newNodeAddress, node)

	node.Pointers[uint64(len(node.Pointers))-1] = *l.config.Root
	node.Pointers[uint64(len(node.Pointers))-2] = *pointer
	node.Header.NumOfSideLists = 2

	l.config.Root = &types.Pointer{
		VolatileAddress: newNodeAddress,
		State:           types.StateData,
	}

	return l.config.Root, nil
}

// Iterator iterates over items in the list.
func (l *List) Iterator(node *Node) func(func(types.Pointer) bool) {
	return func(yield func(types.Pointer) bool) {
		if l.config.Root.VolatileAddress == 0 {
			return
		}

		stack := []types.Pointer{*l.config.Root}
		for {
			if len(stack) == 0 {
				return
			}

			pointer := stack[len(stack)-1]
			stack = stack[:len(stack)-1]

			l.config.NodeAssistant.Project(pointer.VolatileAddress, node)
			for i := range node.Header.NumOfPointers {
				if !yield(node.Pointers[i]) {
					return
				}
			}

			stack = append(stack, node.Pointers[uint64(len(node.Pointers))-node.Header.NumOfSideLists:]...)
		}
	}
}

// Nodes returns list of nodes used by the list.
func (l *List) Nodes(node *Node) []types.VolatileAddress {
	if l.config.Root.VolatileAddress == 0 {
		return nil
	}

	nodes := []types.VolatileAddress{}
	stack := []types.Pointer{*l.config.Root}

	for {
		if len(stack) == 0 {
			sort.Slice(nodes, func(i, j int) bool {
				return nodes[i] < nodes[j]
			})

			return nodes
		}

		pointer := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		nodes = append(nodes, pointer.VolatileAddress)

		l.config.NodeAssistant.Project(pointer.VolatileAddress, node)
		stack = append(stack, node.Pointers[uint64(len(node.Pointers))-node.Header.NumOfSideLists:]...)
	}
}

// Deallocate deallocates nodes referenced by the list.
func Deallocate(
	listRoot types.Pointer,
	volatilePool *alloc.Pool[types.VolatileAddress],
	persistentPool *alloc.Pool[types.PersistentAddress],
	nodeAssistant *NodeAssistant,
	node *Node,
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
		pointer := stack[stackLen]

		nodeAssistant.Project(pointer.VolatileAddress, node)
		for i := range node.Header.NumOfPointers {
			// We don't deallocate from volatile pool here, because those nodes are still used by next revisions.
			persistentPool.Deallocate(node.Pointers[i].PersistentAddress)
		}

		for _, p := range node.Pointers[uint64(len(node.Pointers))-node.Header.NumOfSideLists:] {
			if stackLen < maxStackSize {
				stack[stackLen] = p
				stackLen++

				continue
			}

			Deallocate(
				p,
				volatilePool,
				persistentPool,
				nodeAssistant,
				node,
			)
		}

		volatilePool.Deallocate(pointer.VolatileAddress)
		persistentPool.Deallocate(pointer.PersistentAddress)
	}
}
