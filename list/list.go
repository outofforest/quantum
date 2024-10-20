package list

import (
	"sort"

	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/types"
)

// Config stores list configuration.
type Config struct {
	ListRoot       *types.Pointer
	State          *alloc.State
	NodeAllocator  *NodeAllocator
	StorageEventCh chan<- any
}

// New creates new list.
func New(config Config) (*List, error) {
	return &List{
		config: config,
	}, nil
}

// List represents the list of node addresses.
type List struct {
	config Config
}

// Add adds address to the list.
func (l *List) Add(
	pointer types.Pointer,
	pool *alloc.Pool[types.LogicalAddress],
	node *Node,
) error {
	if l.config.ListRoot.LogicalAddress == 0 {
		newNodeAddress, err := l.config.NodeAllocator.Allocate(pool, node)
		if err != nil {
			return err
		}
		node.Pointers[0] = pointer
		node.Header.NumOfPointers = 1
		l.config.ListRoot.LogicalAddress = newNodeAddress

		l.config.StorageEventCh <- types.ListNodeAllocatedEvent{
			Pointer: l.config.ListRoot,
		}

		return nil
	}

	l.config.NodeAllocator.Get(l.config.ListRoot.LogicalAddress, node)
	if node.Header.NumOfPointers+node.Header.NumOfSideLists < uint64(len(node.Pointers)) {
		node.Pointers[node.Header.NumOfPointers] = pointer
		node.Header.NumOfPointers++

		l.config.StorageEventCh <- types.ListNodeUpdatedEvent{
			Pointer: l.config.ListRoot,
		}

		return nil
	}

	newNodeAddress, err := l.config.NodeAllocator.Allocate(pool, node)
	if err != nil {
		return err
	}
	node.Pointers[0] = pointer
	node.Pointers[len(node.Pointers)-1] = *l.config.ListRoot
	node.Header.NumOfPointers = 1
	node.Header.NumOfSideLists = 1
	l.config.ListRoot.LogicalAddress = newNodeAddress

	l.config.StorageEventCh <- types.ListNodeAllocatedEvent{
		Pointer: l.config.ListRoot,
	}

	return nil
}

// Attach attaches another list.
func (l *List) Attach(
	pointer types.Pointer,
	pool *alloc.Pool[types.LogicalAddress],
	node *Node,
) error {
	if l.config.ListRoot.LogicalAddress == 0 {
		*l.config.ListRoot = pointer
		return nil
	}

	l.config.NodeAllocator.Get(l.config.ListRoot.LogicalAddress, node)
	if node.Header.NumOfPointers+node.Header.NumOfSideLists < uint64(len(node.Pointers)) {
		node.Pointers[uint64(len(node.Pointers))-node.Header.NumOfSideLists-1] = pointer
		node.Header.NumOfSideLists++

		l.config.StorageEventCh <- types.ListNodeUpdatedEvent{
			Pointer: l.config.ListRoot,
		}

		return nil
	}

	newNodeAddress, err := l.config.NodeAllocator.Allocate(pool, node)
	if err != nil {
		return err
	}
	node.Pointers[uint64(len(node.Pointers))-1] = *l.config.ListRoot
	node.Pointers[uint64(len(node.Pointers))-2] = pointer
	node.Header.NumOfSideLists = 2
	l.config.ListRoot.LogicalAddress = newNodeAddress

	l.config.StorageEventCh <- types.ListNodeUpdatedEvent{
		Pointer: l.config.ListRoot,
	}

	return nil
}

// Iterator iterates over items in the list.
func (l *List) Iterator(node *Node) func(func(types.Pointer) bool) {
	return func(yield func(types.Pointer) bool) {
		if l.config.ListRoot.LogicalAddress == 0 {
			return
		}

		stack := []types.Pointer{*l.config.ListRoot}
		for {
			if len(stack) == 0 {
				return
			}

			pointer := stack[len(stack)-1]
			stack = stack[:len(stack)-1]

			l.config.NodeAllocator.Get(pointer.LogicalAddress, node)
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
func (l *List) Nodes(node *Node) []types.LogicalAddress {
	if l.config.ListRoot.LogicalAddress == 0 {
		return nil
	}

	nodes := []types.LogicalAddress{}
	stack := []types.Pointer{*l.config.ListRoot}

	for {
		if len(stack) == 0 {
			sort.Slice(nodes, func(i, j int) bool {
				return nodes[i] < nodes[j]
			})

			return nodes
		}

		pointer := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		nodes = append(nodes, pointer.LogicalAddress)

		l.config.NodeAllocator.Get(pointer.LogicalAddress, node)
		stack = append(stack, node.Pointers[uint64(len(node.Pointers))-node.Header.NumOfSideLists:]...)
	}
}

// Deallocate deallocates nodes referenced by the list.
func Deallocate(
	state *alloc.State,
	listRoot types.Pointer,
	volatilePool *alloc.Pool[types.LogicalAddress],
	persistentPool *alloc.Pool[types.PhysicalAddress],
	nodeAllocator *NodeAllocator,
	node *Node,
) {
	if listRoot.LogicalAddress == 0 {
		return
	}

	// FIXME (wojciech): Optimize heap allocations.
	stack := []types.Pointer{listRoot}
	for {
		if len(stack) == 0 {
			return
		}

		pointer := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		nodeAllocator.Get(pointer.LogicalAddress, node)
		for i := range node.Header.NumOfPointers {
			// We don't deallocate from volatile pool here, because those nodes are still used by next revisions.
			persistentPool.Deallocate(node.Pointers[i].PhysicalAddress)
		}

		stack = append(stack, node.Pointers[uint64(len(node.Pointers))-node.Header.NumOfSideLists:]...)
		volatilePool.Deallocate(pointer.LogicalAddress)
		persistentPool.Deallocate(pointer.PhysicalAddress)
	}
}
