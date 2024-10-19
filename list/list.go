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
	StorageEventCh chan<- any
}

// New creates new list.
func New(config Config) (*List, error) {
	nodeAllocator, err := NewNodeAllocator(config.State)
	if err != nil {
		return nil, err
	}

	return &List{
		config:        config,
		nodeAllocator: nodeAllocator,
	}, nil
}

// List represents the list of node addresses.
type List struct {
	config        Config
	nodeAllocator *NodeAllocator
}

// Add adds address to the list.
func (l *List) Add(pointer types.Pointer, pool *alloc.Pool[types.LogicalAddress]) error {
	if l.config.ListRoot.LogicalAddress == 0 {
		newNodeAddress, newNode, err := l.nodeAllocator.Allocate(pool)
		if err != nil {
			return err
		}
		newNode.Pointers[0] = pointer
		newNode.Header.NumOfPointers = 1
		l.config.ListRoot.LogicalAddress = newNodeAddress

		l.config.StorageEventCh <- types.ListNodeAllocatedEvent{
			Pointer: l.config.ListRoot,
		}

		return nil
	}

	listNode := l.nodeAllocator.Get(l.config.ListRoot.LogicalAddress)
	if listNode.Header.NumOfPointers+listNode.Header.NumOfSideLists < uint64(len(listNode.Pointers)) {
		listNode.Pointers[listNode.Header.NumOfPointers] = pointer
		listNode.Header.NumOfPointers++

		l.config.StorageEventCh <- types.ListNodeUpdatedEvent{
			Pointer: l.config.ListRoot,
		}

		return nil
	}

	newNodeAddress, newNode, err := l.nodeAllocator.Allocate(pool)
	if err != nil {
		return err
	}
	newNode.Pointers[0] = pointer
	newNode.Pointers[len(newNode.Pointers)-1] = *l.config.ListRoot
	newNode.Header.NumOfPointers = 1
	newNode.Header.NumOfSideLists = 1
	l.config.ListRoot.LogicalAddress = newNodeAddress

	l.config.StorageEventCh <- types.ListNodeAllocatedEvent{
		Pointer: l.config.ListRoot,
	}

	return nil
}

// Attach attaches another list.
func (l *List) Attach(pointer types.Pointer, pool *alloc.Pool[types.LogicalAddress]) error {
	if l.config.ListRoot.LogicalAddress == 0 {
		*l.config.ListRoot = pointer
		return nil
	}

	listNode := l.nodeAllocator.Get(l.config.ListRoot.LogicalAddress)
	if listNode.Header.NumOfPointers+listNode.Header.NumOfSideLists < uint64(len(listNode.Pointers)) {
		listNode.Pointers[uint64(len(listNode.Pointers))-listNode.Header.NumOfSideLists-1] = pointer
		listNode.Header.NumOfSideLists++

		l.config.StorageEventCh <- types.ListNodeUpdatedEvent{
			Pointer: l.config.ListRoot,
		}

		return nil
	}

	newNodeAddress, newNode, err := l.nodeAllocator.Allocate(pool)
	if err != nil {
		return err
	}
	newNode.Pointers[uint64(len(listNode.Pointers))-1] = *l.config.ListRoot
	newNode.Pointers[uint64(len(listNode.Pointers))-2] = pointer
	newNode.Header.NumOfSideLists = 2
	l.config.ListRoot.LogicalAddress = newNodeAddress

	l.config.StorageEventCh <- types.ListNodeUpdatedEvent{
		Pointer: l.config.ListRoot,
	}

	return nil
}

// Iterator iterates over items in the list.
func (l *List) Iterator() func(func(types.Pointer) bool) {
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

			listNode := l.nodeAllocator.Get(pointer.LogicalAddress)
			for i := range listNode.Header.NumOfPointers {
				if !yield(listNode.Pointers[i]) {
					return
				}
			}

			stack = append(stack, listNode.Pointers[uint64(len(listNode.Pointers))-listNode.Header.NumOfSideLists:]...)
		}
	}
}

// Nodes returns list of nodes used by the list.
func (l *List) Nodes() []types.LogicalAddress {
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

		listNode := l.nodeAllocator.Get(pointer.LogicalAddress)
		stack = append(stack, listNode.Pointers[uint64(len(listNode.Pointers))-listNode.Header.NumOfSideLists:]...)
	}
}

// Deallocate deallocates nodes referenced by the list.
func Deallocate(
	state *alloc.State,
	listRoot types.Pointer,
	volatilePool *alloc.Pool[types.LogicalAddress],
	persistentPool *alloc.Pool[types.PhysicalAddress],
) error {
	if listRoot.LogicalAddress == 0 {
		return nil
	}

	nodeAllocator, err := NewNodeAllocator(state)
	if err != nil {
		return err
	}

	// FIXME (wojciech): Optimize heap allocations.
	stack := []types.Pointer{listRoot}
	for {
		if len(stack) == 0 {
			return nil
		}

		pointer := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		listNode := nodeAllocator.Get(pointer.LogicalAddress)
		for i := range listNode.Header.NumOfPointers {
			// We don't deallocate from volatile pool here, because those nodes are still used by next revisions.
			persistentPool.Deallocate(listNode.Pointers[i].PhysicalAddress)
		}

		stack = append(stack, listNode.Pointers[uint64(len(listNode.Pointers))-listNode.Header.NumOfSideLists:]...)
		volatilePool.Deallocate(pointer.LogicalAddress)
		persistentPool.Deallocate(pointer.PhysicalAddress)
	}
}
