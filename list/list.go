package list

import (
	"sort"

	"github.com/outofforest/quantum/types"
)

// Config stores list configuration.
type Config struct {
	ListRoot          *types.Pointer
	Allocator         types.Allocator
	SnapshotAllocator types.SnapshotAllocator
	StorageEventCh    chan<- any
}

// New creates new list.
func New(config Config) (*List, error) {
	nodeAllocator, err := NewNodeAllocator(config.Allocator)
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
func (l *List) Add(pointer types.Pointer) error {
	if l.config.ListRoot.LogicalAddress == 0 {
		newNodeAddress, newNode, err := l.nodeAllocator.Allocate(l.config.SnapshotAllocator)
		if err != nil {
			return err
		}
		newNode.Pointers[0] = pointer
		newNode.Header.NumOfItems = 1
		l.config.ListRoot.LogicalAddress = newNodeAddress

		l.config.StorageEventCh <- types.ListNodeAllocatedEvent{
			Pointer: l.config.ListRoot,
		}

		return nil
	}

	listNode := l.nodeAllocator.Get(l.config.ListRoot.LogicalAddress)
	if listNode.Header.NumOfItems+listNode.Header.NumOfSideLists < uint64(len(listNode.Pointers)) {
		listNode.Pointers[listNode.Header.NumOfItems] = pointer
		listNode.Header.NumOfItems++

		l.config.StorageEventCh <- types.ListNodeUpdatedEvent{
			Pointer: l.config.ListRoot,
		}

		return nil
	}

	newNodeAddress, newNode, err := l.nodeAllocator.Allocate(l.config.SnapshotAllocator)
	if err != nil {
		return err
	}
	newNode.Pointers[0] = pointer
	newNode.Pointers[len(newNode.Pointers)-1] = *l.config.ListRoot
	newNode.Header.NumOfItems = 1
	newNode.Header.NumOfSideLists = 1
	l.config.ListRoot.LogicalAddress = newNodeAddress

	l.config.StorageEventCh <- types.ListNodeAllocatedEvent{
		Pointer: l.config.ListRoot,
	}

	return nil
}

// Attach attaches another list.
func (l *List) Attach(pointer types.Pointer) error {
	if l.config.ListRoot.LogicalAddress == 0 {
		*l.config.ListRoot = pointer
		return nil
	}

	listNode := l.nodeAllocator.Get(l.config.ListRoot.LogicalAddress)
	if listNode.Header.NumOfItems+listNode.Header.NumOfSideLists < uint64(len(listNode.Pointers)) {
		listNode.Pointers[uint64(len(listNode.Pointers))-listNode.Header.NumOfSideLists-1] = pointer
		listNode.Header.NumOfSideLists++

		l.config.StorageEventCh <- types.ListNodeUpdatedEvent{
			Pointer: l.config.ListRoot,
		}

		return nil
	}

	newNodeAddress, newNode, err := l.nodeAllocator.Allocate(l.config.SnapshotAllocator)
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

// Deallocate deallocates nodes referenced by the list.
func (l *List) Deallocate() {
	if l.config.ListRoot.LogicalAddress == 0 {
		return
	}

	// FIXME (wojciech): Optimize heap allocations.
	stack := []types.Pointer{*l.config.ListRoot}
	for {
		if len(stack) == 0 {
			return
		}

		pointer := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		listNode := l.nodeAllocator.Get(pointer.LogicalAddress)
		// FIXME (wojciech): No-copy test
		// for i := range listNode.Header.NumOfItems {
		//	allocator.Deallocate(listNode.Pointers[i])
		// }

		stack = append(stack, listNode.Pointers[uint64(len(listNode.Pointers))-listNode.Header.NumOfSideLists:]...)
		l.config.Allocator.Deallocate(pointer.LogicalAddress)
	}
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
			for i := range listNode.Header.NumOfItems {
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
