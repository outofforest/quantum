package list

import (
	"sort"

	"github.com/outofforest/quantum/types"
)

// Config stores list configuration.
type Config struct {
	SnapshotID    types.SnapshotID
	Item          *types.NodeAddress
	NodeAllocator NodeAllocator
	Allocator     types.SnapshotAllocator
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
func (l *List) Add(nodeAddress types.NodeAddress) error {
	if *l.config.Item == 0 {
		newNodeAddress, newNode, err := l.config.NodeAllocator.Allocate(l.config.Allocator)
		if err != nil {
			return err
		}
		newNode.Items[0] = nodeAddress
		newNode.Header.SnapshotID = l.config.SnapshotID
		newNode.Header.NumOfItems = 1
		*l.config.Item = newNodeAddress

		return nil
	}
	listNode := l.config.NodeAllocator.Get(*l.config.Item)
	//nolint:nestif
	if listNode.Header.NumOfItems+listNode.Header.NumOfSideLists < uint64(len(listNode.Items)) {
		if listNode.Header.SnapshotID < l.config.SnapshotID {
			newNodeAddress, newNode, err := l.config.NodeAllocator.Copy(l.config.Allocator, *l.config.Item)
			if err != nil {
				return err
			}
			newNode.Header.SnapshotID = l.config.SnapshotID
			oldNodeAddress := *l.config.Item
			*l.config.Item = newNodeAddress
			if err := l.config.Allocator.Deallocate(oldNodeAddress, listNode.Header.SnapshotID); err != nil {
				return err
			}
			listNode = newNode
		}

		listNode.Items[listNode.Header.NumOfItems] = nodeAddress
		listNode.Header.NumOfItems++

		return nil
	}

	newNodeAddress, newNode, err := l.config.NodeAllocator.Allocate(l.config.Allocator)
	if err != nil {
		return err
	}
	newNode.Items[0] = nodeAddress
	newNode.Items[len(newNode.Items)-1] = *l.config.Item
	newNode.Header.SnapshotID = l.config.SnapshotID
	newNode.Header.NumOfItems = 1
	newNode.Header.NumOfSideLists = 1
	*l.config.Item = newNodeAddress

	return nil
}

// Attach attaches another list.
func (l *List) Attach(nodeAddress types.NodeAddress) error {
	if *l.config.Item == 0 {
		*l.config.Item = nodeAddress
		return nil
	}
	listNode := l.config.NodeAllocator.Get(*l.config.Item)
	//nolint:nestif
	if listNode.Header.NumOfItems+listNode.Header.NumOfSideLists < uint64(len(listNode.Items)) {
		if listNode.Header.SnapshotID < l.config.SnapshotID {
			newNodeAddress, newNode, err := l.config.NodeAllocator.Copy(l.config.Allocator, *l.config.Item)
			if err != nil {
				return err
			}
			newNode.Header.SnapshotID = l.config.SnapshotID
			oldNodeAddress := *l.config.Item
			*l.config.Item = newNodeAddress
			if err := l.config.Allocator.Deallocate(oldNodeAddress, listNode.Header.SnapshotID); err != nil {
				return err
			}
			listNode = newNode
		}

		listNode.Items[uint64(len(listNode.Items))-listNode.Header.NumOfSideLists-1] = nodeAddress
		listNode.Header.NumOfSideLists++

		return nil
	}

	newNodeAddress, newNode, err := l.config.NodeAllocator.Allocate(l.config.Allocator)
	if err != nil {
		return err
	}
	newNode.Items[uint64(len(listNode.Items))-1] = *l.config.Item
	newNode.Items[uint64(len(listNode.Items))-2] = nodeAddress
	newNode.Header.SnapshotID = l.config.SnapshotID
	newNode.Header.NumOfSideLists = 2
	*l.config.Item = newNodeAddress

	return nil
}

// Deallocate deallocates nodes referenced by the list.
func (l *List) Deallocate(allocator types.Allocator) {
	if *l.config.Item == 0 {
		return
	}

	// FIXME (wojciech): Optimize heap allocations.
	stack := []types.NodeAddress{*l.config.Item}
	for {
		if len(stack) == 0 {
			return
		}

		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		listNode := l.config.NodeAllocator.Get(n)
		// FIXME (wojciech): No-copy test
		// for i := range listNode.Header.NumOfItems {
		//	allocator.Deallocate(listNode.Items[i])
		// }

		stack = append(stack, listNode.Items[uint64(len(listNode.Items))-listNode.Header.NumOfSideLists:]...)
		allocator.Deallocate(n)
	}
}

// Iterator iterates over items in the list.
func (l *List) Iterator() func(func(types.NodeAddress) bool) {
	return func(yield func(types.NodeAddress) bool) {
		if *l.config.Item == 0 {
			return
		}

		stack := []types.NodeAddress{*l.config.Item}
		for {
			if len(stack) == 0 {
				return
			}

			n := stack[len(stack)-1]
			stack = stack[:len(stack)-1]

			listNode := l.config.NodeAllocator.Get(n)
			for i := range listNode.Header.NumOfItems {
				if !yield(listNode.Items[i]) {
					return
				}
			}

			stack = append(stack, listNode.Items[uint64(len(listNode.Items))-listNode.Header.NumOfSideLists:]...)
		}
	}
}

// Nodes returns list of nodes used by the list.
func (l *List) Nodes() []types.NodeAddress {
	if *l.config.Item == 0 {
		return nil
	}

	nodes := []types.NodeAddress{}
	stack := []types.NodeAddress{*l.config.Item}

	for {
		if len(stack) == 0 {
			sort.Slice(nodes, func(i, j int) bool {
				return nodes[i] < nodes[j]
			})

			return nodes
		}

		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		nodes = append(nodes, n)

		listNode := l.config.NodeAllocator.Get(n)
		stack = append(stack, listNode.Items[uint64(len(listNode.Items))-listNode.Header.NumOfSideLists:]...)
	}
}
