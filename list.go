package quantum

// ListConfig stores list configuration.
type ListConfig struct {
	SnapshotID    SnapshotID
	Item          NodeAddress
	NodeAllocator ListNodeAllocator
	Allocator     *Allocator
	Deallocator   Deallocator
}

// NewList creates new list.
func NewList(config ListConfig) *List {
	return &List{
		config: config,
	}
}

// List represents the list of node addresses.
type List struct {
	config ListConfig
}

// Add adds address to the list.
func (l *List) Add(nodeAddress NodeAddress) {
	if l.config.Item == 0 {
		newNodeAddress, newNode := l.config.NodeAllocator.Allocate()
		newNode.Items[0] = nodeAddress
		newNode.Header.SnapshotID = l.config.SnapshotID
		newNode.Header.NumOfItems = 1
		l.config.Item = newNodeAddress

		return
	}
	listNodeData, listNode := l.config.NodeAllocator.Get(l.config.Item)
	if listNode.Header.NumOfItems+listNode.Header.NumOfSideLists < uint64(len(listNode.Items)) {
		if listNode.Header.SnapshotID < l.config.SnapshotID {
			newNodeAddress, newNode := l.config.NodeAllocator.Copy(listNodeData)
			newNode.Header.SnapshotID = l.config.SnapshotID
			oldNodeAddress := l.config.Item
			l.config.Item = newNodeAddress
			l.config.Allocator.Deallocate(oldNodeAddress)
			listNode = newNode
		}

		listNode.Items[listNode.Header.NumOfItems] = nodeAddress
		listNode.Header.NumOfItems++

		return
	}

	newNodeAddress, newNode := l.config.NodeAllocator.Allocate()
	newNode.Items[0] = nodeAddress
	newNode.Items[len(newNode.Items)-1] = l.config.Item
	newNode.Header.SnapshotID = l.config.SnapshotID
	newNode.Header.NumOfItems = 1
	newNode.Header.NumOfSideLists = 1
	l.config.Item = newNodeAddress
}

// Attach attaches another list.
func (l *List) Attach(nodeAddress NodeAddress) {
	if l.config.Item == 0 {
		l.config.Item = nodeAddress
		return
	}
	listNodeData, listNode := l.config.NodeAllocator.Get(l.config.Item)
	if listNode.Header.NumOfItems+listNode.Header.NumOfSideLists < uint64(len(listNode.Items)) {
		if listNode.Header.SnapshotID < l.config.SnapshotID {
			newNodeAddress, newNode := l.config.NodeAllocator.Copy(listNodeData)
			newNode.Header.SnapshotID = l.config.SnapshotID
			oldNodeAddress := l.config.Item
			l.config.Item = newNodeAddress
			l.config.Allocator.Deallocate(oldNodeAddress)
			listNode = newNode
		}

		listNode.Items[uint64(len(listNode.Items))-listNode.Header.NumOfSideLists-1] = nodeAddress
		listNode.Header.NumOfSideLists++

		return
	}

	newNodeAddress, newNode := l.config.NodeAllocator.Allocate()
	newNode.Items[uint64(len(listNode.Items))-1] = l.config.Item
	newNode.Items[uint64(len(listNode.Items))-2] = nodeAddress
	newNode.Header.SnapshotID = l.config.SnapshotID
	newNode.Header.NumOfSideLists = 2
	l.config.Item = newNodeAddress
}

// Deallocate deallocates nodes referenced by the list.
func (l *List) Deallocate(allocator *Allocator) {
	if l.config.Item == 0 {
		return
	}

	// FIXME (wojciech): Optimize heap allocations.
	stack := []NodeAddress{l.config.Item}
	for {
		if len(stack) == 0 {
			return
		}

		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		_, listNode := l.config.NodeAllocator.Get(n)
		for i := range listNode.Header.NumOfItems {
			allocator.Deallocate(listNode.Items[i])
		}

		stack = append(stack, listNode.Items[uint64(len(listNode.Items))-listNode.Header.NumOfSideLists:]...)
		allocator.Deallocate(n)
	}
}
