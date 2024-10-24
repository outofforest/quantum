package list

import (
	"sort"
	"sync/atomic"

	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/types"
)

// Config stores list configuration.
type Config struct {
	ListRoot       *types.Pointer
	State          *alloc.State
	NodeAssistant  *NodeAssistant
	StoreRequestCh chan<- types.StoreRequest
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
	pointer types.Pointer,
	snapshotID types.SnapshotID,
	volatilePool *alloc.Pool[types.VolatileAddress],
	persistentPool *alloc.Pool[types.PersistentAddress],
	node *Node,
) error {
	if l.config.ListRoot.LogicalAddress == 0 {
		newNodeAddress, err := volatilePool.Allocate()
		if err != nil {
			return err
		}
		l.config.NodeAssistant.Project(newNodeAddress, node)

		node.Header.SnapshotID = snapshotID
		revision := atomic.AddUint64(&node.Header.RevisionHeader.Revision, 1)
		node.Pointers[0] = pointer
		node.Header.NumOfPointers = 1

		physicalAddress, err := persistentPool.Allocate()
		if err != nil {
			return err
		}

		l.config.ListRoot.LogicalAddress = newNodeAddress
		l.config.ListRoot.PhysicalAddress = physicalAddress

		l.config.StoreRequestCh <- types.StoreRequest{
			Revision: revision,
			Pointer:  l.config.ListRoot,
		}

		return nil
	}

	l.config.NodeAssistant.Project(l.config.ListRoot.LogicalAddress, node)
	if node.Header.NumOfPointers+node.Header.NumOfSideLists < uint64(len(node.Pointers)) {
		if node.Header.SnapshotID != snapshotID {
			node.Header.SnapshotID = snapshotID

			physicalAddress, err := persistentPool.Allocate()
			if err != nil {
				return err
			}

			persistentPool.Deallocate(l.config.ListRoot.PhysicalAddress)
			l.config.ListRoot.PhysicalAddress = physicalAddress
		}
		revision := atomic.AddUint64(&node.Header.RevisionHeader.Revision, 1)

		node.Pointers[node.Header.NumOfPointers] = pointer
		node.Header.NumOfPointers++

		l.config.StoreRequestCh <- types.StoreRequest{
			Revision: revision,
			Pointer:  l.config.ListRoot,
		}

		return nil
	}

	newNodeAddress, err := volatilePool.Allocate()
	if err != nil {
		return err
	}
	l.config.NodeAssistant.Project(newNodeAddress, node)

	node.Header.SnapshotID = snapshotID
	revision := atomic.AddUint64(&node.Header.RevisionHeader.Revision, 1)
	node.Pointers[0] = pointer
	node.Pointers[len(node.Pointers)-1] = *l.config.ListRoot
	node.Header.NumOfPointers = 1
	node.Header.NumOfSideLists = 1

	physicalAddress, err := persistentPool.Allocate()
	if err != nil {
		return err
	}

	l.config.ListRoot.LogicalAddress = newNodeAddress
	l.config.ListRoot.PhysicalAddress = physicalAddress

	l.config.StoreRequestCh <- types.StoreRequest{
		Revision: revision,
		Pointer:  l.config.ListRoot,
	}

	return nil
}

// Attach attaches another list.
func (l *List) Attach(
	pointer types.Pointer,
	snapshotID types.SnapshotID,
	volatilePool *alloc.Pool[types.VolatileAddress],
	persistentPool *alloc.Pool[types.PersistentAddress],
	node *Node,
) error {
	if l.config.ListRoot.LogicalAddress == 0 {
		*l.config.ListRoot = pointer
		return nil
	}

	l.config.NodeAssistant.Project(l.config.ListRoot.LogicalAddress, node)
	if node.Header.NumOfPointers+node.Header.NumOfSideLists < uint64(len(node.Pointers)) {
		if node.Header.SnapshotID != snapshotID {
			node.Header.SnapshotID = snapshotID

			physicalAddress, err := persistentPool.Allocate()
			if err != nil {
				return err
			}

			persistentPool.Deallocate(l.config.ListRoot.PhysicalAddress)
			l.config.ListRoot.PhysicalAddress = physicalAddress
		}
		revision := atomic.AddUint64(&node.Header.RevisionHeader.Revision, 1)

		node.Pointers[uint64(len(node.Pointers))-node.Header.NumOfSideLists-1] = pointer
		node.Header.NumOfSideLists++

		l.config.StoreRequestCh <- types.StoreRequest{
			Revision: revision,
			Pointer:  l.config.ListRoot,
		}

		return nil
	}

	newNodeAddress, err := volatilePool.Allocate()
	if err != nil {
		return err
	}
	l.config.NodeAssistant.Project(newNodeAddress, node)

	node.Header.SnapshotID = snapshotID
	revision := atomic.AddUint64(&node.Header.RevisionHeader.Revision, 1)
	node.Pointers[uint64(len(node.Pointers))-1] = *l.config.ListRoot
	node.Pointers[uint64(len(node.Pointers))-2] = pointer
	node.Header.NumOfSideLists = 2

	physicalAddress, err := persistentPool.Allocate()
	if err != nil {
		return err
	}

	l.config.ListRoot.LogicalAddress = newNodeAddress
	l.config.ListRoot.PhysicalAddress = physicalAddress

	l.config.StoreRequestCh <- types.StoreRequest{
		Revision: revision,
		Pointer:  l.config.ListRoot,
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

			l.config.NodeAssistant.Project(pointer.LogicalAddress, node)
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
	if l.config.ListRoot.LogicalAddress == 0 {
		return nil
	}

	nodes := []types.VolatileAddress{}
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

		l.config.NodeAssistant.Project(pointer.LogicalAddress, node)
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
	if listRoot.LogicalAddress == 0 {
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

		nodeAssistant.Project(pointer.LogicalAddress, node)
		for i := range node.Header.NumOfPointers {
			// We don't deallocate from volatile pool here, because those nodes are still used by next revisions.
			persistentPool.Deallocate(node.Pointers[i].PhysicalAddress)
		}

		for _, p := range node.Pointers[uint64(len(node.Pointers))-node.Header.NumOfSideLists:] {
			if stackLen < maxStackSize {
				stack[stackLen] = p
				stackLen++

				continue
			}

			Deallocate(
				listRoot,
				volatilePool,
				persistentPool,
				nodeAssistant,
				node,
			)
		}

		volatilePool.Deallocate(pointer.LogicalAddress)
		persistentPool.Deallocate(pointer.PhysicalAddress)
	}
}
