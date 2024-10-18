package alloc

import (
	"syscall"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/samber/lo"

	"github.com/outofforest/photon"
	"github.com/outofforest/quantum/list"
	"github.com/outofforest/quantum/types"
)

const nodesPerGroup = 1024

// Config stores configuration of allocator.
type Config struct {
	TotalSize    uint64
	NodeSize     uint64
	UseHugePages bool
}

// NewAllocator creates memory allocator.
func NewAllocator(config Config) (*Allocator, func(), error) {
	numOfGroups := config.TotalSize / config.NodeSize / nodesPerGroup
	numOfNodes := numOfGroups * nodesPerGroup
	opts := syscall.MAP_SHARED | syscall.MAP_ANONYMOUS | syscall.MAP_NORESERVE | syscall.MAP_POPULATE
	if config.UseHugePages {
		opts |= syscall.MAP_HUGETLB
	}
	data, err := syscall.Mmap(-1, 0, int(config.NodeSize*(numOfNodes+1)),
		syscall.PROT_READ|syscall.PROT_WRITE, opts)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "memory allocation failed")
	}

	availableNodes := make([]types.NodeAddress, 0, numOfNodes)
	for i := config.NodeSize; i < uint64(len(data)); i += config.NodeSize {
		availableNodes = append(availableNodes, types.NodeAddress(i))
	}

	availableNodesCh := make(chan []types.NodeAddress, numOfGroups)
	for i := uint64(0); i < uint64(len(availableNodes)); i += nodesPerGroup {
		availableNodesCh <- availableNodes[i : i+nodesPerGroup]
	}

	return &Allocator{
			config:                 config,
			data:                   data,
			dataP:                  unsafe.Pointer(&data[0]),
			availableNodesCh:       availableNodesCh,
			nodesToAllocate:        <-availableNodesCh,
			nodesToDeallocate:      make([]types.NodeAddress, 0, nodesPerGroup),
			nodesToDeallocateStack: make([][]types.NodeAddress, 0, 1),
		}, func() {
			_ = syscall.Munmap(data)
		}, nil
}

// Allocator is the allocator implementation used in tests.
type Allocator struct {
	config             Config
	data               []byte
	dataP              unsafe.Pointer
	availableNodesCh   chan []types.NodeAddress
	deallocatedNodesCh chan []types.NodeAddress

	nodesToAllocate        []types.NodeAddress
	nodesToDeallocate      []types.NodeAddress
	nodesToDeallocateStack [][]types.NodeAddress
}

// Node returns node bytes.
func (a *Allocator) Node(nodeAddress types.NodeAddress) unsafe.Pointer {
	return unsafe.Add(a.dataP, nodeAddress)
}

// Allocate allocates node.
func (a *Allocator) Allocate() (types.NodeAddress, unsafe.Pointer, error) {
	nodeAddress := a.nodesToAllocate[len(a.nodesToAllocate)-1]
	a.nodesToAllocate = a.nodesToAllocate[:len(a.nodesToAllocate)-1]

	if len(a.nodesToAllocate) == 0 {
		if len(a.availableNodesCh) == 0 {
			return 0, nil, errors.New("out of space")
		}
		a.nodesToDeallocateStack = append(a.nodesToDeallocateStack, a.nodesToAllocate)
		a.nodesToAllocate = <-a.availableNodesCh
	}

	return nodeAddress, a.Node(nodeAddress), nil
}

// Copy allocates new node and moves existing one there.
func (a *Allocator) Copy(nodeAddress types.NodeAddress) (types.NodeAddress, unsafe.Pointer, error) {
	// FIXME (wojciech): No-copy test
	// newNodeAddress, newNodeData, err := a.Allocate()
	// if err != nil {
	// 	return 0, nil, err
	// }

	// a.nodes[nodeAddress], a.nodes[newNodeAddress] = newNodeData, a.nodes[nodeAddress]

	// return newNodeAddress, a.nodes[newNodeAddress], nil

	return nodeAddress, a.Node(nodeAddress), nil
}

// Deallocate deallocates node.
func (a *Allocator) Deallocate(nodeAddress types.NodeAddress) {
	if a.deallocatedNodesCh == nil {
		a.deallocatedNodesCh = make(chan []types.NodeAddress, 100)
		for range 5 {
			go func() {
				for nodes := range a.deallocatedNodesCh {
					for _, n := range nodes {
						clear(photon.SliceFromPointer[byte](a.Node(n), int(a.config.NodeSize)))
					}
					a.availableNodesCh <- nodes
				}
			}()
		}
	}

	a.nodesToDeallocate = append(a.nodesToDeallocate, nodeAddress)
	if len(a.nodesToDeallocate) == nodesPerGroup {
		a.deallocatedNodesCh <- a.nodesToDeallocate
		a.nodesToDeallocate = a.nodesToDeallocateStack[len(a.nodesToDeallocateStack)-1]
		a.nodesToDeallocateStack = a.nodesToDeallocateStack[:len(a.nodesToDeallocateStack)-1]
	}
}

// NodeSize returns size of node.
func (a *Allocator) NodeSize() uint64 {
	return a.config.NodeSize
}

// ListToCommit contains cached deallocation list.
type ListToCommit struct {
	List *list.List
	Item *types.NodeAddress
}

// NewSnapshotAllocator returns snapshot-level allocator.
func NewSnapshotAllocator(
	allocator types.Allocator,
	deallocationListCache map[types.SnapshotID]ListToCommit,
	availableSnapshots map[types.SnapshotID]struct{},
	listNodeAllocator list.NodeAllocator,
) *SnapshotAllocator {
	sa := &SnapshotAllocator{
		allocator:             allocator,
		deallocationListCache: deallocationListCache,
		availableSnapshots:    availableSnapshots,
		listNodeAllocator:     listNodeAllocator,
	}
	sa.immediateAllocator = NewImmediateSnapshotAllocator(sa)
	return sa
}

// SnapshotAllocator allocates memory on behalf of snapshot.
type SnapshotAllocator struct {
	snapshotID            types.SnapshotID
	allocator             types.Allocator
	immediateAllocator    types.SnapshotAllocator
	deallocationListCache map[types.SnapshotID]ListToCommit
	availableSnapshots    map[types.SnapshotID]struct{}
	listNodeAllocator     list.NodeAllocator
}

// SnapshotID returns snapshot ID.
func (sa *SnapshotAllocator) SnapshotID() types.SnapshotID {
	return sa.snapshotID
}

// SetSnapshotID sets snapshot ID.
func (sa *SnapshotAllocator) SetSnapshotID(snapshotID types.SnapshotID) {
	sa.snapshotID = snapshotID
}

// Allocate allocates new node.
func (sa *SnapshotAllocator) Allocate() (types.NodeAddress, unsafe.Pointer, error) {
	nodeAddress, node, err := sa.allocator.Allocate()
	if err != nil {
		return 0, nil, err
	}

	return nodeAddress, node, nil
}

// Copy allocates new node and copies content from existing one.
func (sa *SnapshotAllocator) Copy(nodeAddress types.NodeAddress) (types.NodeAddress, unsafe.Pointer, error) {
	newNodeAddress, node, err := sa.allocator.Copy(nodeAddress)
	if err != nil {
		return 0, nil, err
	}

	return newNodeAddress, node, nil
}

// Deallocate marks node for deallocation.
func (sa *SnapshotAllocator) Deallocate(nodeAddress types.NodeAddress, srcSnapshotID types.SnapshotID) error {
	if srcSnapshotID == sa.snapshotID {
		sa.allocator.Deallocate(nodeAddress)
		return nil
	}

	if _, exists := sa.availableSnapshots[srcSnapshotID]; !exists {
		sa.allocator.Deallocate(nodeAddress)
		return nil
	}

	l, exists := sa.deallocationListCache[srcSnapshotID]
	if !exists {
		l = ListToCommit{
			Item: lo.ToPtr[types.NodeAddress](0),
			List: list.New(list.Config{
				Item:          l.Item,
				NodeAllocator: sa.listNodeAllocator,
				Allocator:     sa.immediateAllocator,
			}),
		}
		sa.deallocationListCache[srcSnapshotID] = l
	}

	return l.List.Add(nodeAddress)
}

// NewImmediateSnapshotAllocator creates new immediate snapshot deallocator.
func NewImmediateSnapshotAllocator(
	parentSnapshotAllocator types.SnapshotAllocator,
) *ImmediateSnapshotAllocator {
	return &ImmediateSnapshotAllocator{
		parentSnapshotAllocator: parentSnapshotAllocator,
	}
}

// ImmediateSnapshotAllocator deallocates nodes immediately instead of adding them to deallocation list.
type ImmediateSnapshotAllocator struct {
	parentSnapshotAllocator types.SnapshotAllocator
}

// SnapshotID returns snapshot ID.
func (sa *ImmediateSnapshotAllocator) SnapshotID() types.SnapshotID {
	return sa.parentSnapshotAllocator.SnapshotID()
}

// SetSnapshotID sets snapshot ID.
func (sa *ImmediateSnapshotAllocator) SetSnapshotID(snapshotID types.SnapshotID) {
	sa.parentSnapshotAllocator.SetSnapshotID(snapshotID)
}

// Allocate allocates new node.
func (sa *ImmediateSnapshotAllocator) Allocate() (types.NodeAddress, unsafe.Pointer, error) {
	return sa.parentSnapshotAllocator.Allocate()
}

// Copy allocates new node and copies content from existing one.
func (sa *ImmediateSnapshotAllocator) Copy(nodeAddress types.NodeAddress) (types.NodeAddress, unsafe.Pointer, error) {
	return sa.parentSnapshotAllocator.Copy(nodeAddress)
}

// Deallocate marks node for deallocation.
func (sa *ImmediateSnapshotAllocator) Deallocate(nodeAddress types.NodeAddress, _ types.SnapshotID) error {
	// using sa.snapshotID instead of the snapshotID argument causes immediate deallocation.
	return sa.parentSnapshotAllocator.Deallocate(nodeAddress, sa.parentSnapshotAllocator.SnapshotID())
}
