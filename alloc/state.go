package alloc

import (
	"context"
	"syscall"
	"unsafe"

	"github.com/pkg/errors"

	"github.com/outofforest/quantum/types"
)

// NewState creates new DB state.
func NewState(
	size uint64,
	nodeSize uint64,
	nodesPerGroup uint64,
	useHugePages bool,
	numOfEraseWorkers uint64,
) (*State, func(), error) {
	numOfGroups := size / nodeSize / nodesPerGroup
	numOfNodes := numOfGroups * nodesPerGroup
	size = numOfNodes * nodeSize
	opts := syscall.MAP_SHARED | syscall.MAP_ANONYMOUS | syscall.MAP_NORESERVE | syscall.MAP_POPULATE
	if useHugePages {
		opts |= syscall.MAP_HUGETLB
	}
	data, err := syscall.Mmap(-1, 0, int(size), syscall.PROT_READ|syscall.PROT_WRITE, opts)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "memory allocation failed")
	}

	return &State{
			size:              size,
			nodeSize:          nodeSize,
			nodesPerGroup:     nodesPerGroup,
			numOfEraseWorkers: numOfEraseWorkers,
			data:              data,
			dataP:             unsafe.Pointer(&data[0]),
			allocationCh:      NewAllocationCh[types.LogicalAddress](size, nodeSize, nodesPerGroup),
			deallocationCh:    make(chan []types.LogicalAddress, 100),
		}, func() {
			_ = syscall.Munmap(data)
		}, nil
}

// State stores the DB state.
type State struct {
	size              uint64
	nodeSize          uint64
	nodesPerGroup     uint64
	numOfEraseWorkers uint64
	data              []byte
	dataP             unsafe.Pointer
	allocationCh      chan []types.LogicalAddress
	deallocationCh    chan []types.LogicalAddress
}

// NodeSize returns size of node.
func (s *State) NodeSize() uint64 {
	return s.nodeSize
}

// NewPhysicalAllocationCh creates channel containing physical nodes to allocate.
// FIXME (wojciech): This design is horrible.
func (s *State) NewPhysicalAllocationCh() chan []types.PhysicalAddress {
	return NewAllocationCh[types.PhysicalAddress](s.size, s.nodeSize, s.nodesPerGroup)
}

// NewPool creates new allocation pool.
func (s *State) NewPool() *Pool[types.LogicalAddress] {
	return NewPool[types.LogicalAddress](s.allocationCh, s.deallocationCh)
}

// Node returns node bytes.
func (s *State) Node(nodeAddress types.LogicalAddress) unsafe.Pointer {
	return unsafe.Add(s.dataP, nodeAddress)
}

// Run runs node eraser.
func (s *State) Run(ctx context.Context) error {
	return RunEraser(ctx, s.deallocationCh, s.allocationCh, s.nodeSize, s, s.numOfEraseWorkers)
}

// Close closes the node eraser channel.
// FIXME (wojciech): This design is horrible.
func (s *State) Close() {
	close(s.deallocationCh)
}
