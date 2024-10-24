package alloc

import (
	"context"
	"fmt"
	"unsafe"

	"github.com/pkg/errors"
	"golang.org/x/sys/unix"

	"github.com/outofforest/parallel"
	"github.com/outofforest/photon"
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
	opts := unix.MAP_SHARED | unix.MAP_ANONYMOUS | unix.MAP_NORESERVE | unix.MAP_POPULATE
	if useHugePages {
		opts |= unix.MAP_HUGETLB
	}
	data, err := unix.Mmap(-1, 0, int(size), unix.PROT_READ|unix.PROT_WRITE, opts)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "memory allocation failed")
	}

	return &State{
			size:                         size,
			nodeSize:                     nodeSize,
			nodesPerGroup:                nodesPerGroup,
			numOfEraseWorkers:            numOfEraseWorkers,
			data:                         data,
			dataP:                        unsafe.Pointer(&data[0]),
			volatileAllocationCh:         NewAllocationCh[types.VolatileAddress](size, nodeSize, nodesPerGroup),
			volatileDeallocationCh:       make(chan []types.VolatileAddress, 10),
			persistentAllocationCh:       NewAllocationCh[types.PersistentAddress](size, nodeSize, nodesPerGroup),
			persistentAllocationPoolCh:   make(chan []types.PersistentAddress, 1),
			persistentDeallocationPoolCh: make(chan []types.PersistentAddress, 1),
		}, func() {
			_ = unix.Munmap(data)
		}, nil
}

// State stores the DB state.
type State struct {
	size                         uint64
	nodeSize                     uint64
	nodesPerGroup                uint64
	numOfEraseWorkers            uint64
	data                         []byte
	dataP                        unsafe.Pointer
	volatileAllocationCh         chan []types.VolatileAddress
	volatileDeallocationCh       chan []types.VolatileAddress
	persistentAllocationCh       chan []types.PersistentAddress
	persistentAllocationPoolCh   chan []types.PersistentAddress
	persistentDeallocationPoolCh chan []types.PersistentAddress
}

// NodeSize returns size of node.
func (s *State) NodeSize() uint64 {
	return s.nodeSize
}

// NewVolatilePool creates new volatile allocation pool.
func (s *State) NewVolatilePool() *Pool[types.VolatileAddress] {
	return NewPool[types.VolatileAddress](s.volatileAllocationCh, s.volatileDeallocationCh)
}

// NewPersistentPool creates new persistent allocation pool.
func (s *State) NewPersistentPool() *Pool[types.PersistentAddress] {
	return NewPool[types.PersistentAddress](s.persistentAllocationPoolCh, s.persistentDeallocationPoolCh)
}

// Node returns node bytes.
func (s *State) Node(nodeAddress types.VolatileAddress) unsafe.Pointer {
	return unsafe.Add(s.dataP, nodeAddress)
}

// Bytes returns byte slice of a node.
func (s *State) Bytes(nodeAddress types.VolatileAddress) []byte {
	return photon.SliceFromPointer[byte](s.Node(nodeAddress), int(s.nodeSize))
}

// Run runs node eraser.
func (s *State) Run(ctx context.Context) error {
	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("volatileNodeEraser", parallel.Continue, func(ctx context.Context) error {
			defer close(s.volatileAllocationCh)
			return s.runVolatileBlockEraser(ctx, s.volatileDeallocationCh, s.volatileAllocationCh)
		})
		spawn("persistentPump", parallel.Continue, func(ctx context.Context) error {
			defer close(s.persistentAllocationPoolCh)
			return s.runPersistentPump(
				ctx,
				s.persistentAllocationCh,
				s.persistentAllocationPoolCh,
				s.persistentDeallocationPoolCh,
			)
		})

		return nil
	})
}

// Commit is called when new snapshot starts to mark point where invalid physical address is present in the queue.
func (s *State) Commit() {
	s.persistentDeallocationPoolCh <- nil
}

// Close closes the node eraser channel.
func (s *State) Close() {
	close(s.volatileDeallocationCh)
	close(s.persistentDeallocationPoolCh)
}

func (s *State) runVolatileBlockEraser(
	ctx context.Context,
	deallocationCh <-chan []types.VolatileAddress,
	allocationCh chan<- []types.VolatileAddress,
) error {
	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		for i := range s.numOfEraseWorkers {
			spawn(fmt.Sprintf("worker-%02d", i), parallel.Fail, func(ctx context.Context) error {
				for nodes := range deallocationCh {
					for _, n := range nodes {
						clear(photon.SliceFromPointer[byte](s.Node(n), int(s.nodeSize)))
					}
					allocationCh <- nodes
				}

				return errors.WithStack(ctx.Err())
			})
		}
		return nil
	})
}

func (s *State) runPersistentPump(
	ctx context.Context,
	allocationCh chan []types.PersistentAddress,
	allocationPoolCh chan<- []types.PersistentAddress,
	deallocationPoolCh <-chan []types.PersistentAddress,
) error {
	// Any address from the pool uniquely identifies entire pool, so I take the first one (allocated as the last one).
	var invalidAddress types.PersistentAddress
	// Trick to save on `if` later in the handler.
	previousDeallocatedPool := []types.PersistentAddress{0}

loop:
	for {
		select {
		case allocatedPool := <-allocationCh:
			if allocatedPool[0] == invalidAddress {
				return errors.New("out of space")
			}
			for {
				select {
				case allocationPoolCh <- allocatedPool:
					continue loop
				case deallocatedPool, ok := <-deallocationPoolCh:
					if !ok {
						return errors.WithStack(ctx.Err())
					}

					// This condition means commit has been finalized.
					if deallocatedPool == nil {
						invalidAddress = previousDeallocatedPool[0]
						continue
					}

					previousDeallocatedPool = deallocatedPool
					allocationCh <- deallocatedPool
				}
			}
		default:
			// If we are here it means there was no available pool in `allocationCh`.
			return errors.New("out of space")
		}
	}
}
