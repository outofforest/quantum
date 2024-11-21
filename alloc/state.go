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
	nodesPerGroup uint64,
	useHugePages bool,
	numOfEraseWorkers uint64,
) (*State, func(), error) {
	opts := unix.MAP_SHARED | unix.MAP_ANONYMOUS | unix.MAP_POPULATE
	if useHugePages {
		// When using huge pages, the size must be a multiple of the hugepage size. Otherwise, munmap fails.
		opts |= unix.MAP_HUGETLB
	}
	originalSize := uintptr(size)
	dataP, err := unix.MmapPtr(-1, 0, nil, originalSize, unix.PROT_READ|unix.PROT_WRITE, opts)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "memory allocation failed")
	}

	dataPOrig := dataP

	// Align allocated memory address to the node size. It might be required if using O_DIRECT option to open files.
	diff := uint64((uintptr(dataP)+types.NodeLength-1)/types.NodeLength*types.NodeLength - uintptr(dataP))
	dataP = unsafe.Add(dataP, diff)
	size -= diff

	volatileAllocationCh, volatileSingularityNode := NewAllocationCh[types.VolatileAddress](size, nodesPerGroup)
	persistentAllocationCh, persistentSingularityNode := NewAllocationCh[types.PersistentAddress](size, nodesPerGroup)

	singularityNode := (*types.SingularityNode)(unsafe.Add(dataP, volatileSingularityNode))
	return &State{
			size:          size,
			nodesPerGroup: nodesPerGroup,
			singularityNodeRoot: types.NodeRoot{
				Hash: &singularityNode.Hash,
				Pointer: &types.Pointer{
					Revision:          1,
					VolatileAddress:   volatileSingularityNode,
					PersistentAddress: persistentSingularityNode,
				},
			},
			numOfEraseWorkers:          numOfEraseWorkers,
			dataP:                      dataP,
			volatileAllocationCh:       volatileAllocationCh,
			volatileDeallocationCh:     make(chan []types.VolatileAddress, 10),
			volatileAllocationPoolCh:   make(chan []types.VolatileAddress, 1),
			persistentAllocationCh:     persistentAllocationCh,
			persistentDeallocationCh:   make(chan []types.PersistentAddress, 10),
			persistentAllocationPoolCh: make(chan []types.PersistentAddress, 1),
			closedCh:                   make(chan struct{}),
		}, func() {
			_ = unix.MunmapPtr(dataPOrig, originalSize)
		}, nil
}

// State stores the DB state.
type State struct {
	size                       uint64
	nodesPerGroup              uint64
	singularityNodeRoot        types.NodeRoot
	numOfEraseWorkers          uint64
	dataP                      unsafe.Pointer
	volatileAllocationCh       chan []types.VolatileAddress
	volatileDeallocationCh     chan []types.VolatileAddress
	volatileAllocationPoolCh   chan []types.VolatileAddress
	persistentAllocationCh     chan []types.PersistentAddress
	persistentDeallocationCh   chan []types.PersistentAddress
	persistentAllocationPoolCh chan []types.PersistentAddress
	closedCh                   chan struct{}
}

// NewVolatilePool creates new volatile allocation pool.
func (s *State) NewVolatilePool() *Pool[types.VolatileAddress] {
	return NewPool[types.VolatileAddress](s.volatileAllocationPoolCh, s.volatileDeallocationCh)
}

// NewPersistentPool creates new persistent allocation pool.
func (s *State) NewPersistentPool() *Pool[types.PersistentAddress] {
	return NewPool[types.PersistentAddress](s.persistentAllocationPoolCh, s.persistentDeallocationCh)
}

// SingularityNodeRoot returns node root of singularity node.
func (s *State) SingularityNodeRoot() types.NodeRoot {
	return s.singularityNodeRoot
}

// Node returns node bytes.
func (s *State) Node(nodeAddress types.VolatileAddress) unsafe.Pointer {
	return unsafe.Add(s.dataP, nodeAddress)
}

// Bytes returns byte slice of a node.
func (s *State) Bytes(nodeAddress types.VolatileAddress) []byte {
	return photon.SliceFromPointer[byte](s.Node(nodeAddress), types.NodeLength)
}

// Run runs node eraser.
func (s *State) Run(ctx context.Context) error {
	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("supervisor", parallel.Exit, func(ctx context.Context) error {
			ctxDone := ctx.Done()
			var volatileDeallocationCh <-chan []types.VolatileAddress
			var persistentDeallocationCh <-chan []types.PersistentAddress
			for {
				select {
				case <-ctxDone:
					ctxDone = nil
					volatileDeallocationCh = s.volatileDeallocationCh
					persistentDeallocationCh = s.persistentDeallocationCh
				case <-volatileDeallocationCh:
				case <-persistentDeallocationCh:
				case <-s.closedCh:
					close(s.volatileDeallocationCh)
					for range s.volatileAllocationPoolCh {
					}

					close(s.persistentDeallocationCh)
					for range s.persistentAllocationPoolCh {
					}

					return errors.WithStack(ctx.Err())
				}
			}
		})
		spawn("volatilePump", parallel.Continue, func(ctx context.Context) error {
			defer close(s.volatileAllocationPoolCh)
			return s.runVolatilePump(
				ctx,
				s.volatileAllocationCh,
				s.volatileDeallocationCh,
				s.volatileAllocationPoolCh,
			)
		})
		spawn("persistentPump", parallel.Continue, func(ctx context.Context) error {
			defer close(s.persistentAllocationPoolCh)
			return s.runPersistentPump(
				ctx,
				s.persistentAllocationCh,
				s.persistentDeallocationCh,
				s.persistentAllocationPoolCh,
			)
		})

		return nil
	})
}

// Commit is called when new snapshot starts to mark point where invalid physical address is present in the queue.
func (s *State) Commit() {
	s.persistentDeallocationCh <- nil
}

// Close tells that there will be no more operations done.
func (s *State) Close() {
	select {
	case <-s.closedCh:
	default:
		close(s.closedCh)
	}
}

func (s *State) runVolatilePump(
	ctx context.Context,
	allocationCh chan []types.VolatileAddress,
	deallocationCh <-chan []types.VolatileAddress,
	allocationPoolCh chan<- []types.VolatileAddress,
) error {
	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("allocator", parallel.Fail, func(ctx context.Context) error {
			for {
				select {
				case <-ctx.Done():
					return errors.WithStack(ctx.Err())
				case allocatedPool := <-allocationCh:
					allocationPoolCh <- allocatedPool
				default:
					// If we are here it means there was no available pool in `allocationCh`.
					return errors.New("out of space")
				}
			}
		})

		for i := range s.numOfEraseWorkers {
			spawn(fmt.Sprintf("eraser-%02d", i), parallel.Fail, func(ctx context.Context) error {
				for nodes := range deallocationCh {
					for _, n := range nodes {
						clear(photon.SliceFromPointer[byte](s.Node(n), types.NodeLength))
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
	deallocationCh <-chan []types.PersistentAddress,
	allocationPoolCh chan<- []types.PersistentAddress,
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
				case deallocatedPool, ok := <-deallocationCh:
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
