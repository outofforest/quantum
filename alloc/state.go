package alloc

import (
	"context"
	"unsafe"

	"github.com/pkg/errors"

	"github.com/outofforest/parallel"
	"github.com/outofforest/photon"
	"github.com/outofforest/quantum/types"
)

// NewState creates new DB state.
func NewState(
	volatileSize, persistentSize uint64,
	nodesPerGroup uint64,
	useHugePages bool,
) (*State, func(), error) {
	// Align allocated memory address to the node volatileSize. It might be required if using O_DIRECT option to open files.
	// As a side effect it is also 64-byte aligned which is required by the AVX512 instructions.
	dataP, deallocateFunc, err := Allocate(volatileSize, types.NodeLength, useHugePages)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "memory allocation failed")
	}

	volatileAllocationCh, singularityVolatileAddress := NewAllocationCh[types.VolatileAddress](volatileSize,
		nodesPerGroup, false)
	persistentAllocationCh, singularityPersistentAddress := NewAllocationCh[types.PersistentAddress](
		persistentSize, nodesPerGroup, false)

	singularityNode := (*types.SingularityNode)(unsafe.Add(dataP, types.NodeLength*singularityVolatileAddress))
	return &State{
		nodesPerGroup: nodesPerGroup,
		singularityNodeRoot: types.NodeRoot{
			Hash: &singularityNode.Hash,
			Pointer: &types.Pointer{
				Revision:          1,
				VolatileAddress:   singularityVolatileAddress,
				PersistentAddress: singularityPersistentAddress,
			},
		},
		dataP:                      dataP,
		volatileAllocationCh:       volatileAllocationCh,
		volatileDeallocationCh:     make(chan []types.VolatileAddress, 10),
		volatileAllocationPoolCh:   make(chan []types.VolatileAddress, 1),
		persistentAllocationCh:     persistentAllocationCh,
		persistentDeallocationCh:   make(chan []types.PersistentAddress, 10),
		persistentAllocationPoolCh: make(chan []types.PersistentAddress, 1),
		closedCh:                   make(chan struct{}),
	}, deallocateFunc, nil
}

// State stores the DB state.
type State struct {
	nodesPerGroup              uint64
	singularityNodeRoot        types.NodeRoot
	dataP                      unsafe.Pointer
	volatileAllocationCh       chan []types.VolatileAddress
	volatileDeallocationCh     chan []types.VolatileAddress
	volatileAllocationPoolCh   chan []types.VolatileAddress
	persistentAllocationCh     chan []types.PersistentAddress
	persistentDeallocationCh   chan []types.PersistentAddress
	persistentAllocationPoolCh chan []types.PersistentAddress
	closedCh                   chan struct{}
}

// NewVolatileAllocator creates new volatile address allocator.
func (s *State) NewVolatileAllocator() *Allocator[types.VolatileAddress] {
	return newAllocator(s, s.volatileAllocationPoolCh)
}

// NewVolatileDeallocator creates new volatile address deallocator.
func (s *State) NewVolatileDeallocator() *Deallocator[types.VolatileAddress] {
	return newDeallocator(s.nodesPerGroup, s.volatileDeallocationCh)
}

// NewPersistentAllocator creates new persistent address allocator.
func (s *State) NewPersistentAllocator() *Allocator[types.PersistentAddress] {
	return newAllocator(s, s.persistentAllocationPoolCh)
}

// NewPersistentDeallocator creates new persistent address deallocator.
func (s *State) NewPersistentDeallocator() *Deallocator[types.PersistentAddress] {
	return newDeallocator(s.nodesPerGroup, s.persistentDeallocationCh)
}

// SingularityNodeRoot returns node root of singularity node.
func (s *State) SingularityNodeRoot() types.NodeRoot {
	return s.singularityNodeRoot
}

// Origin returns the pointer to the allocated memory.
func (s *State) Origin() unsafe.Pointer {
	return s.dataP
}

// Node returns node bytes.
func (s *State) Node(nodeAddress types.VolatileAddress) unsafe.Pointer {
	return unsafe.Add(s.dataP, nodeAddress*types.NodeLength)
}

// Bytes returns byte slice of a node.
func (s *State) Bytes(nodeAddress types.VolatileAddress) []byte {
	return photon.SliceFromPointer[byte](s.Node(nodeAddress), types.NodeLength)
}

// Clear sets all the bytes of the node to zero.
func (s *State) Clear(nodeAddress types.VolatileAddress) {
	clear(s.Bytes(nodeAddress))
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
					close(s.persistentDeallocationCh)
					for range s.volatileAllocationPoolCh {
					}
					for range s.persistentAllocationPoolCh {
					}

					return errors.WithStack(ctx.Err())
				}
			}
		})
		spawn("volatilePump", parallel.Continue, func(ctx context.Context) error {
			defer close(s.volatileAllocationPoolCh)

			return runPump(
				ctx,
				s.volatileAllocationCh,
				s.volatileDeallocationCh,
				s.volatileAllocationPoolCh,
			)
		})
		spawn("persistentPump", parallel.Continue, func(ctx context.Context) error {
			defer close(s.persistentAllocationPoolCh)

			return runPump(
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
	s.volatileDeallocationCh <- nil
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

func runPump[A Address](
	ctx context.Context,
	allocationCh chan []A,
	deallocationCh <-chan []A,
	allocationPoolCh chan<- []A,
) error {
	// FIXME (wojciech): Zeroing deallocated volatile nodes could be done in this goroutine.

	// Any address from the pool uniquely identifies entire pool, so I take the first one (allocated as the last one).
	// FIXME (wojciech): This is not needed for volatile addresses.
	var invalidAddress A
	// Trick to save on `if` later in the handler.
	previousDeallocatedPool := []A{0}

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
