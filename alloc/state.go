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
	size uint64,
	nodesPerGroup uint64,
	useHugePages bool,
) (*State, func(), error) {
	// Align allocated memory address to the node size. It might be required if using O_DIRECT option to open files.
	// As a side effect it is also 64-byte aligned which is required by the AVX512 instructions.
	dataP, deallocateFunc, err := Allocate(size, types.NodeLength, useHugePages)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "memory allocation failed")
	}

	allocationCh, singularityNodeAddress := NewAllocationCh(size, nodesPerGroup)

	singularityNode := (*types.SingularityNode)(unsafe.Add(dataP, types.NodeLength*singularityNodeAddress))
	return &State{
		size:          size,
		nodesPerGroup: nodesPerGroup,
		singularityNodeRoot: types.NodeRoot{
			Hash: &singularityNode.Hash,
			Pointer: &types.Pointer{
				Revision:          1,
				VolatileAddress:   singularityNodeAddress,
				PersistentAddress: singularityNodeAddress,
			},
		},
		dataP:            dataP,
		allocationCh:     allocationCh,
		deallocationCh:   make(chan []types.NodeAddress, 10),
		allocationPoolCh: make(chan []types.NodeAddress, 1),
		closedCh:         make(chan struct{}),
	}, deallocateFunc, nil
}

// State stores the DB state.
type State struct {
	size                uint64
	nodesPerGroup       uint64
	singularityNodeRoot types.NodeRoot
	dataP               unsafe.Pointer
	allocationCh        chan []types.NodeAddress
	deallocationCh      chan []types.NodeAddress
	allocationPoolCh    chan []types.NodeAddress
	closedCh            chan struct{}
}

// NewAllocator creates new node allocator.
func (s *State) NewAllocator() *Allocator {
	return newAllocator(s, s.allocationPoolCh)
}

// NewDeallocator creates new node deallocator.
func (s *State) NewDeallocator() *Deallocator {
	return newDeallocator(s.nodesPerGroup, s.deallocationCh)
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
func (s *State) Node(nodeAddress types.NodeAddress) unsafe.Pointer {
	return unsafe.Add(s.dataP, nodeAddress*types.NodeLength)
}

// Bytes returns byte slice of a node.
func (s *State) Bytes(nodeAddress types.NodeAddress) []byte {
	return photon.SliceFromPointer[byte](s.Node(nodeAddress), types.NodeLength)
}

// Clear sets all the bytes of the node to zero.
func (s *State) Clear(nodeAddress types.NodeAddress) {
	clear(s.Bytes(nodeAddress))
}

// Copy copies node between addresses.
func (s *State) Copy(dstNodeAddress, srcNodeAddress types.NodeAddress) {
	copy(s.Bytes(dstNodeAddress), s.Bytes(srcNodeAddress))
}

// Run runs node eraser.
func (s *State) Run(ctx context.Context) error {
	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		spawn("supervisor", parallel.Exit, func(ctx context.Context) error {
			ctxDone := ctx.Done()
			var deallocationCh <-chan []types.NodeAddress
			for {
				select {
				case <-ctxDone:
					ctxDone = nil
					deallocationCh = s.deallocationCh
				case <-deallocationCh:
				case <-s.closedCh:
					close(s.deallocationCh)
					for range s.allocationPoolCh {
					}

					return errors.WithStack(ctx.Err())
				}
			}
		})
		spawn("pump", parallel.Continue, func(ctx context.Context) error {
			defer close(s.allocationPoolCh)
			return s.runPump(
				ctx,
				s.allocationCh,
				s.deallocationCh,
				s.allocationPoolCh,
			)
		})

		return nil
	})
}

// Commit is called when new snapshot starts to mark point where invalid physical address is present in the queue.
func (s *State) Commit() {
	s.deallocationCh <- nil
}

// Close tells that there will be no more operations done.
func (s *State) Close() {
	select {
	case <-s.closedCh:
	default:
		close(s.closedCh)
	}
}

func (s *State) runPump(
	ctx context.Context,
	allocationCh chan []types.NodeAddress,
	deallocationCh <-chan []types.NodeAddress,
	allocationPoolCh chan<- []types.NodeAddress,
) error {
	// Any address from the pool uniquely identifies entire pool, so I take the first one (allocated as the last one).
	var invalidAddress types.NodeAddress
	// Trick to save on `if` later in the handler.
	previousDeallocatedPool := []types.NodeAddress{0}

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
