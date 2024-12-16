package alloc

import (
	"unsafe"

	"github.com/pkg/errors"

	"github.com/outofforest/photon"
	"github.com/outofforest/quantum/types"
)

// NewState creates new DB state.
func NewState(
	volatileSize, persistentSize uint64,
	useHugePages bool,
) (*State, func(), error) {
	// Align allocated memory address to the node volatileSize. It might be required if using O_DIRECT option to open
	// files. As a side effect it is also 64-byte aligned which is required by the AVX512 instructions.
	dataP, deallocateFunc, err := Allocate(volatileSize, types.NodeLength, useHugePages)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "memory allocation failed")
	}

	volatileRing, singularityVolatileAddress := newAllocationRing[types.VolatileAddress](volatileSize, false)
	persistentRing, singularityPersistentAddress := newAllocationRing[types.PersistentAddress](persistentSize, false)

	return &State{
		singularityNodeRoot: types.ToStore{
			VolatileAddress: singularityVolatileAddress,
			Pointer: &types.Pointer{
				Revision:          1,
				VolatileAddress:   singularityVolatileAddress,
				PersistentAddress: singularityPersistentAddress,
			},
		},
		dataP:          dataP,
		volatileSize:   volatileSize,
		volatileRing:   volatileRing,
		persistentRing: persistentRing,
	}, deallocateFunc, nil
}

// State stores the DB state.
type State struct {
	singularityNodeRoot types.ToStore
	dataP               unsafe.Pointer
	volatileSize        uint64
	volatileRing        *ring[types.VolatileAddress]
	persistentRing      *ring[types.PersistentAddress]
}

// NewVolatileAllocator creates new volatile address allocator.
func (s *State) NewVolatileAllocator() *Allocator[types.VolatileAddress] {
	return newAllocator(s.volatileRing)
}

// NewVolatileDeallocator creates new volatile address deallocator.
func (s *State) NewVolatileDeallocator() *Deallocator[types.VolatileAddress] {
	return newDeallocator(s.volatileRing)
}

// NewPersistentAllocator creates new persistent address allocator.
func (s *State) NewPersistentAllocator() *Allocator[types.PersistentAddress] {
	return newAllocator(s.persistentRing)
}

// NewPersistentDeallocator creates new persistent address deallocator.
func (s *State) NewPersistentDeallocator() *Deallocator[types.PersistentAddress] {
	return newDeallocator(s.persistentRing)
}

// SingularityNodeRoot returns node root of singularity node.
func (s *State) SingularityNodeRoot() types.ToStore {
	return s.singularityNodeRoot
}

// Origin returns the pointer to the allocated memory.
func (s *State) Origin() unsafe.Pointer {
	return s.dataP
}

// VolatileSize returns size of the volatile memory.
func (s *State) VolatileSize() uint64 {
	return s.volatileSize
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

// Commit is called when new snapshot starts to mark point where invalid physical address is present in the queue.
func (s *State) Commit() {
	s.volatileRing.Commit()
	s.persistentRing.Commit()
}
