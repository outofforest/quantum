package alloc

import (
	"math/rand/v2"

	"github.com/outofforest/quantum/types"
)

// Address represents node address.
type Address interface {
	types.VolatileAddress | types.PersistentAddress
}

func newAllocationRing[A Address](
	size uint64,
	randomize bool,
) (*ring[A], A) {
	const singularityNodeCount = 1

	totalNumOfNodes := size / types.NodeLength
	numOfNodes := totalNumOfNodes - singularityNodeCount

	r, addresses := newRing[A](numOfNodes)
	for i := range A(numOfNodes) {
		addresses[i] = i + singularityNodeCount
	}

	// FIXME (wojciech): There is no evidence it helps. Consider removing this option.
	if randomize {
		// Randomly shuffle addresses so consecutive items possibly go to different regions of the device, improving
		// performance eventually.
		rand.Shuffle(len(addresses), func(i, j int) {
			addresses[i], addresses[j] = addresses[j], addresses[i]
		})
	}

	var singularityNode A
	return r, singularityNode
}

func newAllocator[A Address](r *ring[A]) *Allocator[A] {
	return &Allocator[A]{
		r: r,
	}
}

// Allocator allocates nodes in chunks.
type Allocator[A Address] struct {
	r *ring[A]
}

// Allocate allocates single node.
func (a *Allocator[A]) Allocate() (A, error) {
	return a.r.Get()
}

func newDeallocator[A Address](r *ring[A]) *Deallocator[A] {
	return &Deallocator[A]{
		r: r,
	}
}

// Deallocator deallocates nodes in chunks.
type Deallocator[A Address] struct {
	r *ring[A]
}

// Deallocate deallocates single node.
func (d *Deallocator[A]) Deallocate(nodeAddress A) {
	// ANDing with FlagNaked is done to erase flags from volatile addresses.
	// Persistent addresses don't have flags, but it is impossible to have so big values there anyway.
	d.r.Put(nodeAddress & types.FlagNaked)
}
