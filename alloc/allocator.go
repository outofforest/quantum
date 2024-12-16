package alloc

import (
	"github.com/outofforest/quantum/types"
)

// Address represents node address.
type Address interface {
	types.VolatileAddress | types.PersistentAddress
}

func newAllocationRing[A Address](
	size uint64,
	singularityNodeCount uint64,
) (*ring[A], []A) {
	totalNumOfNodes := size / types.NodeLength
	singularityNodeFrequency := A(totalNumOfNodes / singularityNodeCount)

	r, addresses := newRing[A](totalNumOfNodes - singularityNodeCount)
	singularityNodes := make([]A, 0, singularityNodeCount)
	for i, j := A(0), 0; i < A(totalNumOfNodes); i++ {
		if i%singularityNodeFrequency == 0 {
			singularityNodes = append(singularityNodes, i)
		} else {
			addresses[j] = i
			j++
		}
	}

	return r, singularityNodes
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
