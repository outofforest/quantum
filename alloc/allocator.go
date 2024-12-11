package alloc

import (
	"math/rand/v2"

	"github.com/pkg/errors"

	"github.com/outofforest/quantum/types"
)

// Address represents node address.
type Address interface {
	types.VolatileAddress | types.PersistentAddress
}

// NewAllocationCh creates channel containing allocatable addresses.
func NewAllocationCh[A Address](
	size uint64,
	nodesPerGroup uint64,
	randomize bool,
) (chan []A, A) {
	const singularityNodeCount = 1

	numOfNodes := size / types.NodeLength
	numOfNodes -= singularityNodeCount

	numOfGroups := numOfNodes / nodesPerGroup
	numOfNodes = numOfGroups * nodesPerGroup
	totalNumOfNodes := singularityNodeCount + numOfNodes

	availableNodes := make([]A, 0, numOfNodes)
	for i := A(singularityNodeCount); i < A(totalNumOfNodes); i++ {
		availableNodes = append(availableNodes, i)
	}

	// FIXME (wojciech): There is no evidence it helps. Consider removing this option.
	if randomize {
		// Randomly shuffle addresses so consecutive items possibly go to different regions of the device, improving
		// performance eventually.
		rand.Shuffle(len(availableNodes), func(i, j int) {
			availableNodes[i], availableNodes[j] = availableNodes[j], availableNodes[i]
		})
	}

	availableNodesCh := make(chan []A, numOfGroups)
	for i := uint64(0); i < uint64(len(availableNodes)); i += nodesPerGroup {
		availableNodesCh <- availableNodes[i : i+nodesPerGroup]
	}

	var singularityNode A
	return availableNodesCh, singularityNode
}

func newAllocator[A Address](state *State, tapCh <-chan []A) *Allocator[A] {
	pool := <-tapCh
	return &Allocator[A]{
		state: state,
		tapCh: tapCh,
		pool:  pool,
	}
}

// Allocator allocates nodes in chunks.
type Allocator[A Address] struct {
	state *State
	tapCh <-chan []A
	pool  []A
}

// Allocate allocates single node.
func (a *Allocator[A]) Allocate() (A, error) {
	nodeAddress := a.pool[len(a.pool)-1]
	a.pool = a.pool[:len(a.pool)-1]

	if len(a.pool) == 0 {
		var ok bool
		if a.pool, ok = <-a.tapCh; !ok {
			var a A
			return 0, errors.Errorf("allocation failed: %T", a)
		}
	}

	return nodeAddress, nil
}

func newDeallocator[A Address](nodesPerGroup uint64, sinkCh chan<- []A) *Deallocator[A] {
	return &Deallocator[A]{
		sinkCh:  sinkCh,
		release: make([]A, 0, nodesPerGroup),
	}
}

// Deallocator deallocates nodes in chunks.
type Deallocator[A Address] struct {
	sinkCh  chan<- []A
	release []A
}

// Deallocate deallocates single node.
func (d *Deallocator[A]) Deallocate(nodeAddress A) {
	d.release = append(d.release, nodeAddress)
	if len(d.release) == cap(d.release) {
		d.sinkCh <- d.release

		// FIXME (wojciech): Avoid heap allocation
		d.release = make([]A, 0, cap(d.release))
	}
}
