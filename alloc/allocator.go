package alloc

import (
	"github.com/pkg/errors"

	"github.com/outofforest/quantum/types"
)

// NewAllocationCh creates channel containing allocatable addresses.
func NewAllocationCh(
	size uint64,
	nodesPerGroup uint64,
) (chan []types.NodeAddress, types.NodeAddress) {
	const singularityNodeCount = 1

	numOfNodes := size / types.NodeLength
	numOfNodes -= singularityNodeCount

	numOfGroups := numOfNodes / nodesPerGroup
	numOfNodes = numOfGroups * nodesPerGroup
	totalNumOfNodes := singularityNodeCount + numOfNodes

	availableNodes := make([]types.NodeAddress, 0, numOfNodes)
	for i := types.NodeAddress(singularityNodeCount); i < types.NodeAddress(totalNumOfNodes); i++ {
		availableNodes = append(availableNodes, i)
	}

	availableNodesCh := make(chan []types.NodeAddress, numOfGroups)
	for i := uint64(0); i < uint64(len(availableNodes)); i += nodesPerGroup {
		availableNodesCh <- availableNodes[i : i+nodesPerGroup]
	}

	var singularityNode types.NodeAddress
	return availableNodesCh, singularityNode
}

func newAllocator(state *State, tapCh <-chan []types.NodeAddress) *Allocator {
	pool := <-tapCh
	return &Allocator{
		state: state,
		tapCh: tapCh,
		pool:  pool,
	}
}

// Allocator allocates nodes in chunks.
type Allocator struct {
	state *State
	tapCh <-chan []types.NodeAddress
	pool  []types.NodeAddress
}

// Allocate allocates single node.
func (a *Allocator) Allocate() (types.NodeAddress, error) {
	nodeAddress := a.pool[len(a.pool)-1]
	a.pool = a.pool[:len(a.pool)-1]

	if len(a.pool) == 0 {
		var ok bool
		if a.pool, ok = <-a.tapCh; !ok {
			return 0, errors.New("allocation failed")
		}
	}

	return nodeAddress, nil
}

func newDeallocator(nodesPerGroup uint64, sinkCh chan<- []types.NodeAddress) *Deallocator {
	return &Deallocator{
		sinkCh:  sinkCh,
		release: make([]types.NodeAddress, 0, nodesPerGroup),
	}
}

// Deallocator deallocates nodes in chunks.
type Deallocator struct {
	sinkCh  chan<- []types.NodeAddress
	release []types.NodeAddress
}

// Deallocate deallocates single node.
func (d *Deallocator) Deallocate(nodeAddress types.NodeAddress) {
	if nodeAddress == 0 {
		return
	}

	d.release = append(d.release, nodeAddress)
	if len(d.release) == cap(d.release) {
		d.sinkCh <- d.release

		// FIXME (wojciech): Avoid heap allocation
		d.release = make([]types.NodeAddress, 0, cap(d.release))
	}
}
