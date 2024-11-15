package alloc

import (
	"github.com/outofforest/quantum/types"
)

// Address defines accepted address types.
type Address interface {
	types.VolatileAddress | types.PersistentAddress
}

// NewAllocationCh creates channel containing allocatable addresses.
func NewAllocationCh[A Address](
	size uint64,
	nodesPerGroup uint64,
) (chan []A, A) {
	const singularityNodeCount = 1

	numOfNodes := size / types.NodeLength
	numOfNodes -= singularityNodeCount

	numOfGroups := numOfNodes / nodesPerGroup
	numOfNodes = numOfGroups * nodesPerGroup
	totalNumOfNodes := singularityNodeCount + numOfNodes

	availableNodes := make([]A, 0, numOfNodes)
	for i := uint64(singularityNodeCount); i < totalNumOfNodes; i++ {
		availableNodes = append(availableNodes, A(i*types.NodeLength))
	}

	availableNodesCh := make(chan []A, numOfGroups)
	for i := uint64(0); i < uint64(len(availableNodes)); i += nodesPerGroup {
		availableNodesCh <- availableNodes[i : i+nodesPerGroup]
	}

	var singularityNode A
	return availableNodesCh, singularityNode
}
