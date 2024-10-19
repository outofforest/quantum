package alloc

import (
	"github.com/outofforest/quantum/types"
)

type Address interface {
	types.LogicalAddress | types.PhysicalAddress
}

func NewAllocationCh[A Address](
	size uint64,
	nodeSize uint64,
	nodesPerGroup uint64,
) chan []A {
	numOfGroups := size / nodeSize / nodesPerGroup
	numOfNodes := numOfGroups * nodesPerGroup
	size = numOfNodes * nodeSize

	availableNodes := make([]A, 0, numOfNodes)
	for i := nodeSize; i < size; i += nodeSize {
		availableNodes = append(availableNodes, A(i))
	}

	availableNodesCh := make(chan []A, numOfGroups)
	for i := uint64(0); i < uint64(len(availableNodes)); i += nodesPerGroup {
		availableNodesCh <- availableNodes[i : i+nodesPerGroup]
	}

	return availableNodesCh
}
