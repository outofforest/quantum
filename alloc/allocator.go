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
	nodeSize uint64,
	nodesPerGroup uint64,
	numOfReservedNodes uint64,
) (chan []A, []A) {
	numOfNodes := size / nodeSize
	numOfNodes -= numOfReservedNodes

	numOfGroups := numOfNodes / nodesPerGroup
	numOfNodes = numOfGroups * nodesPerGroup
	size = (numOfReservedNodes + numOfNodes) * nodeSize

	// FIXME (wojciech): Spread reserved nodes across the store
	reservedNodes := make([]A, 0, numOfReservedNodes)
	for i := range numOfReservedNodes {
		reservedNodes = append(reservedNodes, A(i*nodeSize))
	}

	availableNodes := make([]A, 0, numOfNodes)
	for i := numOfReservedNodes * nodeSize; i < size; i += nodeSize {
		availableNodes = append(availableNodes, A(i))
	}

	availableNodesCh := make(chan []A, numOfGroups)
	for i := uint64(0); i < uint64(len(availableNodes)); i += nodesPerGroup {
		availableNodesCh <- availableNodes[i : i+nodesPerGroup]
	}

	return availableNodesCh, reservedNodes
}
