package alloc

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/quantum/types"
)

func TestAllocationRing(t *testing.T) {
	const (
		numOfNodes            = 25
		numOfSingularityNodes = 4
	)
	requireT := require.New(t)

	r, sNodes := newAllocationRing[types.VolatileAddress](numOfNodes*types.NodeLength, numOfSingularityNodes)

	requireT.Equal([]types.VolatileAddress{0x00, 0x06, 0x0c, 0x12}, sNodes)

	allocator := newAllocator(r)
	addresses := make([]types.VolatileAddress, 0, numOfNodes-numOfSingularityNodes)

	for {
		address, err := allocator.Allocate()
		if err != nil {
			break
		}
		addresses = append(addresses, address)
	}

	requireT.Equal([]types.VolatileAddress{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x13, 0x14, 0x15,
		0x16, 0x17,
	}, addresses)

	deallocator := newDeallocator(r)
	deallocator.Deallocate(0x20)
	deallocator.Deallocate(0x21)
	deallocator.Deallocate(0x22)

	_, err := allocator.Allocate()
	requireT.Error(err)

	r.Commit()

	addresses = addresses[:0]

	for {
		address, err := allocator.Allocate()
		if err != nil {
			break
		}
		addresses = append(addresses, address)
	}

	requireT.Equal([]types.VolatileAddress{0x18, 0x020, 0x21}, addresses)

	// Verify that flags are removed.
	deallocator.Deallocate(0x30 | (1 << 62))
	deallocator.Deallocate(0x00)

	r.Commit()

	addresses = addresses[:0]

	for {
		address, err := allocator.Allocate()
		if err != nil {
			break
		}
		addresses = append(addresses, address)
	}

	requireT.Equal([]types.VolatileAddress{0x22, 0x030}, addresses)
}
