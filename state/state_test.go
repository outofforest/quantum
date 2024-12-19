package state

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/quantum/types"
)

const stateSize = 10 * types.NodeLength

func TestVolatileAllocation(t *testing.T) {
	requireT := require.New(t)

	s := NewForTest(t, stateSize)
	allocator := s.NewVolatileAllocator()
	deallocator := s.NewVolatileDeallocator()

	addresses := make([]types.VolatileAddress, 0, stateSize/types.NodeLength-1)

	for {
		address, err := allocator.Allocate()
		if err != nil {
			break
		}
		addresses = append(addresses, address)
	}

	requireT.Equal([]types.VolatileAddress{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}, addresses)

	deallocator.Deallocate(0x0a)
	deallocator.Deallocate(0x0b)
	deallocator.Deallocate(0x0c)

	_, err := allocator.Allocate()
	requireT.Error(err)

	s.Commit()

	addresses = addresses[:0]

	for {
		address, err := allocator.Allocate()
		if err != nil {
			break
		}
		addresses = append(addresses, address)
	}

	requireT.Equal([]types.VolatileAddress{0x09, 0x0a, 0x0b}, addresses)
}

func TestPersistentAllocation(t *testing.T) {
	requireT := require.New(t)

	s := NewForTest(t, stateSize)
	allocator := s.NewPersistentAllocator()
	deallocator := s.NewPersistentDeallocator()

	addresses := make([]types.PersistentAddress, 0, 2*stateSize/types.NodeLength-numOfPersistentSingularityNodes)

	for {
		address, err := allocator.Allocate()
		if err != nil {
			break
		}
		addresses = append(addresses, address)
	}

	requireT.Equal([]types.PersistentAddress{0x01, 0x03, 0x05, 0x07, 0x09, 0x0b, 0x0d, 0x0f, 0x10, 0x11, 0x12},
		addresses)

	deallocator.Deallocate(0x0a)
	deallocator.Deallocate(0x0b)
	deallocator.Deallocate(0x0c)

	_, err := allocator.Allocate()
	requireT.Error(err)

	s.Commit()

	addresses = addresses[:0]

	for {
		address, err := allocator.Allocate()
		if err != nil {
			break
		}
		addresses = append(addresses, address)
	}

	requireT.Equal([]types.PersistentAddress{0x13, 0x0a, 0x0b}, addresses)
}

func TestSingularityNodes(t *testing.T) {
	requireT := require.New(t)

	s := NewForTest(t, stateSize)

	expectedPersistentAddresses := []types.PersistentAddress{0x00, 0x02, 0x04, 0x06, 0x08, 0x0a, 0x0c, 0x0e}
	for i := range types.SnapshotID(numOfPersistentSingularityNodes) {
		sNode := s.SingularityNodeRoot(i)
		requireT.Equal(types.VolatileAddress(0), sNode.VolatileAddress)
		requireT.Equal(types.VolatileAddress(0), sNode.Pointer.VolatileAddress)
		requireT.Equal(expectedPersistentAddresses[i%numOfPersistentSingularityNodes], sNode.Pointer.PersistentAddress)
	}
}

func TestNode(t *testing.T) {
	requireT := require.New(t)

	s := NewForTest(t, stateSize)

	for i := range types.VolatileAddress(stateSize / types.NodeLength) {
		requireT.Equal(uintptr(s.Node(i))-uintptr(s.origin), uintptr(i*types.NodeLength))
	}
}

func TestBytes(t *testing.T) {
	requireT := require.New(t)

	s := NewForTest(t, stateSize)

	for i := range types.VolatileAddress(stateSize / types.NodeLength) {
		b := s.Bytes(i)
		for j := range b {
			b[j] = byte(i)
		}
	}

	data := unsafe.Slice((*byte)(s.origin), stateSize)
	for i, d := range data {
		requireT.Equal(byte(i/types.NodeLength), d)
	}
}

func TestClear(t *testing.T) {
	requireT := require.New(t)

	s := NewForTest(t, stateSize)

	for i := range types.VolatileAddress(stateSize / types.NodeLength) {
		b := s.Bytes(i)
		for j := range b {
			b[j] = byte(i)
		}
		if i%2 == 0 {
			s.Clear(i)
		}
	}

	data := unsafe.Slice((*byte)(s.origin), stateSize)
	for i, d := range data {
		addr := i / types.NodeLength
		if addr%2 == 0 {
			requireT.Zero(d)
		} else {
			requireT.Equal(byte(i/types.NodeLength), d)
		}
	}
}
