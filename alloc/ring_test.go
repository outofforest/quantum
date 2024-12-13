package alloc

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const ringCapacity = 10

func prepRing() *ring[int] {
	r, addresses := newRing[int](ringCapacity)
	for i := range addresses {
		addresses[i] = i
	}
	return r
}

func TestRingMaxAllocation(t *testing.T) {
	requireT := require.New(t)
	r := prepRing()

	for i := range ringCapacity {
		item, err := r.Allocate()
		requireT.NoError(err, i)
		requireT.Equal(i, item)
	}

	item, err := r.Allocate()
	requireT.Error(err)
	requireT.Equal(0, item)
}

func TestRingAllocationDeallocationWithoutCommitFromTheBeginning(t *testing.T) {
	requireT := require.New(t)
	r := prepRing()

	for i := range ringCapacity {
		item, err := r.Allocate()
		requireT.NoError(err, i)
		requireT.Equal(i, item)

		r.Deallocate(item)
	}

	item, err := r.Allocate()
	requireT.Error(err)
	requireT.Equal(0, item)
}

func TestRingAllocationDeallocationWithCommitFromTheBeginning(t *testing.T) {
	requireT := require.New(t)
	r := prepRing()

	for i := range ringCapacity {
		item, err := r.Allocate()
		requireT.NoError(err, i)
		requireT.Equal(i, item)

		r.Deallocate(item)
	}

	r.Commit()

	for range 10 {
		for i := range ringCapacity - 1 {
			item, err := r.Allocate()
			requireT.NoError(err, i)
			requireT.Equal(i, item)

			r.Deallocate(item)
		}

		item, err := r.Allocate()
		requireT.Error(err)
		requireT.Equal(0, item)

		r.Commit()

		item, err = r.Allocate()
		requireT.NoError(err)
		requireT.Equal(ringCapacity-1, item)

		r.Deallocate(item)
		r.Commit()
	}
}

func TestRingAllocationDeallocationWithoutCommitFromTheMiddle(t *testing.T) {
	requireT := require.New(t)
	r := prepRing()

	// Prepare

	for range ringCapacity / 2 {
		item, err := r.Allocate()
		requireT.NoError(err)
		r.Deallocate(item)
	}

	r.Commit()

	// Test

	for i := range ringCapacity - 1 {
		item, err := r.Allocate()
		requireT.NoError(err, i)
		requireT.Equal((i+ringCapacity/2)%ringCapacity, item)

		r.Deallocate(item)
	}

	item, err := r.Allocate()
	requireT.Error(err)
	requireT.Equal(0, item)
}

func TestRingAllocationDeallocationWithCommitFromTheMiddle(t *testing.T) {
	requireT := require.New(t)
	r := prepRing()

	// Prepare

	for range ringCapacity / 2 {
		item, err := r.Allocate()
		requireT.NoError(err)
		r.Deallocate(item)
	}

	r.Commit()

	// Test

	for range 10 {
		for i := range ringCapacity - 1 {
			item, err := r.Allocate()
			requireT.NoError(err, i)
			requireT.Equal((i+ringCapacity/2)%ringCapacity, item)

			r.Deallocate(item)
		}

		item, err := r.Allocate()
		requireT.Error(err)
		requireT.Equal(0, item)

		r.Commit()

		item, err = r.Allocate()
		requireT.NoError(err)
		requireT.Equal(ringCapacity/2-1, item)

		r.Deallocate(item)
		r.Commit()
	}
}

func TestRingAllocationDeallocationWithoutCommitFromTheEnd(t *testing.T) {
	requireT := require.New(t)
	r := prepRing()

	// Prepare

	for range ringCapacity {
		item, err := r.Allocate()
		requireT.NoError(err)
		r.Deallocate(item)
	}

	r.Commit()

	// Test

	for i := range ringCapacity - 1 {
		item, err := r.Allocate()
		requireT.NoError(err, i)
		requireT.Equal(i, item)

		r.Deallocate(item)
	}

	item, err := r.Allocate()
	requireT.Error(err)
	requireT.Equal(0, item)
}

func TestRingAllocationDeallocationWithCommitFromTheEnd(t *testing.T) {
	requireT := require.New(t)
	r := prepRing()

	// Prepare

	for range ringCapacity {
		item, err := r.Allocate()
		requireT.NoError(err)
		r.Deallocate(item)
	}

	r.Commit()

	// Test

	for range 10 {
		for i := range ringCapacity - 1 {
			item, err := r.Allocate()
			requireT.NoError(err, i)
			requireT.Equal((i+ringCapacity)%ringCapacity, item)

			r.Deallocate(item)
		}

		item, err := r.Allocate()
		requireT.Error(err)
		requireT.Equal(0, item)

		r.Commit()

		item, err = r.Allocate()
		requireT.NoError(err)
		requireT.Equal(ringCapacity-1, item)

		r.Deallocate(item)
		r.Commit()
	}
}

func TestRingAllocationDeallocationCommitOneByOne(t *testing.T) {
	requireT := require.New(t)
	r := prepRing()

	for i := range 10 * ringCapacity {
		item, err := r.Allocate()
		requireT.NoError(err, i)
		requireT.Equal(i%ringCapacity, item)

		r.Deallocate(item)
		r.Commit()
	}
}

func TestRingDeallocationPanicsIfNothingHasBeenAllocated(t *testing.T) {
	requireT := require.New(t)
	r := prepRing()

	requireT.Panics(func() {
		r.Deallocate(100)
	})
	requireT.Panics(func() {
		r.Deallocate(101)
	})

	_, err := r.Allocate()
	requireT.NoError(err)

	r.Deallocate(102)
}

func TestRingDeallocationPanicsIfDeallocatingMoreThanAllocated(t *testing.T) {
	requireT := require.New(t)
	r := prepRing()

	_, err := r.Allocate()
	requireT.NoError(err)

	r.Deallocate(100)

	requireT.Panics(func() {
		r.Deallocate(101)
	})
	requireT.Panics(func() {
		r.Deallocate(102)
	})

	_, err = r.Allocate()
	requireT.NoError(err)

	r.Deallocate(103)
}
