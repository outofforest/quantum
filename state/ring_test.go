package state

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const ringCapacity = 10

func prepRing() *ring[int] {
	r, items := newRing[int](ringCapacity)
	for i := range items {
		items[i] = i
	}
	return r
}

func TestRingMaxAllocation(t *testing.T) {
	requireT := require.New(t)
	r := prepRing()

	for i := range ringCapacity - 1 {
		item, err := r.Get()
		requireT.NoError(err, i)
		requireT.Equal(i, item)
	}

	item, err := r.Get()
	requireT.Error(err)
	requireT.Equal(0, item)
}

func TestRingAllocationDeallocationWithoutCommitFromTheBeginning(t *testing.T) {
	requireT := require.New(t)
	r := prepRing()

	for i := range ringCapacity - 1 {
		item, err := r.Get()
		requireT.NoError(err, i)
		requireT.Equal(i, item)

		r.Put(item)
	}

	item, err := r.Get()
	requireT.Error(err)
	requireT.Equal(0, item)
}

func TestRingAllocationDeallocationWithCommitFromTheBeginning(t *testing.T) {
	requireT := require.New(t)
	r := prepRing()

	for i := range ringCapacity - 1 {
		item, err := r.Get()
		requireT.NoError(err, i)
		requireT.Equal(i, item)

		r.Put(item)
	}

	r.Commit()

	for range 10 {
		for i := range ringCapacity - 1 {
			item, err := r.Get()
			requireT.NoError(err, i)
			requireT.Equal((i+ringCapacity-1)%ringCapacity, item)

			r.Put(item)
		}

		item, err := r.Get()
		requireT.Error(err)
		requireT.Equal(0, item)

		r.Commit()

		item, err = r.Get()
		requireT.NoError(err)
		requireT.Equal(ringCapacity-2, item)

		r.Put(item)
		r.Commit()
	}
}

func TestRingAllocationDeallocationWithoutCommitFromTheMiddle(t *testing.T) {
	requireT := require.New(t)
	r := prepRing()

	// Prepare

	for range ringCapacity / 2 {
		item, err := r.Get()
		requireT.NoError(err)
		r.Put(item)
	}

	r.Commit()

	// Test

	for i := range ringCapacity - 1 {
		item, err := r.Get()
		requireT.NoError(err, i)
		requireT.Equal((i+ringCapacity/2)%ringCapacity, item)

		r.Put(item)
	}

	item, err := r.Get()
	requireT.Error(err)
	requireT.Equal(0, item)
}

func TestRingAllocationDeallocationWithCommitFromTheMiddle(t *testing.T) {
	requireT := require.New(t)
	r := prepRing()

	// Prepare

	for range ringCapacity / 2 {
		item, err := r.Get()
		requireT.NoError(err)
		r.Put(item)
	}

	r.Commit()

	// Test

	for range 10 {
		for i := range ringCapacity - 1 {
			item, err := r.Get()
			requireT.NoError(err, i)
			requireT.Equal((i+ringCapacity/2)%ringCapacity, item)

			r.Put(item)
		}

		item, err := r.Get()
		requireT.Error(err)
		requireT.Equal(0, item)

		r.Commit()

		item, err = r.Get()
		requireT.NoError(err)
		requireT.Equal(ringCapacity/2-1, item)

		r.Put(item)
		r.Commit()
	}
}

func TestRingAllocationDeallocationWithoutCommitFromTheEnd(t *testing.T) {
	requireT := require.New(t)
	r := prepRing()

	// Prepare

	for range ringCapacity - 1 {
		item, err := r.Get()
		requireT.NoError(err)
		r.Put(item)
	}

	r.Commit()

	// Test

	for i := range ringCapacity - 1 {
		item, err := r.Get()
		requireT.NoError(err, i)
		requireT.Equal((i+ringCapacity-1)%ringCapacity, item)

		r.Put(item)
	}

	item, err := r.Get()
	requireT.Error(err)
	requireT.Equal(0, item)
}

func TestRingAllocationDeallocationWithCommitFromTheEnd(t *testing.T) {
	requireT := require.New(t)
	r := prepRing()

	// Prepare

	for range ringCapacity - 1 {
		item, err := r.Get()
		requireT.NoError(err)
		r.Put(item)
	}

	r.Commit()

	// Test

	for range 10 {
		for i := range ringCapacity - 1 {
			item, err := r.Get()
			requireT.NoError(err, i)
			requireT.Equal((i+ringCapacity-1)%ringCapacity, item)

			r.Put(item)
		}

		item, err := r.Get()
		requireT.Error(err)
		requireT.Equal(0, item)

		r.Commit()

		item, err = r.Get()
		requireT.NoError(err)
		requireT.Equal(ringCapacity-2, item)

		r.Put(item)
		r.Commit()
	}
}

func TestRingAllocationDeallocationCommitOneByOne(t *testing.T) {
	requireT := require.New(t)
	r := prepRing()

	for i := range 10 * ringCapacity {
		item, err := r.Get()
		requireT.NoError(err, i)
		requireT.Equal(i%ringCapacity, item)

		r.Put(item)
		r.Commit()
	}
}

func TestRingDeallocationPanicsIfNothingHasBeenAllocated(t *testing.T) {
	requireT := require.New(t)
	r := prepRing()

	requireT.Panics(func() {
		r.Put(100)
	})
	requireT.Panics(func() {
		r.Put(101)
	})

	_, err := r.Get()
	requireT.NoError(err)

	r.Put(102)
}

func TestRingDeallocationPanicsIfDeallocatingMoreThanAllocated(t *testing.T) {
	requireT := require.New(t)
	r := prepRing()

	_, err := r.Get()
	requireT.NoError(err)

	r.Put(100)

	requireT.Panics(func() {
		r.Put(101)
	})
	requireT.Panics(func() {
		r.Put(102)
	})

	_, err = r.Get()
	requireT.NoError(err)

	r.Put(103)
}

func TestRingDeallocationPanicsIfDeallocatingMoreThanAllocatedAfterAllocatingEntireRing(t *testing.T) {
	requireT := require.New(t)
	r := prepRing()

	for {
		_, err := r.Get()
		if err != nil {
			break
		}
	}

	for range ringCapacity - 1 {
		r.Put(100)
	}

	requireT.Panics(func() {
		r.Put(101)
	})
}

func TestCommitOnEmptyRing(t *testing.T) {
	requireT := require.New(t)
	r := prepRing()

	for {
		_, err := r.Get()
		if err != nil {
			break
		}
	}

	r.Commit()

	_, err := r.Get()
	requireT.Error(err)
}

func TestCommitOnRingWithOneItem(t *testing.T) {
	requireT := require.New(t)
	r := prepRing()

	for {
		_, err := r.Get()
		if err != nil {
			break
		}
	}

	r.Put(100)
	r.Commit()

	item, err := r.Get()
	requireT.NoError(err)
	requireT.Equal(9, item)

	_, err = r.Get()
	requireT.Error(err)
}
