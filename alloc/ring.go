package alloc

import "github.com/pkg/errors"

func newRing[T any](capacity uint64) (*ring[T], []T) {
	addresses := make([]T, capacity)
	return &ring[T]{
		addresses: addresses,
		capacity:  capacity,
		commitPtr: capacity,
	}, addresses
}

type ring[T any] struct {
	addresses []T

	capacity                  uint64
	getPtr, commitPtr, putPtr uint64
}

func (r *ring[T]) Get() (T, error) {
	if r.getPtr == r.commitPtr {
		var t T
		return t, errors.New("no free item to get")
	}
	if r.getPtr == r.capacity {
		r.getPtr = 0
	}
	a := r.addresses[r.getPtr]
	r.getPtr++
	return a, nil
}

func (r *ring[T]) Put(item T) {
	if r.putPtr == r.capacity {
		r.putPtr = 0
	}
	if r.putPtr == r.getPtr {
		// This is really critical because it means that we deallocated more than allocated.
		panic("no space left in the ring")
	}

	r.addresses[r.putPtr] = item
	r.putPtr++
}

func (r *ring[T]) Commit() {
	r.commitPtr = r.putPtr - 1
}
