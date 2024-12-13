package alloc

import (
	"github.com/pkg/errors"
)

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

	capacity                        uint64
	allocPtr, commitPtr, deallocPtr uint64
}

func (r *ring[T]) Allocate() (T, error) {
	if r.allocPtr == r.commitPtr {
		var t T
		return t, errors.New("no free address to allocate")
	}
	if r.allocPtr == r.capacity {
		r.allocPtr = 0
	}
	a := r.addresses[r.allocPtr]
	r.allocPtr++
	return a, nil
}

func (r *ring[T]) Deallocate(item T) {
	if r.deallocPtr == r.capacity {
		r.deallocPtr = 0
	}
	if r.deallocPtr == r.allocPtr {
		// This is really critical because it means that we deallocated more than allocated.
		panic("no space left in the ring for deallocation")
	}

	r.addresses[r.deallocPtr] = item
	r.deallocPtr++
}

func (r *ring[T]) Commit() {
	r.commitPtr = r.deallocPtr - 1
}
