package quantum

import (
	"unsafe"

	"github.com/cespare/xxhash"

	"github.com/outofforest/photon"
)

// FIXME (wojciech): avoid individual heap allocations for nodes.

// New creates new quantum store.
func New[K comparable, V any]() Snapshot[K, V] {
	return Snapshot[K, V]{
		root:         new(node[K, V]),
		rootSet:      new(bool),
		defaultValue: *new(V),
	}
}

// Snapshot represents the state at particular point in time.
type Snapshot[K comparable, V any] struct {
	version      uint64
	root         *node[K, V]
	rootSet      *bool
	defaultValue V
	hasher       hasher[K]
	hashMod      uint64
}

// Next transitions to the next snapshot of the state.
func (s Snapshot[K, V]) Next() Snapshot[K, V] {
	s.version++

	r := *s.root
	s.root = &r

	rs := *s.rootSet
	s.rootSet = &rs

	return s
}

// Get gets the value of the key.
func (s Snapshot[K, V]) Get(key K) (value V, exists bool) {
	h := s.hasher.Hash(key)
	n := s.root
	for {
		if n == nil {
			return s.defaultValue, false
		}
		if n.Hash == h {
			if n.Key == key {
				return n.Value, true
			}

			// conflict
			if s.hasher.bytes == nil {
				s.hashMod++
				n.hasher = newHasher[K](s.hashMod)
			}
			h = n.hasher.Hash(key)
		}

		bit := h & 0x01
		h >>= 1

		switch bit {
		case 0x00:
			n = n.Left
		default:
			n = n.Right
		}
	}
}

// Set sets the value for the key.
func (s Snapshot[K, V]) Set(key K, value V) {
	const (
		leftChild int = iota
		rightChild
	)

	h := s.hasher.Hash(key)

	if !*s.rootSet {
		*s.root = node[K, V]{
			Value:   value,
			Key:     key,
			Version: s.version,
			Hash:    h,
		}
		*s.rootSet = true
		return
	}

	var parentNode *node[K, V]
	var child int
	n := s.root
	for {
		if n == nil {
			n = &node[K, V]{
				Value:   value,
				Key:     key,
				Version: s.version,
				Hash:    h,
			}

			if child == leftChild {
				parentNode.Left = n
			} else {
				parentNode.Right = n
			}
			return
		}
		if n.Version < s.version {
			n2 := *n
			n2.Version = s.version

			switch {
			case parentNode == nil:
				*s.root = n2
				n = s.root
			case child == leftChild:
				n = &n2
				parentNode.Left = n
			default:
				n = &n2
				parentNode.Right = n
			}
		}
		if n.Hash == h {
			if n.Key == key {
				n.Value = value
				return
			}

			// conflict
			if s.hasher.bytes == nil {
				s.hashMod++
				n.hasher = newHasher[K](s.hashMod)
			}
			h = n.hasher.Hash(key)
		}

		bit := h & 0x01
		h >>= 1
		parentNode = n

		switch bit {
		case 0x00:
			n = n.Left
			child = leftChild
		case 0x01:
			n = n.Right
			child = rightChild
		}
	}
}

type node[K comparable, V any] struct {
	Key   K
	Value V

	Version uint64
	Hash    uint64
	Left    *node[K, V]
	Right   *node[K, V]

	hasher hasher[K]
}

const uint64Length = 8

func newHasher[K comparable](mod uint64) hasher[K] {
	var k K
	var bytes []byte
	var data []byte
	if mod > 0 {
		bytes = make([]byte, uint64Length+unsafe.Sizeof(k))
		copy(bytes, photon.NewFromValue(&mod).B)
		data = bytes[uint64Length:]
	}

	return hasher[K]{
		bytes: bytes,
		data:  data,
	}
}

type hasher[K comparable] struct {
	bytes []byte
	data  []byte
}

func (h hasher[K]) Hash(key K) uint64 {
	var hash uint64
	if h.bytes == nil {
		hash = xxhash.Sum64(photon.NewFromValue[K](&key).B)
	} else {
		copy(h.data, photon.NewFromValue[K](&key).B)
		hash = xxhash.Sum64(h.data)
	}
	if isTesting {
		hash = testHash(hash)
	}
	return hash
}

func testHash(hash uint64) uint64 {
	return hash & 0x7fffffff
}
