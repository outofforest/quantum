package quantum

import (
	"unsafe"

	"github.com/cespare/xxhash"

	"github.com/outofforest/photon"
)

// FIXME (wojciech): reclaim abandoned nodes to save on heap allocations.

const (
	bitsPerHop   = 4
	arraySize    = 1 << bitsPerHop
	mask         = arraySize - 1
	uint64Length = 8
)

type pointerType byte

const (
	freePointerType pointerType = iota
	kvPointerType
	nodePointerType
)

// New creates new quantum store.
func New[K comparable, V any]() Snapshot[K, V] {
	return Snapshot[K, V]{
		root: node[K, V]{
			KVs: &[arraySize]kvPair[K, V]{},
		},
		rootNodeType: kvPointerType,
		defaultValue: *new(V),
	}
}

// Snapshot represents the state at particular point in time.
type Snapshot[K comparable, V any] struct {
	version      uint64
	root         node[K, V]
	rootNodeType pointerType
	defaultValue V
	hasher       hasher[K]
	hashMod      uint64
}

// Next transitions to the next snapshot of the state.
func (s Snapshot[K, V]) Next() Snapshot[K, V] {
	s.version++
	return s
}

// Get gets the value of the key.
func (s *Snapshot[K, V]) Get(key K) (value V, exists bool) {
	h := s.hasher.Hash(key)
	nType := s.rootNodeType
	n := s.root
	for {
		if n.hasher.bytes != nil {
			h = n.hasher.Hash(key)
		}

		index := h & mask
		h >>= bitsPerHop

		if n.Types[index] == freePointerType {
			return s.defaultValue, false
		}

		switch nType {
		case nodePointerType:
			nType = n.Types[index]
			n = n.Pointers[index]
		default:
			kv := n.KVs[index]
			if kv.Hash == h && kv.Key == key {
				return kv.Value, true
			}
			return s.defaultValue, false
		}
	}
}

// Set sets the value for the key.
func (s *Snapshot[K, V]) Set(key K, value V) {
	h := s.hasher.Hash(key)
	nType := s.rootNodeType
	n := &s.root

	var parentNode *node[K, V]
	var parentIndex uint64

	for {
		if n.Version < s.version {
			n2 := node[K, V]{
				Version: s.version,
				Types:   n.Types,
				hasher:  n.hasher,
			}
			if nType == kvPointerType {
				kvs := *n.KVs
				n2.KVs = &kvs
			} else {
				pointers := *n.Pointers
				n2.Pointers = &pointers
			}

			switch {
			case parentNode == nil:
				s.root = n2
				n = &s.root
			default:
				parentNode.Pointers[parentIndex] = n2
				n = &parentNode.Pointers[parentIndex]
			}
		}

		if n.hasher.bytes != nil {
			h = n.hasher.Hash(key)
		}

		index := h & mask
		h >>= bitsPerHop

		switch nType {
		case nodePointerType:
			if n.Types[index] == freePointerType {
				n.Types[index] = kvPointerType
				n.Pointers[index] = node[K, V]{
					Version: s.version,
					KVs:     &[arraySize]kvPair[K, V]{},
				}
			}
			parentIndex = index
			parentNode = n
			nType = n.Types[index]
			n = &n.Pointers[index]
		default:
			if n.Types[index] == freePointerType {
				n.Types[index] = kvPointerType
				n.KVs[index] = kvPair[K, V]{
					Hash:  h,
					Key:   key,
					Value: value,
				}
				return
			}

			kv := n.KVs[index]
			var conflict bool
			if kv.Hash == h {
				if kv.Key == key {
					n.KVs[index].Value = value
					return
				}

				// hash conflict

				conflict = true
			}

			// conflict or split needed

			n2 := node[K, V]{
				Version:  s.version,
				Pointers: &[arraySize]node[K, V]{},
				hasher:   n.hasher,
			}

			for i := range uint64(arraySize) {
				if n.Types[i] == freePointerType {
					continue
				}

				n2.Types[i] = kvPointerType
				n2.Pointers[i] = node[K, V]{
					Version: s.version,
					KVs:     &[arraySize]kvPair[K, V]{},
				}

				kv := n.KVs[i]
				var hash uint64
				if conflict && i == index {
					s.hashMod++
					n2.Pointers[i].hasher = newHasher[K](s.hashMod)
					hash = n2.Pointers[i].hasher.Hash(kv.Key)
				} else {
					hash = kv.Hash
				}

				index := hash & mask
				n2.Pointers[i].Types[index] = kvPointerType
				n2.Pointers[i].KVs[index] = kvPair[K, V]{
					Hash:  hash >> bitsPerHop,
					Key:   kv.Key,
					Value: kv.Value,
				}
			}

			if parentNode == nil {
				s.rootNodeType = nodePointerType
				s.root = n2
				parentNode = &s.root
			} else {
				parentNode.Types[parentIndex] = nodePointerType
				parentNode.Pointers[parentIndex] = n2
				parentNode = &parentNode.Pointers[parentIndex]
			}

			parentIndex = index
			n = &n2.Pointers[index]
		}
	}
}

type kvPair[K comparable, V any] struct {
	Hash  uint64
	Key   K
	Value V
}

type node[K comparable, V any] struct {
	Version uint64
	hasher  hasher[K]

	Types    [arraySize]pointerType
	KVs      *[arraySize]kvPair[K, V]
	Pointers *[arraySize]node[K, V]
}

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
		hash = xxhash.Sum64(h.bytes)
	}
	if isTesting {
		hash = testHash(hash)
	}
	return hash
}

func testHash(hash uint64) uint64 {
	return hash & 0x7fffffff
}
