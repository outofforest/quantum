package quantum

import (
	"github.com/cespare/xxhash"
	"github.com/samber/lo"

	"github.com/outofforest/photon"
)

const uint64Length = 8

// State enumerates possible slot states.
type State byte

const (
	stateFree State = iota
	stateData
	statePointer
)

// SnapshotConfig stores snapshot configuration.
type SnapshotConfig struct {
	Allocator *Allocator
}

// NodeHeader is the header common to all node types.
type NodeHeader struct {
	Version uint64
	HashMod uint64
}

// Node represents data stored inside node.
type Node[T comparable] struct {
	Header *NodeHeader
	States []State
	Items  []T
}

// DataItem stores single key-value pair.
type DataItem[K, V comparable] struct {
	Hash  uint64
	Key   K
	Value V
}

// New creates new quantum store.
func New[K, V comparable](config SnapshotConfig) (Snapshot[K, V], error) {
	pointerNodeAllocator, err := NewNodeAllocator[uint64](config.Allocator)
	if err != nil {
		return Snapshot[K, V]{}, err
	}

	dataNodeAllocator, err := NewNodeAllocator[DataItem[K, V]](config.Allocator)
	if err != nil {
		return Snapshot[K, V]{}, err
	}

	s := Snapshot[K, V]{
		config:               config,
		pointerNodeAllocator: pointerNodeAllocator,
		dataNodeAllocator:    dataNodeAllocator,
		rootInfo: parentInfo{
			State: lo.ToPtr(stateFree),
			Item:  lo.ToPtr[uint64](0),
		},
		defaultValue: *new(V),
	}
	return s, nil
}

// Snapshot represents the state at particular point in time.
type Snapshot[K, V comparable] struct {
	config               SnapshotConfig
	pointerNodeAllocator NodeAllocator[uint64]
	dataNodeAllocator    NodeAllocator[DataItem[K, V]]

	version      uint64
	rootInfo     parentInfo
	hashMod      uint64
	defaultValue V
}

// Next transitions to the next snapshot of the state.
func (s Snapshot[K, V]) Next() Snapshot[K, V] {
	s.version++
	s.rootInfo = parentInfo{
		State: lo.ToPtr(*s.rootInfo.State),
		Item:  lo.ToPtr(*s.rootInfo.Item),
	}
	return s
}

// Get gets the value of the key.
func (s *Snapshot[K, V]) Get(key K) (value V, exists bool) {
	h := hashKey(key, 0)
	pInfo := s.rootInfo

	for {
		switch *pInfo.State {
		case stateFree:
			return s.defaultValue, false
		case stateData:
			_, dataNode := s.dataNodeAllocator.Get(*pInfo.Item)
			if dataNode.Header.HashMod > 0 {
				h = hashKey(key, dataNode.Header.HashMod)
			}

			index := s.dataNodeAllocator.Index(h)

			if dataNode.States[index] == stateFree {
				return s.defaultValue, false
			}
			item := dataNode.Items[index]
			if item.Hash == h && item.Key == key {
				return item.Value, true
			}
			return s.defaultValue, false
		default:
			_, pointerNode := s.pointerNodeAllocator.Get(*pInfo.Item)
			if pointerNode.Header.HashMod > 0 {
				h = hashKey(key, pointerNode.Header.HashMod)
			}

			index := s.pointerNodeAllocator.Index(h)
			h = s.pointerNodeAllocator.Shift(h)

			pInfo = parentInfo{
				State: &pointerNode.States[index],
				Item:  &pointerNode.Items[index],
			}
		}
	}
}

// Set sets the value for the key.
func (s *Snapshot[K, V]) Set(key K, value V) {
	s.set(s.rootInfo, DataItem[K, V]{
		Hash:  hashKey(key, 0),
		Key:   key,
		Value: value,
	})
}

type parentInfo struct {
	State *State
	Item  *uint64
}

func (s *Snapshot[K, V]) set(pInfo parentInfo, item DataItem[K, V]) {
	for {
		switch *pInfo.State {
		case stateFree:
			dataNodeIndex, _, dataNode := s.dataNodeAllocator.Allocate()
			dataNode.Header.Version = s.version

			*pInfo.State = stateData
			*pInfo.Item = dataNodeIndex

			index := s.dataNodeAllocator.Index(item.Hash)
			dataNode.States[index] = stateData
			dataNode.Items[index] = item

			return
		case stateData:
			dataNodeData, dataNode := s.dataNodeAllocator.Get(*pInfo.Item)
			if dataNode.Header.Version < s.version {
				newNodeIndex, newNodeData, newNode := s.dataNodeAllocator.Allocate()
				copy(newNodeData, dataNodeData)
				dataNode = newNode
				dataNode.Header.Version = s.version
				*pInfo.Item = newNodeIndex
			}
			if dataNode.Header.HashMod > 0 {
				item.Hash = hashKey(item.Key, dataNode.Header.HashMod)
			}

			index := s.dataNodeAllocator.Index(item.Hash)
			if dataNode.States[index] == stateFree {
				dataNode.States[index] = stateData
				dataNode.Items[index] = item

				return
			}

			if item.Hash == dataNode.Items[index].Hash {
				if item.Key == dataNode.Items[index].Key {
					dataNode.Items[index] = item

					return
				}

				// hash conflict
				s.hashMod++
				dataNode.Header.HashMod = s.hashMod
			}

			s.redistributeNode(pInfo)
			s.set(pInfo, item)

			return
		default:
			pointerNodeData, pointerNode := s.pointerNodeAllocator.Get(*pInfo.Item)
			if pointerNode.Header.Version < s.version {
				newNodeIndex, newNodeData, newNode := s.pointerNodeAllocator.Allocate()
				copy(newNodeData, pointerNodeData)
				pointerNode = newNode
				pointerNode.Header.Version = s.version
				*pInfo.Item = newNodeIndex
			}
			if pointerNode.Header.HashMod > 0 {
				item.Hash = hashKey(item.Key, pointerNode.Header.HashMod)
			}

			index := s.pointerNodeAllocator.Index(item.Hash)
			item.Hash = s.pointerNodeAllocator.Shift(item.Hash)

			pInfo = parentInfo{
				State: &pointerNode.States[index],
				Item:  &pointerNode.Items[index],
			}
		}
	}
}

func (s *Snapshot[K, V]) redistributeNode(pInfo parentInfo) {
	_, dataNode := s.dataNodeAllocator.Get(*pInfo.Item)

	pointerNodeIndex, _, pointerNode := s.pointerNodeAllocator.Allocate()
	*pointerNode.Header = NodeHeader{
		Version: s.version,
		HashMod: dataNode.Header.HashMod,
	}

	*pInfo.State = statePointer
	*pInfo.Item = pointerNodeIndex

	for i, state := range dataNode.States {
		if state == stateFree {
			continue
		}

		s.set(pInfo, dataNode.Items[i])
	}
}

func hashKey[K comparable](key K, hashMod uint64) uint64 {
	var hash uint64
	p := photon.NewFromValue[K](&key)
	if hashMod == 0 {
		hash = xxhash.Sum64(p.B)
	} else {
		// FIXME (wojciech): Remove heap allocation
		b := make([]byte, uint64Length+len(p.B))
		copy(b, photon.NewFromValue(&hashMod).B)
		copy(b[uint64Length:], photon.NewFromValue[K](&key).B)
		hash = xxhash.Sum64(b)
	}

	if isTesting {
		hash = testHash(hash)
	}

	return hash
}

func testHash(hash uint64) uint64 {
	return hash & 0x7fffffff
}
