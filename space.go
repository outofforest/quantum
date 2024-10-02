package quantum

import (
	"github.com/cespare/xxhash"

	"github.com/outofforest/photon"
)

// SpaceConfig stores space configuration.
type SpaceConfig[K, V comparable] struct {
	SnapshotID           uint64
	HashMod              *uint64
	SpaceRoot            ParentInfo
	PointerNodeAllocator NodeAllocator[uint64]
	DataNodeAllocator    NodeAllocator[DataItem[K, V]]
}

// NewSpace creates new space.
func NewSpace[K, V comparable](config SpaceConfig[K, V]) (*Space[K, V], error) {
	return &Space[K, V]{
		config:       config,
		defaultValue: *new(V),
	}, nil
}

// Space represents the substate where values V are stored by key K.
type Space[K, V comparable] struct {
	config       SpaceConfig[K, V]
	defaultValue V
}

// Get gets the value of the key.
func (s *Space[K, V]) Get(key K) (value V, exists bool) {
	h := hashKey(key, 0)
	pInfo := s.config.SpaceRoot

	for {
		switch *pInfo.State {
		case stateFree:
			return s.defaultValue, false
		case stateData:
			_, dataNode := s.config.DataNodeAllocator.Get(*pInfo.Item)
			if dataNode.Header.HashMod > 0 {
				h = hashKey(key, dataNode.Header.HashMod)
			}

			index := s.config.DataNodeAllocator.Index(h)

			if dataNode.States[index] == stateFree {
				return s.defaultValue, false
			}
			item := dataNode.Items[index]
			if item.Hash == h && item.Key == key {
				return item.Value, true
			}
			return s.defaultValue, false
		default:
			_, pointerNode := s.config.PointerNodeAllocator.Get(*pInfo.Item)
			if pointerNode.Header.HashMod > 0 {
				h = hashKey(key, pointerNode.Header.HashMod)
			}

			index := s.config.PointerNodeAllocator.Index(h)
			h = s.config.PointerNodeAllocator.Shift(h)

			pInfo = ParentInfo{
				State: &pointerNode.States[index],
				Item:  &pointerNode.Items[index],
			}
		}
	}
}

// Set sets the value for the key.
func (s *Space[K, V]) Set(key K, value V) {
	s.set(s.config.SpaceRoot, DataItem[K, V]{
		Hash:  hashKey(key, 0),
		Key:   key,
		Value: value,
	})
}

func (s *Space[K, V]) set(pInfo ParentInfo, item DataItem[K, V]) {
	for {
		switch *pInfo.State {
		case stateFree:
			dataNodeIndex, dataNode := s.config.DataNodeAllocator.Allocate()
			dataNode.Header.SnapshotID = s.config.SnapshotID

			*pInfo.State = stateData
			*pInfo.Item = dataNodeIndex

			index := s.config.DataNodeAllocator.Index(item.Hash)
			dataNode.States[index] = stateData
			dataNode.Items[index] = item

			return
		case stateData:
			dataNodeData, dataNode := s.config.DataNodeAllocator.Get(*pInfo.Item)
			if dataNode.Header.SnapshotID < s.config.SnapshotID {
				newNodeIndex, newNode := s.config.DataNodeAllocator.Copy(dataNodeData)
				dataNode = newNode
				dataNode.Header.SnapshotID = s.config.SnapshotID
				*pInfo.Item = newNodeIndex
			}
			if dataNode.Header.HashMod > 0 {
				item.Hash = hashKey(item.Key, dataNode.Header.HashMod)
			}

			index := s.config.DataNodeAllocator.Index(item.Hash)
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
				*s.config.HashMod++
				dataNode.Header.HashMod = *s.config.HashMod
			}

			s.redistributeNode(pInfo)
			s.set(pInfo, item)

			return
		default:
			pointerNodeData, pointerNode := s.config.PointerNodeAllocator.Get(*pInfo.Item)
			if pointerNode.Header.SnapshotID < s.config.SnapshotID {
				newNodeIndex, newNode := s.config.PointerNodeAllocator.Copy(pointerNodeData)
				pointerNode = newNode
				pointerNode.Header.SnapshotID = s.config.SnapshotID
				*pInfo.Item = newNodeIndex
			}
			if pointerNode.Header.HashMod > 0 {
				item.Hash = hashKey(item.Key, pointerNode.Header.HashMod)
			}

			index := s.config.PointerNodeAllocator.Index(item.Hash)
			item.Hash = s.config.PointerNodeAllocator.Shift(item.Hash)

			pInfo = ParentInfo{
				State: &pointerNode.States[index],
				Item:  &pointerNode.Items[index],
			}
		}
	}
}

func (s *Space[K, V]) redistributeNode(pInfo ParentInfo) {
	_, dataNode := s.config.DataNodeAllocator.Get(*pInfo.Item)

	pointerNodeIndex, pointerNode := s.config.PointerNodeAllocator.Allocate()
	*pointerNode.Header = NodeHeader{
		SnapshotID: s.config.SnapshotID,
		HashMod:    dataNode.Header.HashMod,
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
