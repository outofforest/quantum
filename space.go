package quantum

import (
	"github.com/cespare/xxhash"

	"github.com/outofforest/photon"
)

// SpaceConfig stores space configuration.
type SpaceConfig[K, V comparable] struct {
	SnapshotID           SnapshotID
	HashMod              *uint64
	SpaceRoot            ParentInfo
	PointerNodeAllocator SpaceNodeAllocator[NodeAddress]
	DataNodeAllocator    SpaceNodeAllocator[DataItem[K, V]]
	Allocator            SnapshotAllocator
}

// NewSpace creates new space.
func NewSpace[K, V comparable](config SpaceConfig[K, V]) *Space[K, V] {
	return &Space[K, V]{
		config:       config,
		defaultValue: *new(V),
	}
}

// Space represents the substate where values V are stored by key K.
type Space[K, V comparable] struct {
	config       SpaceConfig[K, V]
	defaultValue V
}

// Get gets the value of the key.
func (s *Space[K, V]) Get(key K) (V, bool) {
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
func (s *Space[K, V]) Set(key K, value V) error {
	return s.set(s.config.SpaceRoot, DataItem[K, V]{
		Hash:  hashKey(key, 0),
		Key:   key,
		Value: value,
	})
}

// Delete deletes the key from space.
func (s *Space[K, V]) Delete(key K) error {
	h := hashKey(key, 0)
	pInfo := s.config.SpaceRoot

	for {
		switch *pInfo.State {
		case stateFree:
			return nil
		case stateData:
			// FIXME (wojciech): Don't copy the node if split is required.

			dataNodeData, dataNode := s.config.DataNodeAllocator.Get(*pInfo.Item)
			if dataNode.Header.SnapshotID < s.config.SnapshotID {
				newNodeAddress, newNode, err := s.config.DataNodeAllocator.Copy(s.config.Allocator, dataNodeData)
				if err != nil {
					return err
				}
				newNode.Header.SnapshotID = s.config.SnapshotID
				oldNodeAddress := *pInfo.Item
				*pInfo.Item = newNodeAddress
				if err := s.config.Allocator.Deallocate(oldNodeAddress, dataNode.Header.SnapshotID); err != nil {
					return err
				}
				dataNode = newNode
			}
			if dataNode.Header.HashMod > 0 {
				h = hashKey(key, dataNode.Header.HashMod)
			}

			index := s.config.DataNodeAllocator.Index(h)
			if dataNode.States[index] == stateData && h == dataNode.Items[index].Hash && key == dataNode.Items[index].Key {
				dataNode.States[index] = stateFree
			}

			return nil
		default:
			pointerNodeData, pointerNode := s.config.PointerNodeAllocator.Get(*pInfo.Item)
			if pointerNode.Header.SnapshotID < s.config.SnapshotID {
				newNodeAddress, newNode, err := s.config.PointerNodeAllocator.Copy(s.config.Allocator, pointerNodeData)
				if err != nil {
					return err
				}
				newNode.Header.SnapshotID = s.config.SnapshotID
				oldNodeAddress := *pInfo.Item
				*pInfo.Item = newNodeAddress
				if err := s.config.Allocator.Deallocate(oldNodeAddress, pointerNode.Header.SnapshotID); err != nil {
					return err
				}
				pointerNode = newNode
			}
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

func (s *Space[K, V]) set(pInfo ParentInfo, item DataItem[K, V]) error {
	for {
		switch *pInfo.State {
		case stateFree:
			dataNodeAddress, dataNode, err := s.config.DataNodeAllocator.Allocate(s.config.Allocator)
			if err != nil {
				return err
			}
			dataNode.Header.SnapshotID = s.config.SnapshotID

			*pInfo.State = stateData
			*pInfo.Item = dataNodeAddress

			index := s.config.DataNodeAllocator.Index(item.Hash)
			dataNode.States[index] = stateData
			dataNode.Items[index] = item

			return nil
		case stateData:
			// FIXME (wojciech): Don't copy the node if split is required.

			dataNodeData, dataNode := s.config.DataNodeAllocator.Get(*pInfo.Item)
			if dataNode.Header.SnapshotID < s.config.SnapshotID {
				newNodeAddress, newNode, err := s.config.DataNodeAllocator.Copy(s.config.Allocator, dataNodeData)
				if err != nil {
					return err
				}
				newNode.Header.SnapshotID = s.config.SnapshotID
				oldNodeAddress := *pInfo.Item
				*pInfo.Item = newNodeAddress
				if err := s.config.Allocator.Deallocate(oldNodeAddress, dataNode.Header.SnapshotID); err != nil {
					return err
				}
				dataNode = newNode
			}
			if dataNode.Header.HashMod > 0 {
				item.Hash = hashKey(item.Key, dataNode.Header.HashMod)
			}

			index := s.config.DataNodeAllocator.Index(item.Hash)
			if dataNode.States[index] == stateFree {
				dataNode.States[index] = stateData
				dataNode.Items[index] = item

				return nil
			}

			if item.Hash == dataNode.Items[index].Hash {
				if item.Key == dataNode.Items[index].Key {
					dataNode.Items[index] = item

					return nil
				}

				// hash conflict
				*s.config.HashMod++
				dataNode.Header.HashMod = *s.config.HashMod
			}

			if err := s.redistributeNode(pInfo); err != nil {
				return err
			}
			return s.set(pInfo, item)
		default:
			pointerNodeData, pointerNode := s.config.PointerNodeAllocator.Get(*pInfo.Item)
			if pointerNode.Header.SnapshotID < s.config.SnapshotID {
				newNodeAddress, newNode, err := s.config.PointerNodeAllocator.Copy(s.config.Allocator, pointerNodeData)
				if err != nil {
					return err
				}
				newNode.Header.SnapshotID = s.config.SnapshotID
				oldNodeAddress := *pInfo.Item
				*pInfo.Item = newNodeAddress
				if err := s.config.Allocator.Deallocate(oldNodeAddress, pointerNode.Header.SnapshotID); err != nil {
					return err
				}
				pointerNode = newNode
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

func (s *Space[K, V]) redistributeNode(pInfo ParentInfo) error {
	dataNodeAddress := *pInfo.Item
	_, dataNode := s.config.DataNodeAllocator.Get(dataNodeAddress)

	pointerNodeAddress, pointerNode, err := s.config.PointerNodeAllocator.Allocate(s.config.Allocator)
	if err != nil {
		return err
	}
	*pointerNode.Header = SpaceNodeHeader{
		SnapshotID: s.config.SnapshotID,
		HashMod:    dataNode.Header.HashMod,
	}

	*pInfo.State = statePointer
	*pInfo.Item = pointerNodeAddress

	for i, state := range dataNode.States {
		if state == stateFree {
			continue
		}

		if err := s.set(pInfo, dataNode.Items[i]); err != nil {
			return err
		}
	}

	return s.config.Allocator.Deallocate(dataNodeAddress, dataNode.Header.SnapshotID)
}

func hashKey[K comparable](key K, hashMod uint64) Hash {
	var hash Hash
	p := photon.NewFromValue[K](&key)
	if hashMod == 0 {
		hash = Hash(xxhash.Sum64(p.B))
	} else {
		// FIXME (wojciech): Remove heap allocation
		b := make([]byte, uint64Length+len(p.B))
		copy(b, photon.NewFromValue(&hashMod).B)
		copy(b[uint64Length:], photon.NewFromValue[K](&key).B)
		hash = Hash(xxhash.Sum64(b))
	}

	if isTesting {
		hash = testHash(hash)
	}

	return hash
}

func testHash(hash Hash) Hash {
	return hash & 0x7fffffff
}
