package space

import (
	"github.com/cespare/xxhash"

	"github.com/outofforest/photon"
	"github.com/outofforest/quantum/types"
)

// Config stores space configuration.
type Config[K, V comparable] struct {
	SnapshotID           types.SnapshotID
	HashMod              *uint64
	SpaceRoot            types.ParentInfo
	PointerNodeAllocator NodeAllocator[types.NodeAddress]
	DataNodeAllocator    NodeAllocator[types.DataItem[K, V]]
	Allocator            types.SnapshotAllocator
}

// New creates new space.
func New[K, V comparable](config Config[K, V]) *Space[K, V] {
	return &Space[K, V]{
		config:       config,
		defaultValue: *new(V),
	}
}

// Space represents the substate where values V are stored by key K.
type Space[K, V comparable] struct {
	config       Config[K, V]
	defaultValue V
}

// Get gets the value of the key.
func (s *Space[K, V]) Get(key K) (V, bool) {
	h := hashKey(key, 0)
	pInfo := s.config.SpaceRoot

	for {
		switch *pInfo.State {
		case types.StateFree:
			return s.defaultValue, false
		case types.StateData:
			_, dataNode := s.config.DataNodeAllocator.Get(*pInfo.Item)
			if dataNode.Header.HashMod > 0 {
				h = hashKey(key, dataNode.Header.HashMod)
			}

			index := s.config.DataNodeAllocator.Index(h)

			if dataNode.States[index] == types.StateFree {
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

			pInfo = types.ParentInfo{
				State: &pointerNode.States[index],
				Item:  &pointerNode.Items[index],
			}
		}
	}
}

// Set sets the value for the key.
func (s *Space[K, V]) Set(key K, value V) error {
	return s.set(s.config.SpaceRoot, types.DataItem[K, V]{
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
		case types.StateFree:
			return nil
		case types.StateData:
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
			if dataNode.States[index] == types.StateData && h == dataNode.Items[index].Hash &&
				key == dataNode.Items[index].Key {
				dataNode.States[index] = types.StateFree
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

			pInfo = types.ParentInfo{
				State: &pointerNode.States[index],
				Item:  &pointerNode.Items[index],
			}
		}
	}
}

// Iterator returns iterator iterating over items in space.
func (s *Space[K, V]) Iterator() func(func(types.DataItem[K, V]) bool) {
	return func(yield func(item types.DataItem[K, V]) bool) {
		// FIXME (wojciech): avoid heap allocations
		stack := []types.ParentInfo{s.config.SpaceRoot}

		for {
			if len(stack) == 0 {
				return
			}

			pInfo := stack[len(stack)-1]
			stack = stack[:len(stack)-1]

			switch *pInfo.State {
			case types.StateFree:
			case types.StateData:
				_, dataNode := s.config.DataNodeAllocator.Get(*pInfo.Item)
				for i, state := range dataNode.States {
					if state == types.StateFree {
						continue
					}
					if !yield(dataNode.Items[i]) {
						return
					}
				}
			default:
				_, pointerNode := s.config.PointerNodeAllocator.Get(*pInfo.Item)
				for i, state := range pointerNode.States {
					if state == types.StateFree {
						continue
					}
					stack = append(stack, types.ParentInfo{
						State: &pointerNode.States[i],
						Item:  &pointerNode.Items[i],
					})
				}
			}
		}
	}
}

func (s *Space[K, V]) set(pInfo types.ParentInfo, item types.DataItem[K, V]) error {
	for {
		switch *pInfo.State {
		case types.StateFree:
			dataNodeAddress, dataNode, err := s.config.DataNodeAllocator.Allocate(s.config.Allocator)
			if err != nil {
				return err
			}
			dataNode.Header.SnapshotID = s.config.SnapshotID

			*pInfo.State = types.StateData
			*pInfo.Item = dataNodeAddress

			index := s.config.DataNodeAllocator.Index(item.Hash)
			dataNode.States[index] = types.StateData
			dataNode.Items[index] = item

			return nil
		case types.StateData:
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
			if dataNode.States[index] == types.StateFree {
				dataNode.States[index] = types.StateData
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

			pInfo = types.ParentInfo{
				State: &pointerNode.States[index],
				Item:  &pointerNode.Items[index],
			}
		}
	}
}

func (s *Space[K, V]) redistributeNode(pInfo types.ParentInfo) error {
	dataNodeAddress := *pInfo.Item
	_, dataNode := s.config.DataNodeAllocator.Get(dataNodeAddress)

	pointerNodeAddress, pointerNode, err := s.config.PointerNodeAllocator.Allocate(s.config.Allocator)
	if err != nil {
		return err
	}
	*pointerNode.Header = types.SpaceNodeHeader{
		SnapshotID: s.config.SnapshotID,
		HashMod:    dataNode.Header.HashMod,
	}

	*pInfo.State = types.StatePointer
	*pInfo.Item = pointerNodeAddress

	for i, state := range dataNode.States {
		if state == types.StateFree {
			continue
		}

		if err := s.set(pInfo, dataNode.Items[i]); err != nil {
			return err
		}
	}

	return s.config.Allocator.Deallocate(dataNodeAddress, dataNode.Header.SnapshotID)
}

func hashKey[K comparable](key K, hashMod uint64) types.Hash {
	var hash types.Hash
	p := photon.NewFromValue[K](&key)
	if hashMod == 0 {
		hash = types.Hash(xxhash.Sum64(p.B))
	} else {
		// FIXME (wojciech): Remove heap allocation
		b := make([]byte, types.UInt64Length+len(p.B))
		copy(b, photon.NewFromValue(&hashMod).B)
		copy(b[types.UInt64Length:], photon.NewFromValue[K](&key).B)
		hash = types.Hash(xxhash.Sum64(b))
	}

	if types.IsTesting {
		hash = testHash(hash)
	}

	return hash
}

func testHash(hash types.Hash) types.Hash {
	return hash & 0x7fffffff
}
