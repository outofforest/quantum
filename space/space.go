package space

import (
	"sort"

	"github.com/cespare/xxhash"

	"github.com/outofforest/photon"
	"github.com/outofforest/quantum/types"
)

const trials = 10

// Config stores space configuration.
type Config[K, V comparable] struct {
	SnapshotID           types.SnapshotID
	HashMod              *uint64
	SpaceRoot            types.ParentInfo
	PointerNodeAllocator NodeAllocator[types.Pointer]
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
			_, dataNode := s.config.DataNodeAllocator.Get(pInfo.Pointer.Address)
			if dataNode.Header.HashMod > 0 {
				h = hashKey(key, dataNode.Header.HashMod)
			}

			for i := types.Hash(0); i < trials; i++ {
				index := s.config.DataNodeAllocator.Index(h + i)

				switch dataNode.States[index] {
				case types.StateFree:
					return s.defaultValue, false
				case types.StateDeleted:
					continue
				}

				item := dataNode.Items[index]
				if item.Hash == h && item.Key == key {
					return item.Value, true
				}
			}
			return s.defaultValue, false
		default:
			_, pointerNode := s.config.PointerNodeAllocator.Get(pInfo.Pointer.Address)
			if pointerNode.Header.HashMod > 0 {
				h = hashKey(key, pointerNode.Header.HashMod)
			}

			index := s.config.PointerNodeAllocator.Index(h)
			h = s.config.PointerNodeAllocator.Shift(h)

			pInfo = types.ParentInfo{
				State:   &pointerNode.States[index],
				Pointer: &pointerNode.Items[index],
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
			dataNodeData, dataNode := s.config.DataNodeAllocator.Get(pInfo.Pointer.Address)
			if pInfo.Pointer.SnapshotID < s.config.SnapshotID {
				newNodeAddress, newNode, err := s.config.DataNodeAllocator.Copy(s.config.Allocator, dataNodeData)
				if err != nil {
					return err
				}
				if err := s.config.Allocator.Deallocate(pInfo.Pointer.Address, pInfo.Pointer.SnapshotID); err != nil {
					return err
				}
				pInfo.Pointer.SnapshotID = s.config.SnapshotID
				pInfo.Pointer.Address = newNodeAddress
				dataNode = newNode
			}
			if dataNode.Header.HashMod > 0 {
				h = hashKey(key, dataNode.Header.HashMod)
			}

			for i := types.Hash(0); i < trials; i++ {
				index := s.config.DataNodeAllocator.Index(h + i)
				switch dataNode.States[index] {
				case types.StateFree:
					return nil
				case types.StateDeleted:
					continue
				}
				if h == dataNode.Items[index].Hash && key == dataNode.Items[index].Key {
					dataNode.States[index] = types.StateDeleted
					return nil
				}
			}
			return nil
		default:
			pointerNodeData, pointerNode := s.config.PointerNodeAllocator.Get(pInfo.Pointer.Address)
			if pInfo.Pointer.SnapshotID < s.config.SnapshotID {
				newNodeAddress, newNode, err := s.config.PointerNodeAllocator.Copy(s.config.Allocator, pointerNodeData)
				if err != nil {
					return err
				}
				if err := s.config.Allocator.Deallocate(pInfo.Pointer.Address, pInfo.Pointer.SnapshotID); err != nil {
					return err
				}
				pInfo.Pointer.SnapshotID = s.config.SnapshotID
				pInfo.Pointer.Address = newNodeAddress
				pointerNode = newNode
			}
			if pointerNode.Header.HashMod > 0 {
				h = hashKey(key, pointerNode.Header.HashMod)
			}

			index := s.config.PointerNodeAllocator.Index(h)
			h = s.config.PointerNodeAllocator.Shift(h)

			pInfo = types.ParentInfo{
				State:   &pointerNode.States[index],
				Pointer: &pointerNode.Items[index],
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
				_, dataNode := s.config.DataNodeAllocator.Get(pInfo.Pointer.Address)
				for i, state := range dataNode.States {
					if state != types.StateData {
						continue
					}
					if !yield(dataNode.Items[i]) {
						return
					}
				}
			default:
				_, pointerNode := s.config.PointerNodeAllocator.Get(pInfo.Pointer.Address)
				for i, state := range pointerNode.States {
					if state == types.StateFree {
						continue
					}
					stack = append(stack, types.ParentInfo{
						State:   &pointerNode.States[i],
						Pointer: &pointerNode.Items[i],
					})
				}
			}
		}
	}
}

// DeallocateAll deallocate all deallocates all nodes used by the space.
func (s *Space[K, V]) DeallocateAll() error {
	if *s.config.SpaceRoot.State == types.StateFree {
		return nil
	}

	// FIXME (wojciech): Optimize heap allocations
	stack := []types.Pointer{*s.config.SpaceRoot.Pointer}

	for {
		if len(stack) == 0 {
			return nil
		}

		pointer := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		_, pointerNode := s.config.PointerNodeAllocator.Get(pointer.Address)
		for i, p := range pointerNode.Items {
			switch pointerNode.States[i] {
			case types.StateFree:
			case types.StateData:
				if err := s.config.Allocator.Deallocate(p.Address, p.SnapshotID); err != nil {
					return err
				}
			case types.StatePointer:
				stack = append(stack, p)
			}
		}
		if err := s.config.Allocator.Deallocate(pointer.Address, pointer.SnapshotID); err != nil {
			return err
		}
	}
}

// Nodes returns list of nodes used by the space.
func (s *Space[K, V]) Nodes() []types.NodeAddress {
	switch *s.config.SpaceRoot.State {
	case types.StateFree:
		return nil
	case types.StateData:
		return []types.NodeAddress{s.config.SpaceRoot.Pointer.Address}
	case types.StatePointer:
	}

	nodes := []types.NodeAddress{}
	stack := []types.NodeAddress{s.config.SpaceRoot.Pointer.Address}

	for {
		if len(stack) == 0 {
			sort.Slice(nodes, func(i, j int) bool {
				return nodes[i] < nodes[j]
			})

			return nodes
		}

		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		nodes = append(nodes, n)

		_, pointerNode := s.config.PointerNodeAllocator.Get(n)
		for i, state := range pointerNode.States {
			switch state {
			case types.StateFree:
			case types.StateData:
				nodes = append(nodes, pointerNode.Items[i].Address)
			case types.StatePointer:
				stack = append(stack, pointerNode.Items[i].Address)
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

			*pInfo.State = types.StateData
			pInfo.Pointer.SnapshotID = s.config.SnapshotID
			pInfo.Pointer.Address = dataNodeAddress

			index := s.config.DataNodeAllocator.Index(item.Hash)
			dataNode.States[index] = types.StateData
			dataNode.Items[index] = item

			return nil
		case types.StateData:
			dataNodeData, dataNode := s.config.DataNodeAllocator.Get(pInfo.Pointer.Address)
			if dataNode.Header.HashMod > 0 {
				item.Hash = hashKey(item.Key, dataNode.Header.HashMod)
			}
			var index uint64
			var keyMatches, conflict bool
			for i := types.Hash(0); i < trials; i++ {
				index = s.config.DataNodeAllocator.Index(item.Hash + i)
				hashMatches := item.Hash == dataNode.Items[index].Hash
				keyMatches = hashMatches && item.Key == dataNode.Items[index].Key
				conflict = conflict || hashMatches
				if dataNode.States[index] == types.StateFree || dataNode.States[index] == types.StateDeleted ||
					keyMatches {
					if pInfo.Pointer.SnapshotID < s.config.SnapshotID {
						newNodeAddress, newNode, err := s.config.DataNodeAllocator.Copy(s.config.Allocator, dataNodeData)
						if err != nil {
							return err
						}
						if err := s.config.Allocator.Deallocate(pInfo.Pointer.Address, pInfo.Pointer.SnapshotID); err != nil {
							return err
						}
						pInfo.Pointer.SnapshotID = s.config.SnapshotID
						pInfo.Pointer.Address = newNodeAddress
						dataNode = newNode
					}

					break
				}
			}
			if dataNode.States[index] == types.StateFree || dataNode.States[index] == types.StateDeleted {
				dataNode.States[index] = types.StateData
				dataNode.Items[index] = item

				return nil
			}

			if keyMatches {
				dataNode.Items[index] = item
				return nil
			}

			hashMod := dataNode.Header.HashMod
			if conflict {
				// hash conflict
				*s.config.HashMod++
				hashMod = *s.config.HashMod
			}

			if err := s.redistributeNode(pInfo, hashMod); err != nil {
				return err
			}
			return s.set(pInfo, item)
		default:
			pointerNodeData, pointerNode := s.config.PointerNodeAllocator.Get(pInfo.Pointer.Address)
			if pInfo.Pointer.SnapshotID < s.config.SnapshotID {
				newNodeAddress, newNode, err := s.config.PointerNodeAllocator.Copy(s.config.Allocator, pointerNodeData)
				if err != nil {
					return err
				}
				if err := s.config.Allocator.Deallocate(pInfo.Pointer.Address, pInfo.Pointer.SnapshotID); err != nil {
					return err
				}
				pInfo.Pointer.SnapshotID = s.config.SnapshotID
				pInfo.Pointer.Address = newNodeAddress
				pointerNode = newNode
			}
			if pointerNode.Header.HashMod > 0 {
				item.Hash = hashKey(item.Key, pointerNode.Header.HashMod)
			}

			index := s.config.PointerNodeAllocator.Index(item.Hash)
			item.Hash = s.config.PointerNodeAllocator.Shift(item.Hash)

			pInfo = types.ParentInfo{
				State:   &pointerNode.States[index],
				Pointer: &pointerNode.Items[index],
			}
		}
	}
}

func (s *Space[K, V]) redistributeNode(pInfo types.ParentInfo, hashMod uint64) error {
	dataNodePointer := *pInfo.Pointer
	_, dataNode := s.config.DataNodeAllocator.Get(dataNodePointer.Address)

	pointerNodeAddress, pointerNode, err := s.config.PointerNodeAllocator.Allocate(s.config.Allocator)
	if err != nil {
		return err
	}
	*pointerNode.Header = types.SpaceNodeHeader{
		HashMod: hashMod,
	}

	*pInfo.State = types.StatePointer
	*pInfo.Pointer = types.Pointer{
		SnapshotID: s.config.SnapshotID,
		Address:    pointerNodeAddress,
	}

	for i, state := range dataNode.States {
		if state != types.StateData {
			continue
		}

		if err := s.set(pInfo, dataNode.Items[i]); err != nil {
			return err
		}
	}

	return s.config.Allocator.Deallocate(dataNodePointer.Address, dataNodePointer.SnapshotID)
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
