package space

import (
	"sort"

	"github.com/cespare/xxhash"

	"github.com/outofforest/photon"
	"github.com/outofforest/quantum/types"
)

const trials = 100

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
		config: config,
	}
}

// Space represents the substate where values V are stored by key K.
type Space[K, V comparable] struct {
	config Config[K, V]
}

// Get gets the value of the key.
func (s *Space[K, V]) Get(key K) Entry[K, V] {
	// For get, err is always nil.
	v, _ := s.find([]types.ParentInfo{s.config.SpaceRoot}, hashKey(key, 0), key)
	return v
}

// Iterator returns iterator iterating over items in space.
// FIXME (wojciech): Iterator should return Value[K, V] objects.
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
			case types.StateData:
				dataNode := s.config.DataNodeAllocator.Get(pInfo.Pointer.Address)
				for i, state := range dataNode.States {
					if state != types.StateData {
						continue
					}
					if !yield(dataNode.Items[i]) {
						return
					}
				}
			case types.StatePointer:
				pointerNode := s.config.PointerNodeAllocator.Get(pInfo.Pointer.Address)
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
	switch *s.config.SpaceRoot.State {
	case types.StateFree:
		return nil
	case types.StateData:
		return s.config.Allocator.Deallocate(s.config.SpaceRoot.Pointer.Address, s.config.SpaceRoot.Pointer.SnapshotID)
	}

	// FIXME (wojciech): Optimize heap allocations
	stack := []types.Pointer{*s.config.SpaceRoot.Pointer}

	for {
		if len(stack) == 0 {
			return nil
		}

		pointer := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		pointerNode := s.config.PointerNodeAllocator.Get(pointer.Address)
		for i, p := range pointerNode.Items {
			switch pointerNode.States[i] {
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

		pointerNode := s.config.PointerNodeAllocator.Get(n)
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

// Stats returns stats about the space.
func (s *Space[K, V]) Stats() (uint64, uint64, uint64) {
	switch *s.config.SpaceRoot.State {
	case types.StateFree:
		return 0, 0, 0
	case types.StateData:
		return 1, 0, 1
	}

	stack := []types.NodeAddress{s.config.SpaceRoot.Pointer.Address}

	levels := map[types.NodeAddress]uint64{
		s.config.SpaceRoot.Pointer.Address: 1,
	}
	var maxLevel, pointerNodes, dataNodes uint64

	for {
		if len(stack) == 0 {
			return maxLevel, pointerNodes, dataNodes
		}

		n := stack[len(stack)-1]
		level := levels[n] + 1
		pointerNodes++
		stack = stack[:len(stack)-1]

		pointerNode := s.config.PointerNodeAllocator.Get(n)
		for i, state := range pointerNode.States {
			switch state {
			case types.StateFree:
			case types.StateData:
				dataNodes++
				if level > maxLevel {
					maxLevel = level
				}
			case types.StatePointer:
				stack = append(stack, pointerNode.Items[i].Address)
				levels[pointerNode.Items[i].Address] = level
			}
		}
	}
}

func (s *Space[K, V]) deleteValue(v Entry[K, V]) error {
	pInfo := v.parentInfos[len(v.parentInfos)-1]
	if *pInfo.State == types.StatePointer {
		v, _ = s.find(v.parentInfos, v.item.Hash, v.item.Key)
		pInfo = v.parentInfos[len(v.parentInfos)-1]
	}

	if *pInfo.State == types.StateFree {
		return nil
	}

	if v.stateP == nil || *v.stateP <= types.StateDeleted {
		return nil
	}
	if v.itemP.Hash == v.item.Hash && v.itemP.Key == v.item.Key {
		if err := s.copy(v.parentInfos); err != nil {
			return err
		}
		*v.stateP = types.StateDeleted
		return nil
	}
	return nil
}

func (s *Space[K, V]) setValue(v Entry[K, V], value V) (Entry[K, V], error) {
	pInfo := v.parentInfos[len(v.parentInfos)-1]
	if *pInfo.State == types.StatePointer {
		v, _ = s.find(v.parentInfos, v.item.Hash, v.item.Key)
		pInfo = v.parentInfos[len(v.parentInfos)-1]
	}

	v.item.Value = value

	if err := s.copy(v.parentInfos); err != nil {
		return Entry[K, V]{}, err
	}

	if *pInfo.State == types.StateData && v.stateP != nil {
		if *v.stateP <= types.StateDeleted {
			*v.itemP = v.item
			*v.stateP = types.StateData
			v.exists = true
			return v, nil
		}
		if v.itemP.Hash == v.item.Hash && v.itemP.Key == v.item.Key {
			v.itemP.Value = value
			return v, nil
		}
	}

	var newPInfos []types.ParentInfo
	var err error
	v.itemP, v.stateP, newPInfos, err = s.set(pInfo, v.item)
	if err != nil {
		return Entry[K, V]{}, err
	}

	v.item.Hash = v.itemP.Hash
	// FIXME (wojciech): Optimize heap allocations
	v.parentInfos = append(v.parentInfos, newPInfos...)
	v.exists = true

	return v, nil
}

func (s *Space[K, V]) set(
	pInfo types.ParentInfo,
	item types.DataItem[K, V],
) (*types.DataItem[K, V], *types.State, []types.ParentInfo, error) {
	var pInfos []types.ParentInfo
	for {
		switch *pInfo.State {
		case types.StateFree:
			dataNodeAddress, dataNode, err := s.config.DataNodeAllocator.Allocate(s.config.Allocator)
			if err != nil {
				return nil, nil, nil, err
			}

			*pInfo.State = types.StateData
			pInfo.Pointer.SnapshotID = s.config.SnapshotID
			pInfo.Pointer.Address = dataNodeAddress

			index := s.config.DataNodeAllocator.Index(item.Hash + 1)
			dataNode.States[index] = types.StateData
			dataNode.Items[index] = item

			return &dataNode.Items[index], &dataNode.States[index], pInfos, nil
		case types.StateData:
			dataNode := s.config.DataNodeAllocator.Get(pInfo.Pointer.Address)
			// FIXME (wojciech): This is redundant for the first data node (before redistribution)
			if dataNode.Header.HashMod > 0 {
				item.Hash = hashKey(item.Key, dataNode.Header.HashMod)
			}
			var index uint64
			var keyMatches, conflict bool
			for i := types.Hash(0); i < trials; i++ {
				index = s.config.DataNodeAllocator.Index(item.Hash + 1<<i + i)
				hashMatches := item.Hash == dataNode.Items[index].Hash
				keyMatches = hashMatches && item.Key == dataNode.Items[index].Key
				conflict = conflict || hashMatches

				if dataNode.States[index] == types.StateFree || dataNode.States[index] == types.StateDeleted ||
					keyMatches {
					break
				}
			}
			if dataNode.States[index] == types.StateFree || dataNode.States[index] == types.StateDeleted {
				dataNode.States[index] = types.StateData
				dataNode.Items[index] = item

				return &dataNode.Items[index], &dataNode.States[index], pInfos, nil
			}

			if keyMatches {
				dataNode.Items[index] = item
				return &dataNode.Items[index], &dataNode.States[index], pInfos, nil
			}

			hashMod := dataNode.Header.HashMod
			if conflict {
				// hash conflict
				*s.config.HashMod++
				hashMod = *s.config.HashMod
			}

			if err := s.redistributeNode(pInfo, hashMod); err != nil {
				return nil, nil, nil, err
			}
			return s.set(pInfo, item)
		default:
			pointerNode := s.config.PointerNodeAllocator.Get(pInfo.Pointer.Address)
			if pointerNode.Header.HashMod > 0 {
				item.Hash = hashKey(item.Key, pointerNode.Header.HashMod)
			}

			index := s.config.PointerNodeAllocator.Index(item.Hash)
			item.Hash = s.config.PointerNodeAllocator.Shift(item.Hash)

			pInfo = types.ParentInfo{
				State:   &pointerNode.States[index],
				Pointer: &pointerNode.Items[index],
			}
			pInfos = append(pInfos, pInfo)
		}
	}
}

func (s *Space[K, V]) redistributeNode(pInfo types.ParentInfo, hashMod uint64) error {
	dataNodePointer := *pInfo.Pointer
	dataNode := s.config.DataNodeAllocator.Get(dataNodePointer.Address)

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

		item := dataNode.Items[i]
		if pointerNode.Header.HashMod > 0 {
			item.Hash = hashKey(item.Key, pointerNode.Header.HashMod)
		}
		index := s.config.PointerNodeAllocator.Index(item.Hash)
		item.Hash = s.config.PointerNodeAllocator.Shift(item.Hash)

		newPInfo := types.ParentInfo{
			State:   &pointerNode.States[index],
			Pointer: &pointerNode.Items[index],
		}

		if _, _, _, err := s.set(newPInfo, item); err != nil {
			return err
		}
	}

	return s.config.Allocator.Deallocate(dataNodePointer.Address, dataNodePointer.SnapshotID)
}

func (s *Space[K, V]) find(pInfos []types.ParentInfo, hash types.Hash, key K) (Entry[K, V], error) {
	v := Entry[K, V]{
		space: s,
		item: types.DataItem[K, V]{
			Hash: hash,
			Key:  key,
		},
		// FIXME (wojciech): Optimize heap allocations
		parentInfos: pInfos,
	}

	pInfo := pInfos[len(pInfos)-1]

	for {
		switch *pInfo.State {
		case types.StatePointer:
			pointerNode := s.config.PointerNodeAllocator.Get(pInfo.Pointer.Address)
			if pointerNode.Header.HashMod != 0 {
				v.item.Hash = hashKey(key, pointerNode.Header.HashMod)
			}

			index := s.config.PointerNodeAllocator.Index(v.item.Hash)
			v.item.Hash = s.config.PointerNodeAllocator.Shift(v.item.Hash)

			pInfo = types.ParentInfo{
				State:   &pointerNode.States[index],
				Pointer: &pointerNode.Items[index],
			}
			v.parentInfos = append(v.parentInfos, pInfo)
		case types.StateData:
			dataNode := s.config.DataNodeAllocator.Get(pInfo.Pointer.Address)
			if dataNode.Header.HashMod != 0 {
				v.item.Hash = hashKey(key, dataNode.Header.HashMod)
			}

			for i := types.Hash(0); i < trials; i++ {
				index := s.config.DataNodeAllocator.Index(v.item.Hash + 1<<i + i)

				switch dataNode.States[index] {
				case types.StateFree:
					if v.stateP == nil {
						v.stateP = &dataNode.States[index]
						v.itemP = &dataNode.Items[index]
					}
					return v, nil
				case types.StateDeleted:
					if v.stateP == nil {
						v.stateP = &dataNode.States[index]
						v.itemP = &dataNode.Items[index]
					}
					continue
				}

				item := dataNode.Items[index]
				if item.Hash == v.item.Hash && item.Key == key {
					v.exists = true
					v.stateP = &dataNode.States[index]
					v.itemP = &dataNode.Items[index]
					v.item.Value = item.Value
					return v, nil
				}
			}
			return v, nil
		default:
			return v, nil
		}
	}
}

func (s *Space[K, V]) copy(pInfos []types.ParentInfo) error {
	for i := len(pInfos) - 1; i >= 0; i-- {
		pInfo := pInfos[i]
		if *pInfo.State == types.StateFree {
			continue
		}
		if pInfo.Pointer.SnapshotID == s.config.SnapshotID {
			break
		}

		// FIXME (wojciech): Don't copy data node if it requires redistribution

		newNodeAddress, _, err := s.config.Allocator.Copy(pInfo.Pointer.Address)
		if err != nil {
			return err
		}

		// FIXME (wojciech): No-copy test
		// if err := s.config.Allocator.Deallocate(pInfo.Pointer.Address, pInfo.Pointer.SnapshotID); err != nil {
		//	return err
		// }

		pInfo.Pointer.SnapshotID = s.config.SnapshotID
		pInfo.Pointer.Address = newNodeAddress
	}

	return nil
}

// Entry represents entry in the space.
type Entry[K, V comparable] struct {
	space       *Space[K, V]
	item        types.DataItem[K, V]
	itemP       *types.DataItem[K, V]
	stateP      *types.State
	exists      bool
	parentInfos []types.ParentInfo
}

// Value returns the value from entry.
func (v Entry[K, V]) Value() V {
	return v.item.Value
}

// Key returns the key from entry.
func (v Entry[K, V]) Key() K {
	return v.item.Key
}

// Exists returns true if entry exists in the space.
func (v Entry[K, V]) Exists() bool {
	return v.exists
}

// Set sts value for entry.
func (v Entry[K, V]) Set(value V) (Entry[K, V], error) {
	return v.space.setValue(v, value)
}

// Delete deletes the entry.
func (v Entry[K, V]) Delete() error {
	return v.space.deleteValue(v)
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
		copy(b[types.UInt64Length:], p.B)
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
