package space

import (
	"sort"

	"github.com/cespare/xxhash"
	"github.com/pkg/errors"

	"github.com/outofforest/photon"
	"github.com/outofforest/quantum/types"
)

const trials = 50

// Config stores space configuration.
type Config[K, V comparable] struct {
	HashMod              *uint64
	SpaceRoot            types.ParentInfo
	PointerNodeAllocator *NodeAllocator[types.Pointer]
	DataNodeAllocator    *NodeAllocator[types.DataItem[K, V]]
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
	v := Entry[K, V]{
		space: s,
		item: types.DataItem[K, V]{
			Hash: hashKey(key, 0),
			Key:  key,
		},
		pInfo: s.config.SpaceRoot,
	}

	// For get, err is always nil.
	v, _ = s.find(v)
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

// AllocatePointers allocates specified levels of pointer nodes.
func (s *Space[K, V]) AllocatePointers(levels uint64) error {
	if *s.config.SpaceRoot.State != types.StateFree {
		return errors.New("pointers can be preallocated only on empty space")
	}
	if levels == 0 {
		return nil
	}

	stack := map[types.ParentInfo]uint64{
		s.config.SpaceRoot: 1,
	}

	for {
		if len(stack) == 0 {
			return nil
		}
		for pInfo, level := range stack {
			delete(stack, pInfo)

			pointerNodeAddress, pointerNode, err := s.config.PointerNodeAllocator.Allocate(s.config.Allocator)
			if err != nil {
				return err
			}
			*pInfo.State = types.StatePointer
			*pInfo.Pointer = types.Pointer{
				SnapshotID: s.config.Allocator.SnapshotID(),
				Address:    pointerNodeAddress,
			}

			if level == levels {
				continue
			}

			level++
			for i := range pointerNode.States {
				stack[types.ParentInfo{
					State:   &pointerNode.States[i],
					Pointer: &pointerNode.Items[i],
				}] = level
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
	if *v.pInfo.State == types.StatePointer {
		v, _ = s.find(v)
	}

	if *v.pInfo.State == types.StateFree {
		return nil
	}

	if v.stateP == nil || *v.stateP <= types.StateDeleted {
		return nil
	}
	if v.itemP.Hash == v.item.Hash && v.itemP.Key == v.item.Key {
		*v.stateP = types.StateDeleted
		return nil
	}
	return nil
}

func (s *Space[K, V]) setValue(v Entry[K, V], value V) (Entry[K, V], error) {
	if *v.pInfo.State == types.StatePointer {
		v, _ = s.find(v)
	}

	v.item.Value = value

	if *v.pInfo.State == types.StateData && v.stateP != nil {
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

	return s.set(v)
}

func (s *Space[K, V]) set(v Entry[K, V]) (Entry[K, V], error) {
	for {
		switch *v.pInfo.State {
		case types.StateFree:
			dataNodeAddress, dataNode, err := s.config.DataNodeAllocator.Allocate(s.config.Allocator)
			if err != nil {
				return Entry[K, V]{}, err
			}

			*v.pInfo.State = types.StateData
			v.pInfo.Pointer.SnapshotID = s.config.Allocator.SnapshotID()
			v.pInfo.Pointer.Address = dataNodeAddress

			index := s.config.DataNodeAllocator.Index(v.item.Hash + 1)
			dataNode.States[index] = types.StateData
			dataNode.Items[index] = v.item

			v.stateP = &dataNode.States[index]
			v.itemP = &dataNode.Items[index]
			v.exists = true

			return v, nil
		case types.StateData:
			dataNode := s.config.DataNodeAllocator.Get(v.pInfo.Pointer.Address)

			var index uint64
			var keyMatches, conflict bool
			for i := types.Hash(0); i < trials; i++ {
				index = s.config.DataNodeAllocator.Index(v.item.Hash + 1<<i + i)
				hashMatches := v.item.Hash == dataNode.Items[index].Hash
				keyMatches = hashMatches && v.item.Key == dataNode.Items[index].Key
				conflict = conflict || hashMatches

				if dataNode.States[index] == types.StateFree || dataNode.States[index] == types.StateDeleted ||
					keyMatches {
					break
				}
			}
			if dataNode.States[index] == types.StateFree || dataNode.States[index] == types.StateDeleted {
				dataNode.States[index] = types.StateData
				dataNode.Items[index] = v.item

				v.stateP = &dataNode.States[index]
				v.itemP = &dataNode.Items[index]
				v.exists = true

				return v, nil
			}

			if keyMatches {
				dataNode.Items[index] = v.item

				v.stateP = &dataNode.States[index]
				v.itemP = &dataNode.Items[index]
				v.exists = true

				return v, nil
			}

			if err := s.redistributeNode(v.pInfo, conflict); err != nil {
				return Entry[K, V]{}, err
			}
			return s.set(v)
		default:
			pointerNode := s.config.PointerNodeAllocator.Get(v.pInfo.Pointer.Address)
			if pointerNode.Header.HashMod > 0 {
				v.item.Hash = hashKey(v.item.Key, pointerNode.Header.HashMod)
			}

			index := s.config.PointerNodeAllocator.Index(v.item.Hash)
			v.item.Hash = s.config.PointerNodeAllocator.Shift(v.item.Hash)

			v.pInfo = types.ParentInfo{
				State:   &pointerNode.States[index],
				Pointer: &pointerNode.Items[index],
			}
		}
	}
}

func (s *Space[K, V]) redistributeNode(pInfo types.ParentInfo, conflict bool) error {
	dataNodePointer := *pInfo.Pointer
	dataNode := s.config.DataNodeAllocator.Get(dataNodePointer.Address)

	pointerNodeAddress, pointerNode, err := s.config.PointerNodeAllocator.Allocate(s.config.Allocator)
	if err != nil {
		return err
	}

	if conflict {
		*s.config.HashMod++
		*pointerNode.Header = types.SpaceNodeHeader{
			HashMod: *s.config.HashMod,
		}
	}

	*pInfo.State = types.StatePointer
	*pInfo.Pointer = types.Pointer{
		SnapshotID: s.config.Allocator.SnapshotID(),
		Address:    pointerNodeAddress,
	}

	for i, state := range dataNode.States {
		if state != types.StateData {
			continue
		}

		item := dataNode.Items[i]
		if conflict {
			item.Hash = hashKey(item.Key, pointerNode.Header.HashMod)
		}
		index := s.config.PointerNodeAllocator.Index(item.Hash)
		item.Hash = s.config.PointerNodeAllocator.Shift(item.Hash)

		if _, err := s.set(Entry[K, V]{
			space: s,
			item:  item,
			pInfo: types.ParentInfo{
				State:   &pointerNode.States[index],
				Pointer: &pointerNode.Items[index],
			},
		}); err != nil {
			return err
		}
	}

	return s.config.Allocator.Deallocate(dataNodePointer.Address, dataNodePointer.SnapshotID)
}

func (s *Space[K, V]) find(v Entry[K, V]) (Entry[K, V], error) {
	for {
		switch *v.pInfo.State {
		case types.StatePointer:
			pointerNode := s.config.PointerNodeAllocator.Get(v.pInfo.Pointer.Address)
			if pointerNode.Header.HashMod != 0 {
				v.item.Hash = hashKey(v.item.Key, pointerNode.Header.HashMod)
			}

			index := s.config.PointerNodeAllocator.Index(v.item.Hash)
			v.item.Hash = s.config.PointerNodeAllocator.Shift(v.item.Hash)

			v.pInfo = types.ParentInfo{
				State:   &pointerNode.States[index],
				Pointer: &pointerNode.Items[index],
			}
		case types.StateData:
			dataNode := s.config.DataNodeAllocator.Get(v.pInfo.Pointer.Address)
			for i := types.Hash(0); i < trials; i++ {
				index := s.config.DataNodeAllocator.Index(v.item.Hash + 1<<i + i)

				switch dataNode.States[index] {
				case types.StateFree:
					if v.stateP == nil {
						v.stateP = &dataNode.States[index]
						v.itemP = &dataNode.Items[index]
					}
					return v, nil
				case types.StateData:
					item := dataNode.Items[index]
					if item.Hash == v.item.Hash && item.Key == v.item.Key {
						v.exists = true
						v.stateP = &dataNode.States[index]
						v.itemP = &dataNode.Items[index]
						v.item.Value = item.Value
						return v, nil
					}
				default:
					if v.stateP == nil {
						v.stateP = &dataNode.States[index]
						v.itemP = &dataNode.Items[index]
					}
				}
			}
			return v, nil
		default:
			return v, nil
		}
	}
}

// Entry represents entry in the space.
type Entry[K, V comparable] struct {
	space  *Space[K, V]
	item   types.DataItem[K, V]
	itemP  *types.DataItem[K, V]
	stateP *types.State
	exists bool
	pInfo  types.ParentInfo
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
