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
	HashMod           *uint64
	SpaceRoot         types.ParentInfo
	Allocator         types.Allocator
	SnapshotAllocator types.SnapshotAllocator
}

// New creates new space.
func New[K, V comparable](config Config[K, V]) (*Space[K, V], error) {
	pointerNodeAllocator, err := NewNodeAllocator[types.Pointer](config.Allocator)
	if err != nil {
		return nil, err
	}

	dataNodeAllocator, err := NewNodeAllocator[types.DataItem[K, V]](config.Allocator)
	if err != nil {
		return nil, err
	}

	return &Space[K, V]{
		config:               config,
		pointerNodeAllocator: pointerNodeAllocator,
		dataNodeAllocator:    dataNodeAllocator,
	}, nil
}

// Space represents the substate where values V are stored by key K.
type Space[K, V comparable] struct {
	config               Config[K, V]
	pointerNodeAllocator *NodeAllocator[types.Pointer]
	dataNodeAllocator    *NodeAllocator[types.DataItem[K, V]]
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
				dataNode := s.dataNodeAllocator.Get(pInfo.Pointer.Address)
				for item, state := range dataNode.Iterator() {
					if *state != types.StateData {
						continue
					}
					if !yield(*item) {
						return
					}
				}
			case types.StatePointer:
				pointerNode := s.pointerNodeAllocator.Get(pInfo.Pointer.Address)
				for item, state := range pointerNode.Iterator() {
					if *state == types.StateFree {
						continue
					}
					stack = append(stack, types.ParentInfo{
						State:   state,
						Pointer: item,
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

			pointerNodeAddress, pointerNode, err := s.pointerNodeAllocator.Allocate(s.config.SnapshotAllocator)
			if err != nil {
				return err
			}
			*pInfo.State = types.StatePointer
			*pInfo.Pointer = types.Pointer{
				SnapshotID: s.config.SnapshotAllocator.SnapshotID(),
				Address:    pointerNodeAddress,
			}

			if level == levels {
				continue
			}

			level++
			for item, state := range pointerNode.Iterator() {
				stack[types.ParentInfo{
					State:   state,
					Pointer: item,
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
		return s.config.SnapshotAllocator.Deallocate(s.config.SpaceRoot.Pointer.Address,
			s.config.SpaceRoot.Pointer.SnapshotID)
	}

	// FIXME (wojciech): Optimize heap allocations
	stack := []types.Pointer{*s.config.SpaceRoot.Pointer}

	for {
		if len(stack) == 0 {
			return nil
		}

		pointer := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		pointerNode := s.pointerNodeAllocator.Get(pointer.Address)
		for item, state := range pointerNode.Iterator() {
			switch *state {
			case types.StateData:
				if err := s.config.SnapshotAllocator.Deallocate(item.Address, item.SnapshotID); err != nil {
					return err
				}
			case types.StatePointer:
				stack = append(stack, *item)
			}
		}
		if err := s.config.SnapshotAllocator.Deallocate(pointer.Address, pointer.SnapshotID); err != nil {
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

		pointerNode := s.pointerNodeAllocator.Get(n)
		for item, state := range pointerNode.Iterator() {
			switch *state {
			case types.StateFree:
			case types.StateData:
				nodes = append(nodes, item.Address)
			case types.StatePointer:
				stack = append(stack, item.Address)
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

		pointerNode := s.pointerNodeAllocator.Get(n)
		for item, state := range pointerNode.Iterator() {
			switch *state {
			case types.StateFree:
			case types.StateData:
				dataNodes++
				if level > maxLevel {
					maxLevel = level
				}
			case types.StatePointer:
				stack = append(stack, item.Address)
				levels[item.Address] = level
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
			dataNodeAddress, dataNode, err := s.dataNodeAllocator.Allocate(s.config.SnapshotAllocator)
			if err != nil {
				return Entry[K, V]{}, err
			}

			*v.pInfo.State = types.StateData
			v.pInfo.Pointer.SnapshotID = s.config.SnapshotAllocator.SnapshotID()
			v.pInfo.Pointer.Address = dataNodeAddress

			item, state := dataNode.ItemByHash(v.item.Hash + 1)
			*state = types.StateData
			*item = v.item

			v.stateP = state
			v.itemP = item
			v.exists = true

			return v, nil
		case types.StateData:
			dataNode := s.dataNodeAllocator.Get(v.pInfo.Pointer.Address)

			var stateMatches, keyMatches, conflict bool
			var item *types.DataItem[K, V]
			var state *types.State
			for i := types.Hash(0); i < trials; i++ {
				item, state = dataNode.ItemByHash(v.item.Hash + 1<<i + i)
				hashMatches := v.item.Hash == item.Hash
				keyMatches = hashMatches && v.item.Key == item.Key
				conflict = conflict || hashMatches
				stateMatches = *state == types.StateFree || *state == types.StateDeleted

				if stateMatches || keyMatches {
					break
				}
			}
			if stateMatches {
				*state = types.StateData
				*item = v.item

				v.stateP = state
				v.itemP = item
				v.exists = true

				return v, nil
			}

			if keyMatches {
				*item = v.item

				v.stateP = state
				v.itemP = item
				v.exists = true

				return v, nil
			}

			if err := s.redistributeNode(v.pInfo, conflict); err != nil {
				return Entry[K, V]{}, err
			}
			return s.set(v)
		default:
			pointerNode := s.pointerNodeAllocator.Get(v.pInfo.Pointer.Address)
			if pointerNode.Header.HashMod > 0 {
				v.item.Hash = hashKey(v.item.Key, pointerNode.Header.HashMod)
			}

			item, state := pointerNode.ItemByHash(v.item.Hash)
			v.item.Hash = s.pointerNodeAllocator.Shift(v.item.Hash)

			v.pInfo = types.ParentInfo{
				State:   state,
				Pointer: item,
			}
		}
	}
}

func (s *Space[K, V]) redistributeNode(pInfo types.ParentInfo, conflict bool) error {
	dataNodePointer := *pInfo.Pointer
	dataNode := s.dataNodeAllocator.Get(dataNodePointer.Address)

	pointerNodeAddress, pointerNode, err := s.pointerNodeAllocator.Allocate(s.config.SnapshotAllocator)
	if err != nil {
		return err
	}

	if conflict {
		*s.config.HashMod++
		*pointerNode.Header = NodeHeader{
			HashMod: *s.config.HashMod,
		}
	}

	*pInfo.State = types.StatePointer
	*pInfo.Pointer = types.Pointer{
		SnapshotID: s.config.SnapshotAllocator.SnapshotID(),
		Address:    pointerNodeAddress,
	}

	for item, state := range dataNode.Iterator() {
		if *state != types.StateData {
			continue
		}

		if conflict {
			item.Hash = hashKey(item.Key, pointerNode.Header.HashMod)
		}
		pointerItem, pointerState := pointerNode.ItemByHash(item.Hash)
		item.Hash = s.pointerNodeAllocator.Shift(item.Hash)

		if _, err := s.set(Entry[K, V]{
			space: s,
			item:  *item,
			pInfo: types.ParentInfo{
				State:   pointerState,
				Pointer: pointerItem,
			},
		}); err != nil {
			return err
		}
	}

	return s.config.SnapshotAllocator.Deallocate(dataNodePointer.Address, dataNodePointer.SnapshotID)
}

func (s *Space[K, V]) find(v Entry[K, V]) (Entry[K, V], error) {
	for {
		switch *v.pInfo.State {
		case types.StatePointer:
			pointerNode := s.pointerNodeAllocator.Get(v.pInfo.Pointer.Address)
			if pointerNode.Header.HashMod != 0 {
				v.item.Hash = hashKey(v.item.Key, pointerNode.Header.HashMod)
			}

			item, state := pointerNode.ItemByHash(v.item.Hash)
			v.item.Hash = s.pointerNodeAllocator.Shift(v.item.Hash)

			v.pInfo = types.ParentInfo{
				State:   state,
				Pointer: item,
			}
		case types.StateData:
			dataNode := s.dataNodeAllocator.Get(v.pInfo.Pointer.Address)
			for i := types.Hash(0); i < trials; i++ {
				item, state := dataNode.ItemByHash(v.item.Hash + 1<<i + i)

				switch *state {
				case types.StateFree:
					if v.stateP == nil {
						v.stateP = state
						v.itemP = item
					}
					return v, nil
				case types.StateData:
					if item.Hash == v.item.Hash && item.Key == v.item.Key {
						v.exists = true
						v.stateP = state
						v.itemP = item
						v.item.Value = item.Value
						return v, nil
					}
				default:
					if v.stateP == nil {
						v.stateP = state
						v.itemP = item
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
