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
	SpaceRoot         types.ParentEntry
	Allocator         types.Allocator
	SnapshotAllocator types.SnapshotAllocator
	DirtySpaceNodesCh chan<- types.DirtySpaceNode
}

// New creates new space.
func New[K, V comparable](config Config[K, V]) (*Space[K, V], error) {
	pointerNodeAllocator, err := NewNodeAllocator[PointerNodeHeader, types.SpacePointer](config.Allocator)
	if err != nil {
		return nil, err
	}

	dataNodeAllocator, err := NewNodeAllocator[DataNodeHeader, types.DataItem[K, V]](config.Allocator)
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
	pointerNodeAllocator *NodeAllocator[PointerNodeHeader, types.SpacePointer]
	dataNodeAllocator    *NodeAllocator[DataNodeHeader, types.DataItem[K, V]]
}

// Get gets the value of the key.
func (s *Space[K, V]) Get(key K) Entry[K, V] {
	v := Entry[K, V]{
		space: s,
		item: types.DataItem[K, V]{
			Hash: hashKey(key, 0),
			Key:  key,
		},
		pEntry: s.config.SpaceRoot,
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
		stack := []types.ParentEntry{s.config.SpaceRoot}

		for {
			if len(stack) == 0 {
				return
			}

			pEntry := stack[len(stack)-1]
			stack = stack[:len(stack)-1]

			switch *pEntry.State {
			case types.StateData:
				dataNode := s.dataNodeAllocator.Get(pEntry.SpacePointer.Pointer.LogicalAddress)
				for item, state := range dataNode.Iterator() {
					if *state != types.StateData {
						continue
					}
					if !yield(*item) {
						return
					}
				}
			case types.StatePointer:
				pointerNode := s.pointerNodeAllocator.Get(pEntry.SpacePointer.Pointer.LogicalAddress)
				for item, state := range pointerNode.Iterator() {
					if *state == types.StateFree {
						continue
					}
					stack = append(stack, types.ParentEntry{
						State:        state,
						SpacePointer: item,
					})
				}
			}
		}
	}
}

type pointerToAllocate struct {
	Level    uint64
	PEntry   types.ParentEntry
	PAddress types.LogicalAddress
}

// AllocatePointers allocates specified levels of pointer nodes.
func (s *Space[K, V]) AllocatePointers(levels uint64) error {
	if *s.config.SpaceRoot.State != types.StateFree {
		return errors.New("pointers can be preallocated only on empty space")
	}
	if levels == 0 {
		return nil
	}

	stack := []pointerToAllocate{
		{
			Level:  1,
			PEntry: s.config.SpaceRoot,
		},
	}

	for {
		if len(stack) == 0 {
			return nil
		}

		pToAllocate := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		pointerNodeAddress, pointerNode, err := s.pointerNodeAllocator.Allocate(s.config.SnapshotAllocator)
		if err != nil {
			return err
		}
		pointerNode.Header.ParentNodeAddress = pToAllocate.PAddress

		s.config.DirtySpaceNodesCh <- types.DirtySpaceNode{}

		*pToAllocate.PEntry.State = types.StatePointer
		*pToAllocate.PEntry.SpacePointer = types.SpacePointer{
			SnapshotID: s.config.SnapshotAllocator.SnapshotID(),
			Pointer: types.Pointer{
				LogicalAddress: pointerNodeAddress,
			},
		}

		if pToAllocate.Level == levels {
			continue
		}

		pToAllocate.Level++
		for item, state := range pointerNode.Iterator() {
			stack = append(stack, pointerToAllocate{
				Level: pToAllocate.Level,
				PEntry: types.ParentEntry{
					State:        state,
					SpacePointer: item,
				},
				PAddress: pointerNodeAddress,
			})
		}
	}
}

// DeallocateAll deallocate all deallocates all nodes used by the space.
func (s *Space[K, V]) DeallocateAll() error {
	switch *s.config.SpaceRoot.State {
	case types.StateFree:
		return nil
	case types.StateData:
		return s.config.SnapshotAllocator.Deallocate(s.config.SpaceRoot.SpacePointer.Pointer.LogicalAddress,
			s.config.SpaceRoot.SpacePointer.SnapshotID)
	}

	// FIXME (wojciech): Optimize heap allocations
	stack := []types.SpacePointer{*s.config.SpaceRoot.SpacePointer}

	for {
		if len(stack) == 0 {
			return nil
		}

		spacePointer := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		pointerNode := s.pointerNodeAllocator.Get(spacePointer.Pointer.LogicalAddress)
		for sPointer, state := range pointerNode.Iterator() {
			switch *state {
			case types.StateData:
				if err := s.config.SnapshotAllocator.Deallocate(sPointer.Pointer.LogicalAddress,
					sPointer.SnapshotID); err != nil {
					return err
				}
			case types.StatePointer:
				stack = append(stack, *sPointer)
			}
		}
		if err := s.config.SnapshotAllocator.Deallocate(spacePointer.Pointer.LogicalAddress,
			spacePointer.SnapshotID); err != nil {
			return err
		}
	}
}

// Nodes returns list of nodes used by the space.
func (s *Space[K, V]) Nodes() []types.LogicalAddress {
	switch *s.config.SpaceRoot.State {
	case types.StateFree:
		return nil
	case types.StateData:
		return []types.LogicalAddress{s.config.SpaceRoot.SpacePointer.Pointer.LogicalAddress}
	}

	nodes := []types.LogicalAddress{}
	stack := []types.LogicalAddress{s.config.SpaceRoot.SpacePointer.Pointer.LogicalAddress}

	for {
		if len(stack) == 0 {
			sort.Slice(nodes, func(i, j int) bool {
				return nodes[i] < nodes[j]
			})

			return nodes
		}

		pointerNodeAddress := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		nodes = append(nodes, pointerNodeAddress)

		pointerNode := s.pointerNodeAllocator.Get(pointerNodeAddress)
		for sPointer, state := range pointerNode.Iterator() {
			switch *state {
			case types.StateFree:
			case types.StateData:
				nodes = append(nodes, sPointer.Pointer.LogicalAddress)
			case types.StatePointer:
				stack = append(stack, sPointer.Pointer.LogicalAddress)
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

	stack := []types.LogicalAddress{s.config.SpaceRoot.SpacePointer.Pointer.LogicalAddress}

	levels := map[types.LogicalAddress]uint64{
		s.config.SpaceRoot.SpacePointer.Pointer.LogicalAddress: 1,
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
		for sPointer, state := range pointerNode.Iterator() {
			switch *state {
			case types.StateFree:
			case types.StateData:
				dataNodes++
				if level > maxLevel {
					maxLevel = level
				}
			case types.StatePointer:
				stack = append(stack, sPointer.Pointer.LogicalAddress)
				levels[sPointer.Pointer.LogicalAddress] = level
			}
		}
	}
}

func (s *Space[K, V]) deleteValue(v Entry[K, V]) error {
	if *v.pEntry.State == types.StatePointer {
		v, _ = s.find(v)
	}

	if *v.pEntry.State == types.StateFree {
		return nil
	}

	if v.stateP == nil || *v.stateP <= types.StateDeleted {
		return nil
	}
	if v.itemP.Hash == v.item.Hash && v.itemP.Key == v.item.Key {
		*v.stateP = types.StateDeleted
		s.config.DirtySpaceNodesCh <- types.DirtySpaceNode{}
		return nil
	}
	return nil
}

func (s *Space[K, V]) setValue(v Entry[K, V], value V) (Entry[K, V], error) {
	if *v.pEntry.State == types.StatePointer {
		v, _ = s.find(v)
	}

	v.item.Value = value

	if *v.pEntry.State == types.StateData && v.stateP != nil {
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
		switch *v.pEntry.State {
		case types.StateFree:
			dataNodeAddress, dataNode, err := s.dataNodeAllocator.Allocate(s.config.SnapshotAllocator)
			if err != nil {
				return Entry[K, V]{}, err
			}

			*v.pEntry.State = types.StateData
			v.pEntry.SpacePointer.SnapshotID = s.config.SnapshotAllocator.SnapshotID()
			v.pEntry.SpacePointer.Pointer.LogicalAddress = dataNodeAddress

			item, state := dataNode.ItemByHash(v.item.Hash + 1)
			*state = types.StateData
			*item = v.item

			v.stateP = state
			v.itemP = item
			v.exists = true

			s.config.DirtySpaceNodesCh <- types.DirtySpaceNode{}

			return v, nil
		case types.StateData:
			dataNode := s.dataNodeAllocator.Get(v.pEntry.SpacePointer.Pointer.LogicalAddress)

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

				s.config.DirtySpaceNodesCh <- types.DirtySpaceNode{}

				return v, nil
			}

			if keyMatches {
				*item = v.item

				v.stateP = state
				v.itemP = item
				v.exists = true

				s.config.DirtySpaceNodesCh <- types.DirtySpaceNode{}

				return v, nil
			}

			if err := s.redistributeNode(v.pEntry, v.pAddress, conflict); err != nil {
				return Entry[K, V]{}, err
			}
			return s.set(v)
		default:
			pointerNode := s.pointerNodeAllocator.Get(v.pEntry.SpacePointer.Pointer.LogicalAddress)
			if pointerNode.Header.HashMod > 0 {
				v.item.Hash = hashKey(v.item.Key, pointerNode.Header.HashMod)
			}

			item, state := pointerNode.ItemByHash(v.item.Hash)
			v.item.Hash = s.pointerNodeAllocator.Shift(v.item.Hash)

			v.pEntry = types.ParentEntry{
				State:        state,
				SpacePointer: item,
			}
			v.pAddress = v.pEntry.SpacePointer.Pointer.LogicalAddress
		}
	}
}

func (s *Space[K, V]) redistributeNode(pEntry types.ParentEntry, pAddress types.LogicalAddress, conflict bool) error {
	dataNodePointer := *pEntry.SpacePointer
	dataNode := s.dataNodeAllocator.Get(dataNodePointer.Pointer.LogicalAddress)

	pointerNodeAddress, pointerNode, err := s.pointerNodeAllocator.Allocate(s.config.SnapshotAllocator)
	if err != nil {
		return err
	}

	pointerNode.Header.ParentNodeAddress = pAddress
	if conflict {
		*s.config.HashMod++
		*pointerNode.Header = PointerNodeHeader{
			HashMod: *s.config.HashMod,
		}
	}

	*pEntry.State = types.StatePointer
	*pEntry.SpacePointer = types.SpacePointer{
		SnapshotID: s.config.SnapshotAllocator.SnapshotID(),
		Pointer: types.Pointer{
			LogicalAddress: pointerNodeAddress,
		},
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
			pEntry: types.ParentEntry{
				State:        pointerState,
				SpacePointer: pointerItem,
			},
			pAddress: pointerNodeAddress,
		}); err != nil {
			return err
		}
	}

	return s.config.SnapshotAllocator.Deallocate(dataNodePointer.Pointer.LogicalAddress, dataNodePointer.SnapshotID)
}

func (s *Space[K, V]) find(v Entry[K, V]) (Entry[K, V], error) {
	for {
		switch *v.pEntry.State {
		case types.StatePointer:
			pointerNode := s.pointerNodeAllocator.Get(v.pEntry.SpacePointer.Pointer.LogicalAddress)
			if pointerNode.Header.HashMod != 0 {
				v.item.Hash = hashKey(v.item.Key, pointerNode.Header.HashMod)
			}

			item, state := pointerNode.ItemByHash(v.item.Hash)
			v.item.Hash = s.pointerNodeAllocator.Shift(v.item.Hash)

			v.pEntry = types.ParentEntry{
				State:        state,
				SpacePointer: item,
			}
			v.pAddress = v.pEntry.SpacePointer.Pointer.LogicalAddress
		case types.StateData:
			dataNode := s.dataNodeAllocator.Get(v.pEntry.SpacePointer.Pointer.LogicalAddress)
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
	space    *Space[K, V]
	item     types.DataItem[K, V]
	itemP    *types.DataItem[K, V]
	stateP   *types.State
	exists   bool
	pEntry   types.ParentEntry
	pAddress types.LogicalAddress
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
