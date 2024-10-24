package space

import (
	"sort"

	"github.com/cespare/xxhash"
	"github.com/pkg/errors"

	"github.com/outofforest/mass"
	"github.com/outofforest/photon"
	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/types"
)

const trials = 50

// Config stores space configuration.
type Config[K, V comparable] struct {
	HashMod               *uint64
	SpaceRoot             types.ParentEntry
	State                 *alloc.State
	PointerNodeAllocator  *NodeAllocator[PointerNodeHeader, types.Pointer]
	DataNodeAllocator     *NodeAllocator[DataNodeHeader, types.DataItem[K, V]]
	MassEntry             *mass.Mass[Entry[K, V]]
	EventCh               chan<- any
	ImmediateDeallocation bool
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

// NewPointerNode creates new pointer node representation.
func (s *Space[K, V]) NewPointerNode() *Node[PointerNodeHeader, types.Pointer] {
	return s.config.PointerNodeAllocator.NewNode()
}

// NewDataNode creates new data node representation.
func (s *Space[K, V]) NewDataNode() *Node[DataNodeHeader, types.DataItem[K, V]] {
	return s.config.DataNodeAllocator.NewNode()
}

// Get gets the value of the key.
func (s *Space[K, V]) Get(
	key K,
	pointerNode *Node[PointerNodeHeader, types.Pointer],
	dataNode *Node[DataNodeHeader, types.DataItem[K, V]],
) *Entry[K, V] {
	v := s.config.MassEntry.New()
	v.space = s
	v.item = types.DataItem[K, V]{
		Hash: hashKey(key, 0),
		Key:  key,
	}
	v.pEntry = s.config.SpaceRoot

	// For get, err is always nil.
	_ = s.find(v, pointerNode, dataNode)
	return v
}

// Iterator returns iterator iterating over items in space.
// FIXME (wojciech): Iterator should return Value[K, V] objects.
func (s *Space[K, V]) Iterator(
	pointerNode *Node[PointerNodeHeader, types.Pointer],
	dataNode *Node[DataNodeHeader, types.DataItem[K, V]],
) func(func(types.DataItem[K, V]) bool) {
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
				s.config.DataNodeAllocator.Get(pEntry.Pointer.LogicalAddress, dataNode)
				for item, state := range dataNode.Iterator() {
					if *state != types.StateData {
						continue
					}
					if !yield(*item) {
						return
					}
				}
			case types.StatePointer:
				s.config.PointerNodeAllocator.Get(pEntry.Pointer.LogicalAddress, pointerNode)
				for item, state := range pointerNode.Iterator() {
					if *state == types.StateFree {
						continue
					}
					stack = append(stack, types.ParentEntry{
						State:   state,
						Pointer: item,
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
	PIndex   uintptr
}

// AllocatePointers allocates specified levels of pointer nodes.
func (s *Space[K, V]) AllocatePointers(
	levels uint64,
	pool *alloc.Pool[types.LogicalAddress],
	pointerNode *Node[PointerNodeHeader, types.Pointer],
) error {
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

		pointerNodeAddress, err := s.config.PointerNodeAllocator.Allocate(pool, pointerNode)
		if err != nil {
			return err
		}
		pointerNode.Header.ParentNodeAddress = pToAllocate.PAddress
		pointerNode.Header.ParentNodeIndex = pToAllocate.PIndex

		*pToAllocate.PEntry.State = types.StatePointer
		pToAllocate.PEntry.Pointer.LogicalAddress = pointerNodeAddress

		if pToAllocate.Level == levels {
			s.config.EventCh <- types.SpacePointerNodeAllocatedEvent{
				NodeAddress:           pointerNodeAddress,
				RootPointer:           s.config.SpaceRoot.Pointer,
				ImmediateDeallocation: s.config.ImmediateDeallocation,
			}

			continue
		}

		pToAllocate.Level++
		var index uintptr
		for item, state := range pointerNode.Iterator() {
			stack = append(stack, pointerToAllocate{
				Level: pToAllocate.Level,
				PEntry: types.ParentEntry{
					State:   state,
					Pointer: item,
				},
				PAddress: pointerNodeAddress,
				PIndex:   index,
			})

			index++
		}
	}
}

// Nodes returns list of nodes used by the space.
func (s *Space[K, V]) Nodes(pointerNode *Node[PointerNodeHeader, types.Pointer]) []types.LogicalAddress {
	switch *s.config.SpaceRoot.State {
	case types.StateFree:
		return nil
	case types.StateData:
		return []types.LogicalAddress{s.config.SpaceRoot.Pointer.LogicalAddress}
	}

	nodes := []types.LogicalAddress{}
	stack := []types.LogicalAddress{s.config.SpaceRoot.Pointer.LogicalAddress}

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

		s.config.PointerNodeAllocator.Get(pointerNodeAddress, pointerNode)
		for pointer, state := range pointerNode.Iterator() {
			switch *state {
			case types.StateFree:
			case types.StateData:
				nodes = append(nodes, pointer.LogicalAddress)
			case types.StatePointer:
				stack = append(stack, pointer.LogicalAddress)
			}
		}
	}
}

// Stats returns stats about the space.
func (s *Space[K, V]) Stats(pointerNode *Node[PointerNodeHeader, types.Pointer]) (uint64, uint64, uint64) {
	switch *s.config.SpaceRoot.State {
	case types.StateFree:
		return 0, 0, 0
	case types.StateData:
		return 1, 0, 1
	}

	stack := []types.LogicalAddress{s.config.SpaceRoot.Pointer.LogicalAddress}

	levels := map[types.LogicalAddress]uint64{
		s.config.SpaceRoot.Pointer.LogicalAddress: 1,
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

		s.config.PointerNodeAllocator.Get(n, pointerNode)
		for pointer, state := range pointerNode.Iterator() {
			switch *state {
			case types.StateFree:
			case types.StateData:
				dataNodes++
				if level > maxLevel {
					maxLevel = level
				}
			case types.StatePointer:
				stack = append(stack, pointer.LogicalAddress)
				levels[pointer.LogicalAddress] = level
			}
		}
	}
}

func (s *Space[K, V]) deleteValue(
	v *Entry[K, V],
	pointerNode *Node[PointerNodeHeader, types.Pointer],
	dataNode *Node[DataNodeHeader, types.DataItem[K, V]],
) error {
	if *v.pEntry.State == types.StatePointer {
		_ = s.find(v, pointerNode, dataNode)
	}

	if *v.pEntry.State == types.StateFree {
		return nil
	}

	if v.stateP == nil || *v.stateP <= types.StateDeleted {
		return nil
	}
	if v.itemP.Hash == v.item.Hash && v.itemP.Key == v.item.Key {
		*v.stateP = types.StateDeleted
		s.config.EventCh <- types.SpaceDataNodeUpdatedEvent{
			Pointer:               v.pEntry.Pointer,
			PNodeAddress:          v.pAddress,
			RootPointer:           s.config.SpaceRoot.Pointer,
			ImmediateDeallocation: s.config.ImmediateDeallocation,
		}
		return nil
	}
	return nil
}

func (s *Space[K, V]) setValue(
	v *Entry[K, V],
	value V,
	pool *alloc.Pool[types.LogicalAddress],
	pointerNode *Node[PointerNodeHeader, types.Pointer],
	dataNode *Node[DataNodeHeader, types.DataItem[K, V]],
) error {
	if *v.pEntry.State == types.StatePointer {
		_ = s.find(v, pointerNode, dataNode)
	}

	v.item.Value = value

	if *v.pEntry.State == types.StateData && v.stateP != nil {
		if *v.stateP <= types.StateDeleted {
			*v.itemP = v.item
			*v.stateP = types.StateData
			v.exists = true
			return nil
		}
		if v.itemP.Hash == v.item.Hash && v.itemP.Key == v.item.Key {
			v.itemP.Value = value
			return nil
		}
	}

	return s.set(v, pool, pointerNode, dataNode)
}

func (s *Space[K, V]) set(
	v *Entry[K, V],
	pool *alloc.Pool[types.LogicalAddress],
	pointerNode *Node[PointerNodeHeader, types.Pointer],
	dataNode *Node[DataNodeHeader, types.DataItem[K, V]],
) error {
	for {
		switch *v.pEntry.State {
		case types.StateFree:
			dataNodeAddress, err := s.config.DataNodeAllocator.Allocate(pool, dataNode)
			if err != nil {
				return err
			}

			*v.pEntry.State = types.StateData
			v.pEntry.Pointer.LogicalAddress = dataNodeAddress

			item, state := dataNode.Item(s.config.DataNodeAllocator.Index(v.item.Hash + 1))
			*state = types.StateData
			*item = v.item

			v.stateP = state
			v.itemP = item
			v.exists = true

			s.config.EventCh <- types.SpaceDataNodeAllocatedEvent{
				Pointer:               v.pEntry.Pointer,
				PNodeAddress:          v.pAddress,
				RootPointer:           s.config.SpaceRoot.Pointer,
				ImmediateDeallocation: s.config.ImmediateDeallocation,
			}

			return nil
		case types.StateData:
			s.config.DataNodeAllocator.Get(v.pEntry.Pointer.LogicalAddress, dataNode)

			var conflict bool
			for i := types.Hash(0); i < trials; i++ {
				item, state := dataNode.Item(s.config.DataNodeAllocator.Index(v.item.Hash + 1<<i + i))
				if *state <= types.StateDeleted {
					*item = v.item

					v.stateP = state
					v.itemP = item
					v.exists = true

					s.config.EventCh <- types.SpaceDataNodeUpdatedEvent{
						Pointer:               v.pEntry.Pointer,
						PNodeAddress:          v.pAddress,
						RootPointer:           s.config.SpaceRoot.Pointer,
						ImmediateDeallocation: s.config.ImmediateDeallocation,
					}

					return nil
				}

				if v.item.Hash == item.Hash {
					if v.item.Key == item.Key {
						item.Value = v.item.Value

						v.stateP = state
						v.itemP = item
						v.exists = true

						s.config.EventCh <- types.SpaceDataNodeUpdatedEvent{
							Pointer:               v.pEntry.Pointer,
							PNodeAddress:          v.pAddress,
							RootPointer:           s.config.SpaceRoot.Pointer,
							ImmediateDeallocation: s.config.ImmediateDeallocation,
						}

						return nil
					}

					conflict = true
				}
			}

			if err := s.redistributeNode(
				v.pEntry,
				v.pAddress,
				v.pIndex,
				conflict,
				pool,
				pointerNode,
				dataNode,
			); err != nil {
				return err
			}
			return s.set(v, pool, pointerNode, dataNode)
		default:
			s.config.PointerNodeAllocator.Get(v.pEntry.Pointer.LogicalAddress,
				pointerNode)
			if pointerNode.Header.HashMod > 0 {
				v.item.Hash = hashKey(v.item.Key, pointerNode.Header.HashMod)
			}

			index := s.config.PointerNodeAllocator.Index(v.item.Hash)
			item, state := pointerNode.Item(index)
			v.item.Hash = s.config.PointerNodeAllocator.Shift(v.item.Hash)

			v.pAddress = v.pEntry.Pointer.LogicalAddress
			v.pIndex = index
			v.pEntry = types.ParentEntry{
				State:   state,
				Pointer: item,
			}
		}
	}
}

func (s *Space[K, V]) redistributeNode(
	pEntry types.ParentEntry,
	pAddress types.LogicalAddress,
	pIndex uintptr,
	conflict bool,
	pool *alloc.Pool[types.LogicalAddress],
	pointerNode *Node[PointerNodeHeader, types.Pointer],
	dataNode *Node[DataNodeHeader, types.DataItem[K, V]],
) error {
	dataNodePointer := pEntry.Pointer
	s.config.DataNodeAllocator.Get(dataNodePointer.LogicalAddress, dataNode)

	pointerNodeAddress, err := s.config.PointerNodeAllocator.Allocate(pool, pointerNode)
	if err != nil {
		return err
	}

	pointerNode.Header.ParentNodeAddress = pAddress
	pointerNode.Header.ParentNodeIndex = pIndex
	if conflict {
		*s.config.HashMod++
		*pointerNode.Header = PointerNodeHeader{
			HashMod: *s.config.HashMod,
		}
	}

	*pEntry.State = types.StatePointer
	pEntry.Pointer.LogicalAddress = pointerNodeAddress

	for item, state := range dataNode.Iterator() {
		if *state != types.StateData {
			continue
		}

		if conflict {
			item.Hash = hashKey(item.Key, pointerNode.Header.HashMod)
		}
		index := s.config.PointerNodeAllocator.Index(item.Hash)
		pointerItem, pointerState := pointerNode.Item(index)
		item.Hash = s.config.PointerNodeAllocator.Shift(item.Hash)

		if err := s.set(&Entry[K, V]{
			space: s,
			item:  *item,
			pEntry: types.ParentEntry{
				State:   pointerState,
				Pointer: pointerItem,
			},
			pAddress: pointerNodeAddress,
			pIndex:   index,
		}, pool, pointerNode, dataNode); err != nil {
			return err
		}
	}

	s.config.EventCh <- types.SpaceDataNodeDeallocationEvent{
		Pointer:               *dataNodePointer,
		ImmediateDeallocation: s.config.ImmediateDeallocation,
	}

	return nil
}

func (s *Space[K, V]) find(
	v *Entry[K, V],
	pointerNode *Node[PointerNodeHeader, types.Pointer],
	dataNode *Node[DataNodeHeader, types.DataItem[K, V]],
) error {
	for {
		switch *v.pEntry.State {
		case types.StatePointer:
			s.config.PointerNodeAllocator.Get(v.pEntry.Pointer.LogicalAddress, pointerNode)
			if pointerNode.Header.HashMod != 0 {
				v.item.Hash = hashKey(v.item.Key, pointerNode.Header.HashMod)
			}

			index := s.config.PointerNodeAllocator.Index(v.item.Hash)
			item, state := pointerNode.Item(index)
			v.item.Hash = s.config.PointerNodeAllocator.Shift(v.item.Hash)

			v.pAddress = v.pEntry.Pointer.LogicalAddress
			v.pIndex = index
			v.pEntry = types.ParentEntry{
				State:   state,
				Pointer: item,
			}
		case types.StateData:
			s.config.DataNodeAllocator.Get(v.pEntry.Pointer.LogicalAddress, dataNode)
			for i := types.Hash(0); i < trials; i++ {
				item, state := dataNode.Item(s.config.DataNodeAllocator.Index(v.item.Hash + 1<<i + i))

				switch *state {
				case types.StateFree:
					if v.stateP == nil {
						v.stateP = state
						v.itemP = item
					}
					return nil
				case types.StateData:
					if item.Hash == v.item.Hash && item.Key == v.item.Key {
						v.exists = true
						v.stateP = state
						v.itemP = item
						v.item.Value = item.Value
						return nil
					}
				default:
					if v.stateP == nil {
						v.stateP = state
						v.itemP = item
					}
				}
			}
			return nil
		default:
			return nil
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
	pIndex   uintptr
}

// Value returns the value from entry.
func (v *Entry[K, V]) Value() V {
	return v.item.Value
}

// Key returns the key from entry.
func (v *Entry[K, V]) Key() K {
	return v.item.Key
}

// Exists returns true if entry exists in the space.
func (v *Entry[K, V]) Exists() bool {
	return v.exists
}

// Set sts value for entry.
func (v *Entry[K, V]) Set(
	value V,
	pool *alloc.Pool[types.LogicalAddress],
	pointerNode *Node[PointerNodeHeader, types.Pointer],
	dataNode *Node[DataNodeHeader, types.DataItem[K, V]],
) error {
	return v.space.setValue(v, value, pool, pointerNode, dataNode)
}

// Delete deletes the entry.
func (v *Entry[K, V]) Delete(
	pointerNode *Node[PointerNodeHeader, types.Pointer],
	dataNode *Node[DataNodeHeader, types.DataItem[K, V]],
) error {
	return v.space.deleteValue(v, pointerNode, dataNode)
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

// Deallocate deallocates all nodes used by the space.
func Deallocate(
	state *alloc.State,
	spaceRoot types.ParentEntry,
	volatilePool *alloc.Pool[types.LogicalAddress],
	persistentPool *alloc.Pool[types.PhysicalAddress],
	pointerNodeAllocator *NodeAllocator[PointerNodeHeader, types.Pointer],
	pointerNode *Node[PointerNodeHeader, types.Pointer],
) {
	switch *spaceRoot.State {
	case types.StateFree:
		return
	case types.StateData:
		volatilePool.Deallocate(spaceRoot.Pointer.LogicalAddress)
		persistentPool.Deallocate(spaceRoot.Pointer.PhysicalAddress)
		return
	}

	// FIXME (wojciech): Optimize heap allocations
	stack := []*types.Pointer{spaceRoot.Pointer}

	for {
		if len(stack) == 0 {
			return
		}

		pointer := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		pointerNodeAllocator.Get(pointer.LogicalAddress, pointerNode)
		for p, state := range pointerNode.Iterator() {
			switch *state {
			case types.StateData:
				volatilePool.Deallocate(pointer.LogicalAddress)
				persistentPool.Deallocate(p.PhysicalAddress)
			case types.StatePointer:
				stack = append(stack, p)
			}
		}
		volatilePool.Deallocate(pointer.LogicalAddress)
		persistentPool.Deallocate(pointer.PhysicalAddress)
	}
}
