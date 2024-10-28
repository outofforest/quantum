package space

import (
	"sort"
	"unsafe"

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
	HashMod                  *uint64
	SpaceRoot                types.ParentEntry
	State                    *alloc.State
	PointerNodeAssistant     *NodeAssistant[PointerNodeHeader, types.Pointer]
	DataNodeAssistant        *NodeAssistant[DataNodeHeader, types.DataItem[K, V]]
	MassEntry                *mass.Mass[Entry[K, V]]
	MassDataNodeUpdatedEvent *mass.Mass[types.SpaceDataNodeUpdatedEvent]
	EventCh                  chan<- any
	ImmediateDeallocation    bool
}

// New creates new space.
func New[K, V comparable](config Config[K, V]) *Space[K, V] {
	var k K
	return &Space[K, V]{
		config:   config,
		hashBuff: make([]byte, unsafe.Sizeof(k)+types.UInt64Length),
	}
}

// Space represents the substate where values V are stored by key K.
type Space[K, V comparable] struct {
	config   Config[K, V]
	hashBuff []byte
}

// NewPointerNode creates new pointer node representation.
func (s *Space[K, V]) NewPointerNode() *Node[PointerNodeHeader, types.Pointer] {
	return s.config.PointerNodeAssistant.NewNode()
}

// NewDataNode creates new data node representation.
func (s *Space[K, V]) NewDataNode() *Node[DataNodeHeader, types.DataItem[K, V]] {
	return s.config.DataNodeAssistant.NewNode()
}

// Find locates key in the space.
func (s *Space[K, V]) Find(
	key K,
	pointerNode *Node[PointerNodeHeader, types.Pointer],
	dataNode *Node[DataNodeHeader, types.DataItem[K, V]],
) *Entry[K, V] {
	v := s.config.MassEntry.New()
	v.space = s
	v.item = types.DataItem[K, V]{
		Hash: hashKey(key, s.hashBuff, 0),
		Key:  key,
	}
	v.pEntry = s.config.SpaceRoot

	// For get, err is always nil.
	_ = s.find(v, pointerNode, dataNode)
	return v
}

// Iterator returns iterator iterating over items in space.
func (s *Space[K, V]) Iterator(
	pointerNode *Node[PointerNodeHeader, types.Pointer],
	dataNode *Node[DataNodeHeader, types.DataItem[K, V]],
) func(func(item *types.DataItem[K, V]) bool) {
	return func(yield func(item *types.DataItem[K, V]) bool) {
		s.iterate(
			pointerNode,
			dataNode,
			s.config.SpaceRoot,
			yield,
		)
	}
}

func (s *Space[K, V]) iterate(
	pointerNode *Node[PointerNodeHeader, types.Pointer],
	dataNode *Node[DataNodeHeader, types.DataItem[K, V]],
	pEntry types.ParentEntry,
	yield func(item *types.DataItem[K, V]) bool,
) {
	switch *pEntry.State {
	case types.StatePointer:
		s.config.PointerNodeAssistant.Project(pEntry.Pointer.VolatileAddress, pointerNode)
		for item, state := range pointerNode.Iterator() {
			if *state == types.StateFree {
				continue
			}

			s.iterate(pointerNode,
				dataNode,
				types.ParentEntry{
					State:   state,
					Pointer: item,
				},
				yield,
			)
		}
	case types.StateData:
		s.config.DataNodeAssistant.Project(pEntry.Pointer.VolatileAddress, dataNode)
		for item, state := range dataNode.Iterator() {
			if *state != types.StateData {
				continue
			}
			if !yield(item) {
				return
			}
		}
	}
}

type pointerToAllocate struct {
	Level    uint64
	PEntry   types.ParentEntry
	PAddress types.VolatileAddress
	PIndex   uintptr
}

// AllocatePointers allocates specified levels of pointer nodes.
func (s *Space[K, V]) AllocatePointers(
	levels uint64,
	pool *alloc.Pool[types.VolatileAddress],
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

		pointerNodeAddress, err := pool.Allocate()
		if err != nil {
			return err
		}

		s.config.PointerNodeAssistant.Project(pointerNodeAddress, pointerNode)

		pointerNode.Header.ParentNodeAddress = pToAllocate.PAddress
		pointerNode.Header.ParentNodeIndex = pToAllocate.PIndex

		*pToAllocate.PEntry.State = types.StatePointer
		pToAllocate.PEntry.Pointer.VolatileAddress = pointerNodeAddress

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
func (s *Space[K, V]) Nodes(pointerNode *Node[PointerNodeHeader, types.Pointer]) []types.VolatileAddress {
	switch *s.config.SpaceRoot.State {
	case types.StateFree:
		return nil
	case types.StateData:
		return []types.VolatileAddress{s.config.SpaceRoot.Pointer.VolatileAddress}
	}

	nodes := []types.VolatileAddress{}
	stack := []types.VolatileAddress{s.config.SpaceRoot.Pointer.VolatileAddress}

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

		s.config.PointerNodeAssistant.Project(pointerNodeAddress, pointerNode)
		for pointer, state := range pointerNode.Iterator() {
			switch *state {
			case types.StateFree:
			case types.StateData:
				nodes = append(nodes, pointer.VolatileAddress)
			case types.StatePointer:
				stack = append(stack, pointer.VolatileAddress)
			}
		}
	}
}

// Stats returns stats about the space.
func (s *Space[K, V]) Stats(
	pointerNode *Node[PointerNodeHeader, types.Pointer],
	dataNode *Node[DataNodeHeader, types.DataItem[K, V]],
) (uint64, uint64, uint64, float64) {
	switch *s.config.SpaceRoot.State {
	case types.StateFree:
		return 0, 0, 0, 0
	case types.StateData:
		return 1, 0, 1, 0
	}

	stack := []types.VolatileAddress{s.config.SpaceRoot.Pointer.VolatileAddress}

	levels := map[types.VolatileAddress]uint64{
		s.config.SpaceRoot.Pointer.VolatileAddress: 1,
	}
	var maxLevel, pointerNodes, dataNodes, dataItems, dataSlots uint64

	for {
		if len(stack) == 0 {
			return maxLevel, pointerNodes, dataNodes, float64(dataItems) / float64(dataSlots)
		}

		n := stack[len(stack)-1]
		level := levels[n] + 1
		pointerNodes++
		stack = stack[:len(stack)-1]

		s.config.PointerNodeAssistant.Project(n, pointerNode)
		for pointer, state := range pointerNode.Iterator() {
			switch *state {
			case types.StateFree:
			case types.StateData:
				dataNodes++
				if level > maxLevel {
					maxLevel = level
				}

				s.config.DataNodeAssistant.Project(pointer.VolatileAddress, dataNode)
				for _, dState := range dataNode.Iterator() {
					dataSlots++
					if *dState == types.StateData {
						dataItems++
					}
				}
			case types.StatePointer:
				stack = append(stack, pointer.VolatileAddress)
				levels[pointer.VolatileAddress] = level
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
		s.config.EventCh <- &types.SpaceDataNodeUpdatedEvent{
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
	pool *alloc.Pool[types.VolatileAddress],
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

			event := s.config.MassDataNodeUpdatedEvent.New()
			event.Pointer = v.pEntry.Pointer
			event.PNodeAddress = v.pAddress
			event.RootPointer = s.config.SpaceRoot.Pointer
			event.ImmediateDeallocation = s.config.ImmediateDeallocation

			s.config.EventCh <- event

			return nil
		}
		if v.itemP.Hash == v.item.Hash && v.itemP.Key == v.item.Key {
			v.itemP.Value = value

			event := s.config.MassDataNodeUpdatedEvent.New()
			event.Pointer = v.pEntry.Pointer
			event.PNodeAddress = v.pAddress
			event.RootPointer = s.config.SpaceRoot.Pointer
			event.ImmediateDeallocation = s.config.ImmediateDeallocation

			s.config.EventCh <- event

			return nil
		}
	}

	return s.set(v, pool, pointerNode, dataNode)
}

func (s *Space[K, V]) set(
	v *Entry[K, V],
	pool *alloc.Pool[types.VolatileAddress],
	pointerNode *Node[PointerNodeHeader, types.Pointer],
	dataNode *Node[DataNodeHeader, types.DataItem[K, V]],
) error {
	for {
		switch *v.pEntry.State {
		case types.StateFree:
			dataNodeAddress, err := pool.Allocate()
			if err != nil {
				return err
			}

			s.config.DataNodeAssistant.Project(dataNodeAddress, dataNode)

			*v.pEntry.State = types.StateData
			v.pEntry.Pointer.VolatileAddress = dataNodeAddress

			index := s.config.DataNodeAssistant.Index(v.item.Hash + 1)
			state := dataNode.State(index)
			item := dataNode.Item(index)

			*state = types.StateData
			*item = v.item

			v.stateP = state
			v.itemP = item
			v.exists = true

			event := s.config.MassDataNodeUpdatedEvent.New()
			event.Pointer = v.pEntry.Pointer
			event.PNodeAddress = v.pAddress
			event.RootPointer = s.config.SpaceRoot.Pointer
			event.ImmediateDeallocation = s.config.ImmediateDeallocation

			s.config.EventCh <- event

			return nil
		case types.StateData:
			s.config.DataNodeAssistant.Project(v.pEntry.Pointer.VolatileAddress, dataNode)

			var conflict bool
			for i := types.Hash(0); i < trials; i++ {
				index := s.config.DataNodeAssistant.Index(v.item.Hash + 1<<i + i)
				state := dataNode.State(index)
				item := dataNode.Item(index)

				if *state <= types.StateDeleted {
					*item = v.item

					v.stateP = state
					v.itemP = item
					v.exists = true

					event := s.config.MassDataNodeUpdatedEvent.New()
					event.Pointer = v.pEntry.Pointer
					event.PNodeAddress = v.pAddress
					event.RootPointer = s.config.SpaceRoot.Pointer
					event.ImmediateDeallocation = s.config.ImmediateDeallocation

					s.config.EventCh <- event

					return nil
				}

				if v.item.Hash == item.Hash {
					if v.item.Key == item.Key {
						item.Value = v.item.Value

						v.stateP = state
						v.itemP = item
						v.exists = true

						event := s.config.MassDataNodeUpdatedEvent.New()
						event.Pointer = v.pEntry.Pointer
						event.PNodeAddress = v.pAddress
						event.RootPointer = s.config.SpaceRoot.Pointer
						event.ImmediateDeallocation = s.config.ImmediateDeallocation

						s.config.EventCh <- event

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
			s.config.PointerNodeAssistant.Project(v.pEntry.Pointer.VolatileAddress,
				pointerNode)
			if pointerNode.Header.HashMod > 0 {
				v.item.Hash = hashKey(v.item.Key, s.hashBuff, pointerNode.Header.HashMod)
			}

			index := s.config.PointerNodeAssistant.Index(v.item.Hash)
			v.item.Hash = s.config.PointerNodeAssistant.Shift(v.item.Hash)
			v.pAddress = v.pEntry.Pointer.VolatileAddress
			v.pIndex = index
			v.pEntry = types.ParentEntry{
				State:   pointerNode.State(index),
				Pointer: pointerNode.Item(index),
			}
		}
	}
}

func (s *Space[K, V]) redistributeNode(
	pEntry types.ParentEntry,
	pAddress types.VolatileAddress,
	pIndex uintptr,
	conflict bool,
	pool *alloc.Pool[types.VolatileAddress],
	pointerNode *Node[PointerNodeHeader, types.Pointer],
	dataNode *Node[DataNodeHeader, types.DataItem[K, V]],
) error {
	dataNodePointer := pEntry.Pointer
	s.config.DataNodeAssistant.Project(dataNodePointer.VolatileAddress, dataNode)

	pointerNodeAddress, err := pool.Allocate()
	if err != nil {
		return err
	}
	s.config.PointerNodeAssistant.Project(pointerNodeAddress, pointerNode)

	pointerNode.Header.ParentNodeAddress = pAddress
	pointerNode.Header.ParentNodeIndex = pIndex
	if conflict {
		*s.config.HashMod++
		*pointerNode.Header = PointerNodeHeader{
			HashMod: *s.config.HashMod,
		}
	}

	*pEntry.State = types.StatePointer
	pEntry.Pointer.VolatileAddress = pointerNodeAddress

	for item, state := range dataNode.Iterator() {
		if *state != types.StateData {
			continue
		}

		if conflict {
			item.Hash = hashKey(item.Key, s.hashBuff, pointerNode.Header.HashMod)
		}
		index := s.config.PointerNodeAssistant.Index(item.Hash)
		item.Hash = s.config.PointerNodeAssistant.Shift(item.Hash)

		if err := s.set(&Entry[K, V]{
			space:    s,
			item:     *item,
			pAddress: pointerNodeAddress,
			pIndex:   index,
			pEntry: types.ParentEntry{
				State:   pointerNode.State(index),
				Pointer: pointerNode.Item(index),
			},
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
			s.config.PointerNodeAssistant.Project(v.pEntry.Pointer.VolatileAddress, pointerNode)
			if pointerNode.Header.HashMod != 0 {
				v.item.Hash = hashKey(v.item.Key, s.hashBuff, pointerNode.Header.HashMod)
			}

			index := s.config.PointerNodeAssistant.Index(v.item.Hash)
			v.item.Hash = s.config.PointerNodeAssistant.Shift(v.item.Hash)
			v.pAddress = v.pEntry.Pointer.VolatileAddress
			v.pIndex = index
			v.pEntry = types.ParentEntry{
				State:   pointerNode.State(index),
				Pointer: pointerNode.Item(index),
			}
		case types.StateData:
			s.config.DataNodeAssistant.Project(v.pEntry.Pointer.VolatileAddress, dataNode)
			for i := types.Hash(0); i < trials; i++ {
				index := s.config.DataNodeAssistant.Index(v.item.Hash + 1<<i + i)
				state := dataNode.State(index)

				switch *state {
				case types.StateFree:
					if v.stateP == nil {
						v.stateP = state
						v.itemP = dataNode.Item(index)
					}
					return nil
				case types.StateData:
					item := dataNode.Item(index)
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
						v.itemP = dataNode.Item(index)
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
	pAddress types.VolatileAddress
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
	pool *alloc.Pool[types.VolatileAddress],
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

func hashKey[K comparable](
	key K,
	buff []byte,
	hashMod uint64,
) types.Hash {
	var hash types.Hash
	p := photon.NewFromValue[K](&key)
	if hashMod == 0 {
		hash = types.Hash(xxhash.Sum64(p.B))
	} else {
		copy(buff, photon.NewFromValue(&hashMod).B)
		copy(buff[types.UInt64Length:], p.B)
		hash = types.Hash(xxhash.Sum64(buff))
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
	spaceRoot types.ParentEntry,
	volatilePool *alloc.Pool[types.VolatileAddress],
	persistentPool *alloc.Pool[types.PersistentAddress],
	pointerNodeAssistant *NodeAssistant[PointerNodeHeader, types.Pointer],
	pointerNode *Node[PointerNodeHeader, types.Pointer],
) {
	switch *spaceRoot.State {
	case types.StateFree:
		return
	case types.StateData:
		volatilePool.Deallocate(spaceRoot.Pointer.VolatileAddress)
		persistentPool.Deallocate(spaceRoot.Pointer.PersistentAddress)
		return
	}

	deallocatePointerNode(spaceRoot.Pointer, volatilePool, persistentPool, pointerNodeAssistant, pointerNode)
}

func deallocatePointerNode(
	pointer *types.Pointer,
	volatilePool *alloc.Pool[types.VolatileAddress],
	persistentPool *alloc.Pool[types.PersistentAddress],
	pointerNodeAssistant *NodeAssistant[PointerNodeHeader, types.Pointer],
	pointerNode *Node[PointerNodeHeader, types.Pointer],
) {
	pointerNodeAssistant.Project(pointer.VolatileAddress, pointerNode)
	for p, state := range pointerNode.Iterator() {
		switch *state {
		case types.StateData:
			volatilePool.Deallocate(pointer.VolatileAddress)
			persistentPool.Deallocate(p.PersistentAddress)
		case types.StatePointer:
			deallocatePointerNode(p, volatilePool, persistentPool, pointerNodeAssistant, pointerNode)
		}
	}
	volatilePool.Deallocate(pointer.VolatileAddress)
	persistentPool.Deallocate(pointer.PersistentAddress)
}
