package space

import (
	"sort"
	"unsafe"

	"github.com/cespare/xxhash"
	"github.com/pkg/errors"
	"github.com/samber/lo"

	"github.com/outofforest/mass"
	"github.com/outofforest/photon"
	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/queue"
	"github.com/outofforest/quantum/types"
)

const trials = 20

// Config stores space configuration.
type Config[K, V comparable] struct {
	SpaceRoot             types.ParentEntry
	State                 *alloc.State
	PointerNodeAssistant  *NodeAssistant[types.Pointer]
	DataNodeAssistant     *NodeAssistant[types.DataItem[K, V]]
	MassEntry             *mass.Mass[Entry[K, V]]
	StoreQ                *queue.Queue
	ImmediateDeallocation bool
}

// New creates new space.
func New[K, V comparable](config Config[K, V]) *Space[K, V] {
	var k K
	s := &Space[K, V]{
		config:      config,
		hashBuff:    make([]byte, unsafe.Sizeof(k)+types.UInt64Length),
		massPointer: mass.New[*types.Pointer](10000),
	}

	defaultInit := Entry[K, V]{
		space:  s,
		pEntry: s.config.SpaceRoot,
		storeRequest: queue.Request{
			ImmediateDeallocation: s.config.ImmediateDeallocation,
			PointersToStore:       1,
			Store:                 [queue.StoreCapacity]*types.Pointer{s.config.SpaceRoot.Pointer},
		},
	}

	s.initSize = uint64(unsafe.Sizeof(defaultInit))
	// FIXME (wojciech): Calculate this number automatically
	s.initSize = 48
	s.defaultInit = make([]byte, s.initSize)
	copy(s.defaultInit, unsafe.Slice((*byte)(unsafe.Pointer(&defaultInit)), s.initSize))

	return s
}

// Space represents the substate where values V are stored by key K.
type Space[K, V comparable] struct {
	config      Config[K, V]
	hashBuff    []byte
	massPointer *mass.Mass[*types.Pointer]
	initSize    uint64
	defaultInit []byte
}

// NewPointerNode creates new pointer node representation.
func (s *Space[K, V]) NewPointerNode() *Node[types.Pointer] {
	return s.config.PointerNodeAssistant.NewNode()
}

// NewDataNode creates new data node representation.
func (s *Space[K, V]) NewDataNode() *Node[types.DataItem[K, V]] {
	return s.config.DataNodeAssistant.NewNode()
}

// Find locates key in the space.
func (s *Space[K, V]) Find(
	key K,
	pointerNode *Node[types.Pointer],
	dataNode *Node[types.DataItem[K, V]],
) *Entry[K, V] {
	v := s.config.MassEntry.New()
	initBytes := unsafe.Slice((*byte)(unsafe.Pointer(v)), s.initSize)
	copy(initBytes, s.defaultInit)
	v.item.Hash = hashKey(&key, s.hashBuff, 0)
	v.item.Key = key

	s.find(v, pointerNode, dataNode)
	return v
}

// Iterator returns iterator iterating over items in space.
func (s *Space[K, V]) Iterator(
	pointerNode *Node[types.Pointer],
	dataNode *Node[types.DataItem[K, V]],
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
	pointerNode *Node[types.Pointer],
	dataNode *Node[types.DataItem[K, V]],
	pEntry types.ParentEntry,
	yield func(item *types.DataItem[K, V]) bool,
) {
	switch *pEntry.State {
	case types.StatePointer, types.StatePointerWithHashMod:
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
	Level  uint64
	PEntry types.ParentEntry
}

// AllocatePointers allocates specified levels of pointer nodes.
func (s *Space[K, V]) AllocatePointers(
	levels uint64,
	pool *alloc.Pool[types.VolatileAddress],
	pointerNode *Node[types.Pointer],
) error {
	if *s.config.SpaceRoot.State != types.StateFree {
		return errors.New("pointers can be preallocated only on empty space")
	}
	if levels == 0 {
		return nil
	}

	numOfItems := s.config.PointerNodeAssistant.NumOfItems()
	var numOfPointers uint64 = 1
	for i := uint64(1); i < levels; i++ {
		numOfPointers = numOfItems*numOfPointers + 1
	}

	sr := queue.Request{
		Store:           [queue.StoreCapacity]*types.Pointer{s.config.SpaceRoot.Pointer},
		PointersToStore: 1,
	}

	stack := []pointerToAllocate{
		{
			Level:  1,
			PEntry: s.config.SpaceRoot,
		},
	}

	for {
		if len(stack) == 0 {
			if sr.PointersToStore > 0 {
				s.config.StoreQ.Push(&sr)
			}
			return nil
		}

		pToAllocate := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		pointerNodeAddress, err := pool.Allocate()
		if err != nil {
			return err
		}

		s.config.PointerNodeAssistant.Project(pointerNodeAddress, pointerNode)

		*pToAllocate.PEntry.State = types.StatePointer
		pToAllocate.PEntry.Pointer.VolatileAddress = pointerNodeAddress

		if pToAllocate.Level == levels {
			continue
		}

		pToAllocate.Level++
		for item, state := range pointerNode.Iterator() {
			sr.Store[sr.PointersToStore] = item
			sr.PointersToStore++

			if sr.PointersToStore == queue.StoreCapacity {
				s.config.StoreQ.Push(lo.ToPtr(sr))
				sr.PointersToStore = 0
			}

			stack = append(stack, pointerToAllocate{
				Level: pToAllocate.Level,
				PEntry: types.ParentEntry{
					State:   state,
					Pointer: item,
				},
			})
		}
	}
}

// Nodes returns list of nodes used by the space.
func (s *Space[K, V]) Nodes(pointerNode *Node[types.Pointer]) []types.VolatileAddress {
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
			case types.StatePointer, types.StatePointerWithHashMod:
				stack = append(stack, pointer.VolatileAddress)
			}
		}
	}
}

// Stats returns stats about the space.
func (s *Space[K, V]) Stats(
	pointerNode *Node[types.Pointer],
	dataNode *Node[types.DataItem[K, V]],
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
			case types.StatePointer, types.StatePointerWithHashMod:
				stack = append(stack, pointer.VolatileAddress)
				levels[pointer.VolatileAddress] = level
			}
		}
	}
}

func (s *Space[K, V]) deleteValue(
	v *Entry[K, V],
	pointerNode *Node[types.Pointer],
	dataNode *Node[types.DataItem[K, V]],
) error {
	if *v.pEntry.State == types.StatePointer || *v.pEntry.State == types.StatePointerWithHashMod {
		s.find(v, pointerNode, dataNode)
	}

	if *v.pEntry.State == types.StateFree {
		return nil
	}

	if v.stateP == nil || *v.stateP <= types.StateDeleted {
		return nil
	}
	if v.itemP.Hash == v.item.Hash && v.itemP.Key == v.item.Key {
		*v.stateP = types.StateDeleted

		s.config.StoreQ.Push(&v.storeRequest)

		return nil
	}
	return nil
}

func (s *Space[K, V]) setValue(
	v *Entry[K, V],
	value V,
	pool *alloc.Pool[types.VolatileAddress],
	pointerNode *Node[types.Pointer],
	dataNode *Node[types.DataItem[K, V]],
) error {
	if *v.pEntry.State == types.StatePointer || *v.pEntry.State == types.StatePointerWithHashMod {
		s.find(v, pointerNode, dataNode)
	}

	v.item.Value = value

	if *v.pEntry.State == types.StateData && v.stateP != nil {
		if *v.stateP <= types.StateDeleted {
			*v.itemP = v.item
			*v.stateP = types.StateData
			v.exists = true

			s.config.StoreQ.Push(&v.storeRequest)

			return nil
		}
		if v.itemP.Hash == v.item.Hash && v.itemP.Key == v.item.Key {
			v.itemP.Value = value

			s.config.StoreQ.Push(&v.storeRequest)

			return nil
		}
	}

	return s.set(v, pool, pointerNode, dataNode)
}

func (s *Space[K, V]) set(
	v *Entry[K, V],
	pool *alloc.Pool[types.VolatileAddress],
	pointerNode *Node[types.Pointer],
	dataNode *Node[types.DataItem[K, V]],
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

			s.config.StoreQ.Push(&v.storeRequest)

			return nil
		case types.StateData:
			s.config.DataNodeAssistant.Project(v.pEntry.Pointer.VolatileAddress, dataNode)

			var conflict bool
			for i := types.Hash(0); i < trials; i++ {
				index := s.config.DataNodeAssistant.Index(v.item.Hash + 1<<i + i)
				state := dataNode.State(index)
				item := dataNode.Item(index)

				if *state <= types.StateDeleted {
					*state = types.StateData
					*item = v.item

					v.stateP = state
					v.itemP = item
					v.exists = true

					s.config.StoreQ.Push(&v.storeRequest)

					return nil
				}

				if v.item.Hash == item.Hash {
					if v.item.Key == item.Key {
						item.Value = v.item.Value

						v.stateP = state
						v.itemP = item
						v.exists = true

						s.config.StoreQ.Push(&v.storeRequest)

						return nil
					}

					conflict = true
				}
			}

			return s.redistributeAndSet(
				v,
				conflict,
				pool,
				pointerNode,
				dataNode,
			)
		default:
			s.config.PointerNodeAssistant.Project(v.pEntry.Pointer.VolatileAddress,
				pointerNode)
			if *v.pEntry.State == types.StatePointerWithHashMod {
				v.item.Hash = hashKey(&v.item.Key, s.hashBuff, v.pEntry.Pointer.VolatileAddress)
			}

			index := s.config.PointerNodeAssistant.Index(v.item.Hash)
			state := pointerNode.State(index)
			pointer := pointerNode.Item(index)
			v.item.Hash = s.config.PointerNodeAssistant.Shift(v.item.Hash)
			v.pEntry = types.ParentEntry{
				State:   state,
				Pointer: pointer,
			}

			// FIXME (wojciech): What if by any chance number of pointers exceeds 10?
			v.storeRequest.Store[v.storeRequest.PointersToStore] = pointer
			v.storeRequest.PointersToStore++
		}
	}
}

func (s *Space[K, V]) redistributeAndSet(
	v *Entry[K, V],
	conflict bool,
	pool *alloc.Pool[types.VolatileAddress],
	pointerNode *Node[types.Pointer],
	dataNode *Node[types.DataItem[K, V]],
) error {
	s.config.DataNodeAssistant.Project(v.pEntry.Pointer.VolatileAddress, dataNode)

	pointerNodeAddress, err := pool.Allocate()
	if err != nil {
		return err
	}
	s.config.PointerNodeAssistant.Project(pointerNodeAddress, pointerNode)

	if conflict {
		*v.pEntry.State = types.StatePointerWithHashMod
	} else {
		*v.pEntry.State = types.StatePointer
	}
	v.pEntry.Pointer.VolatileAddress = pointerNodeAddress

	// Persistent address stays the same, so data node will be reused for pointer node if both are
	// created in the same snapshot, or data node will be deallocated otherwise.

	// FIXME (wojciech): volatile address of the data node must be deallocated.

	for item, state := range dataNode.Iterator() {
		if *state != types.StateData {
			continue
		}

		if conflict {
			item.Hash = hashKey(&item.Key, s.hashBuff, pointerNodeAddress)
		}

		index := s.config.PointerNodeAssistant.Index(item.Hash)
		pointer := pointerNode.Item(index)
		item.Hash = s.config.PointerNodeAssistant.Shift(item.Hash)

		if err := s.set(&Entry[K, V]{
			space: s,
			pEntry: types.ParentEntry{
				State:   pointerNode.State(index),
				Pointer: pointer,
			},
			storeRequest: queue.Request{
				Store:           [queue.StoreCapacity]*types.Pointer{pointer},
				PointersToStore: 1,
			},
			item: *item,
		}, pool, pointerNode, dataNode); err != nil {
			return err
		}
	}

	if conflict {
		v.item.Hash = hashKey(&v.item.Key, s.hashBuff, pointerNodeAddress)
	}

	index := s.config.PointerNodeAssistant.Index(v.item.Hash)
	v.item.Hash = s.config.PointerNodeAssistant.Shift(v.item.Hash)
	v.pEntry.State = pointerNode.State(index)
	v.pEntry.Pointer = pointerNode.Item(index)

	// FIXME (wojciech): What if by any chance number of pointers exceeds 10?
	v.storeRequest.Store[v.storeRequest.PointersToStore] = v.pEntry.Pointer
	v.storeRequest.PointersToStore++

	return s.set(v, pool, pointerNode, dataNode)
}

func (s *Space[K, V]) find(
	v *Entry[K, V],
	pointerNode *Node[types.Pointer],
	dataNode *Node[types.DataItem[K, V]],
) {
	for {
		switch *v.pEntry.State {
		case types.StatePointer, types.StatePointerWithHashMod:
			s.config.PointerNodeAssistant.Project(v.pEntry.Pointer.VolatileAddress, pointerNode)
			if *v.pEntry.State == types.StatePointerWithHashMod {
				v.item.Hash = hashKey(&v.item.Key, s.hashBuff, v.pEntry.Pointer.VolatileAddress)
			}

			index := s.config.PointerNodeAssistant.Index(v.item.Hash)
			pointer := pointerNode.Item(index)
			v.item.Hash = s.config.PointerNodeAssistant.Shift(v.item.Hash)
			v.pEntry = types.ParentEntry{
				State:   pointerNode.State(index),
				Pointer: pointer,
			}

			v.storeRequest.Store[v.storeRequest.PointersToStore] = pointer
			v.storeRequest.PointersToStore++
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
					return
				case types.StateData:
					item := dataNode.Item(index)
					if item.Hash == v.item.Hash && item.Key == v.item.Key {
						v.exists = true
						v.stateP = state
						v.itemP = item
						v.item.Value = item.Value

						return
					}
				default:
					if v.stateP == nil {
						v.stateP = state
						v.itemP = dataNode.Item(index)
					}
				}
			}
			return
		default:
			return
		}
	}
}

// Entry represents entry in the space.
type Entry[K, V comparable] struct {
	space        *Space[K, V]
	pEntry       types.ParentEntry
	storeRequest queue.Request

	itemP  *types.DataItem[K, V]
	stateP *types.State
	item   types.DataItem[K, V]
	exists bool
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
	pointerNode *Node[types.Pointer],
	dataNode *Node[types.DataItem[K, V]],
) error {
	return v.space.setValue(v, value, pool, pointerNode, dataNode)
}

// Delete deletes the entry.
func (v *Entry[K, V]) Delete(
	pointerNode *Node[types.Pointer],
	dataNode *Node[types.DataItem[K, V]],
) error {
	return v.space.deleteValue(v, pointerNode, dataNode)
}

func hashKey[K comparable](
	key *K,
	buff []byte,
	// FIXME (wojciech): Better if this is deterministic, so taking the address is not a good idea.
	address types.VolatileAddress,
) types.Hash {
	var hash types.Hash
	p := photon.NewFromValue[K](key)
	if address == 0 {
		hash = types.Hash(xxhash.Sum64(p.B))
	} else {
		copy(buff, photon.NewFromValue(&address).B)
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
	pointerNodeAssistant *NodeAssistant[types.Pointer],
	pointerNode *Node[types.Pointer],
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
	pointerNodeAssistant *NodeAssistant[types.Pointer],
	pointerNode *Node[types.Pointer],
) {
	pointerNodeAssistant.Project(pointer.VolatileAddress, pointerNode)
	for p, state := range pointerNode.Iterator() {
		switch *state {
		case types.StateData:
			volatilePool.Deallocate(pointer.VolatileAddress)
			persistentPool.Deallocate(p.PersistentAddress)
		case types.StatePointer, types.StatePointerWithHashMod:
			deallocatePointerNode(p, volatilePool, persistentPool, pointerNodeAssistant, pointerNode)
		}
	}
	volatilePool.Deallocate(pointer.VolatileAddress)
	persistentPool.Deallocate(pointer.PersistentAddress)
}
