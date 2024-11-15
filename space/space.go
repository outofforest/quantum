package space

import (
	"sort"
	"unsafe"

	"github.com/cespare/xxhash"

	"github.com/outofforest/mass"
	"github.com/outofforest/photon"
	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/pipeline"
	"github.com/outofforest/quantum/types"
)

const trials = 20

// Config stores space configuration.
type Config[K, V comparable] struct {
	SpaceRoot             types.NodeRoot
	State                 *alloc.State
	DataNodeAssistant     *DataNodeAssistant[K, V]
	MassEntry             *mass.Mass[Entry[K, V]]
	ImmediateDeallocation bool
}

// New creates new space.
func New[K, V comparable](config Config[K, V]) *Space[K, V] {
	var k K
	s := &Space[K, V]{
		config:   config,
		hashBuff: make([]byte, unsafe.Sizeof(k)+1),
	}

	defaultInit := Entry[K, V]{
		space: s,
		storeRequest: pipeline.StoreRequest{
			ImmediateDeallocation: s.config.ImmediateDeallocation,
			PointersToStore:       1,
			Store:                 [pipeline.StoreCapacity]types.NodeRoot{s.config.SpaceRoot},
		},
	}

	s.initSize = uint64(uintptr(unsafe.Pointer(&defaultInit.storeRequest.Store[1])) -
		uintptr(unsafe.Pointer(&defaultInit)))
	s.defaultInit = make([]byte, s.initSize)
	copy(s.defaultInit, unsafe.Slice((*byte)(unsafe.Pointer(&defaultInit)), s.initSize))

	numOfItems := config.DataNodeAssistant.NumOfItems()
	s.trials = make([][trials]uint64, 0, numOfItems)
	for startIndex := range uint64(cap(s.trials)) {
		var offsets [trials]uint64
		for i := range uint64(trials) {
			offsets[i] = config.DataNodeAssistant.ItemOffset((startIndex + 1<<i + i) % numOfItems)
		}
		s.trials = append(s.trials, offsets)
	}

	return s
}

// Space represents the substate where values V are stored by key K.
type Space[K, V comparable] struct {
	config       Config[K, V]
	hashBuff     []byte
	initSize     uint64
	defaultInit  []byte
	defaultValue V
	trials       [][trials]uint64
}

// Find locates key in the space.
func (s *Space[K, V]) Find(key K) *Entry[K, V] {
	v := s.config.MassEntry.New()
	initBytes := unsafe.Slice((*byte)(unsafe.Pointer(v)), s.initSize)
	copy(initBytes, s.defaultInit)
	v.item.KeyHash = hashKey(&key, s.hashBuff, 0)
	v.item.Key = key

	s.find(v, false)
	return v
}

// Iterator returns iterator iterating over items in space.
func (s *Space[K, V]) Iterator() func(func(item *types.DataItem[K, V]) bool) {
	return func(yield func(item *types.DataItem[K, V]) bool) {
		s.iterate(s.config.SpaceRoot.Pointer, yield)
	}
}

func (s *Space[K, V]) iterate(pointer *types.Pointer, yield func(item *types.DataItem[K, V]) bool) {
	switch pointer.State {
	case types.StatePointer:
		pointerNode := ProjectPointerNode(s.config.State.Node(pointer.VolatileAddress))
		for pi := range pointerNode.Pointers {
			p := &pointerNode.Pointers[pi]
			if p.State == types.StateFree {
				continue
			}

			s.iterate(p, yield)
		}
	case types.StateData:
		for item := range s.config.DataNodeAssistant.Iterator(s.config.State.Node(pointer.VolatileAddress)) {
			if item.State != types.StateData {
				continue
			}
			if !yield(item) {
				return
			}
		}
	}
}

// Nodes returns list of nodes used by the space.
func (s *Space[K, V]) Nodes() []types.VolatileAddress {
	switch s.config.SpaceRoot.Pointer.State {
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

		pointerNode := ProjectPointerNode(s.config.State.Node(pointerNodeAddress))
		for pi := range pointerNode.Pointers {
			switch pointerNode.Pointers[pi].State {
			case types.StateFree:
			case types.StateData:
				nodes = append(nodes, pointerNode.Pointers[pi].VolatileAddress)
			case types.StatePointer:
				stack = append(stack, pointerNode.Pointers[pi].VolatileAddress)
			}
		}
	}
}

// Stats returns stats about the space.
func (s *Space[K, V]) Stats() (uint64, uint64, uint64, float64) {
	switch s.config.SpaceRoot.Pointer.State {
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

		pointerNode := ProjectPointerNode(s.config.State.Node(n))
		for pi := range pointerNode.Pointers {
			switch pointerNode.Pointers[pi].State {
			case types.StateFree:
			case types.StateData:
				dataNodes++
				if level > maxLevel {
					maxLevel = level
				}

				for dItem := range s.config.DataNodeAssistant.Iterator(s.config.State.Node(
					pointerNode.Pointers[pi].VolatileAddress,
				)) {
					dataSlots++
					if dItem.State == types.StateData {
						dataItems++
					}
				}
			case types.StatePointer:
				stack = append(stack, pointerNode.Pointers[pi].VolatileAddress)
				levels[pointerNode.Pointers[pi].VolatileAddress] = level
			}
		}
	}
}

func (s *Space[K, V]) valueExists(v *Entry[K, V]) bool {
	s.find(v, true)

	return v.exists
}

func (s *Space[K, V]) readValue(v *Entry[K, V]) V {
	s.find(v, true)

	return v.item.Value
}

func (s *Space[K, V]) deleteValue(tx *pipeline.TransactionRequest, v *Entry[K, V]) error {
	s.find(v, true)

	switch {
	case v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.State == types.StateFree:
	case v.itemP == nil || v.itemP.State <= types.StateDeleted:
	default:
		// If we are here it means `s.find` found the slot with matching key so don't need to check hash and key again.

		tx.AddStoreRequest(&v.storeRequest)

		v.item.State = types.StateDeleted
		v.itemP.State = types.StateDeleted
	}

	return nil
}

func (s *Space[K, V]) setValue(
	tx *pipeline.TransactionRequest,
	v *Entry[K, V],
	value V,
	pool *alloc.Pool[types.VolatileAddress],
) error {
	v.item.Value = value

	return s.set(tx, v, pool)
}

func (s *Space[K, V]) find(v *Entry[K, V], processDataNode bool) {
	s.walkPointers(v)

	if !processDataNode || v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.State != types.StateData {
		v.item.State = types.StateFree
		v.itemP = nil
		v.exists = false
		v.item.Value = s.defaultValue

		return
	}

	s.walkDataItems(v)

	if v.item.State == types.StateData {
		v.item.Value = v.itemP.Value
		return
	}

	v.item.Value = s.defaultValue
}

func (s *Space[K, V]) set(
	tx *pipeline.TransactionRequest,
	v *Entry[K, V],
	pool *alloc.Pool[types.VolatileAddress],
) error {
	s.walkPointers(v)

	if v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.State == types.StateFree {
		dataNodeAddress, err := pool.Allocate()
		if err != nil {
			return err
		}

		v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.VolatileAddress = dataNodeAddress
		v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.State = types.StateData
	}

	// Starting from here the data node is allocated.

	conflict := s.walkDataItems(v)

	if v.itemP != nil {
		tx.AddStoreRequest(&v.storeRequest)

		if v.item.State == types.StateData {
			v.itemP.Value = v.item.Value
			return nil
		}

		v.item.State = types.StateData
		*v.itemP = v.item
		return nil
	}

	return s.redistributeAndSet(tx, v, conflict, pool)
}

func (s *Space[K, V]) redistributeAndSet(
	tx *pipeline.TransactionRequest,
	v *Entry[K, V],
	conflict bool,
	pool *alloc.Pool[types.VolatileAddress],
) error {
	v.level++

	pointerNodeAddress, err := pool.Allocate()
	if err != nil {
		return err
	}

	// Persistent address stays the same, so data node will be reused for pointer node if both are
	// created in the same snapshot, or data node will be deallocated otherwise.

	tx.AddStoreRequest(&pipeline.StoreRequest{
		DeallocateVolatileAddress: v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.VolatileAddress,
	})

	pointerNode := ProjectPointerNode(s.config.State.Node(pointerNodeAddress))
	for item := range s.config.DataNodeAssistant.Iterator(s.config.State.Node(
		v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.VolatileAddress,
	)) {
		if item.State != types.StateData {
			continue
		}

		if conflict {
			item.KeyHash = hashKey(&item.Key, s.hashBuff, v.level)
		}

		index := PointerIndex(item.KeyHash)
		root := types.NodeRoot{
			Hash:    &pointerNode.Hashes[index],
			Pointer: &pointerNode.Pointers[index],
		}
		item.KeyHash = PointerShift(item.KeyHash)

		if err := s.set(tx,
			&Entry[K, V]{
				space: s,
				storeRequest: pipeline.StoreRequest{
					Store:           [pipeline.StoreCapacity]types.NodeRoot{root},
					PointersToStore: 1,
				},
				item:  *item,
				level: v.level,
			}, pool); err != nil {
			return err
		}
	}

	pointer := v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer

	if conflict {
		v.item.KeyHash = hashKey(&v.item.Key, s.hashBuff, v.level)
	}

	index := PointerIndex(v.item.KeyHash)
	v.item.KeyHash = PointerShift(v.item.KeyHash)

	// FIXME (wojciech): What if by any chance number of pointers exceeds 10?
	v.storeRequest.Store[v.storeRequest.PointersToStore].Hash = &pointerNode.Hashes[index]
	v.storeRequest.Store[v.storeRequest.PointersToStore].Pointer = &pointerNode.Pointers[index]
	v.storeRequest.PointersToStore++

	if err := s.set(tx, v, pool); err != nil {
		return err
	}

	// It must (!!!) be done as a last step, after moving all the data items to their new positions and
	// setting the revision by adding store request containing this pointer to the transaction request.
	pointer.VolatileAddress = pointerNodeAddress
	pointer.State = types.StatePointer

	if conflict {
		pointer.Flags = pointer.Flags.Set(types.FlagHashMod)
	}

	return nil
}

func (s *Space[K, V]) walkPointers(v *Entry[K, V]) {
	for v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.State == types.StatePointer {
		v.level++
		if v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.Flags.IsSet(types.FlagHashMod) {
			v.item.KeyHash = hashKey(&v.item.Key, s.hashBuff, v.level)
		}

		pointerNode := ProjectPointerNode(s.config.State.Node(
			v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.VolatileAddress,
		))
		index := PointerIndex(v.item.KeyHash)
		v.item.KeyHash = PointerShift(v.item.KeyHash)

		v.storeRequest.Store[v.storeRequest.PointersToStore].Hash = &pointerNode.Hashes[index]
		v.storeRequest.Store[v.storeRequest.PointersToStore].Pointer = &pointerNode.Pointers[index]
		v.storeRequest.PointersToStore++
	}
}

func (s *Space[K, V]) walkDataItems(v *Entry[K, V]) bool {
	v.item.State = types.StateFree
	v.itemP = nil
	v.exists = false

	var conflict bool
	node := s.config.State.Node(v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.VolatileAddress)
	startIndex := s.config.DataNodeAssistant.Index(v.item.KeyHash)
	for i, offsetP := 0, unsafe.Pointer(&s.trials[startIndex]); i < trials; i, offsetP = i+1,
		unsafe.Add(offsetP, types.UInt64Length) {
		item := (*types.DataItem[K, V])(unsafe.Add(node, *(*uint64)(offsetP)))

		switch item.State {
		case types.StateFree:
			if v.itemP == nil {
				v.itemP = item
			}
			v.item.State = v.itemP.State
			return conflict
		case types.StateData:
			if item.KeyHash == v.item.KeyHash {
				if item.Key == v.item.Key {
					v.exists = true
					v.itemP = item
					v.item.State = v.itemP.State
					return conflict
				}
				conflict = true
			}
		default:
			if v.itemP == nil {
				v.itemP = item
			}
		}
	}

	return conflict
}

// Entry represents entry in the space.
type Entry[K, V comparable] struct {
	space        *Space[K, V]
	storeRequest pipeline.StoreRequest

	itemP  *types.DataItem[K, V]
	item   types.DataItem[K, V]
	exists bool
	level  uint8
}

// Value returns the value from entry.
func (v *Entry[K, V]) Value() V {
	return v.space.readValue(v)
}

// Key returns the key from entry.
func (v *Entry[K, V]) Key() K {
	return v.item.Key
}

// Exists returns true if entry exists in the space.
func (v *Entry[K, V]) Exists() bool {
	return v.space.valueExists(v)
}

// Set sts value for entry.
func (v *Entry[K, V]) Set(
	value V,
	tx *pipeline.TransactionRequest,
	pool *alloc.Pool[types.VolatileAddress],
) error {
	return v.space.setValue(tx, v, value, pool)
}

// Delete deletes the entry.
func (v *Entry[K, V]) Delete(
	tx *pipeline.TransactionRequest,
) error {
	return v.space.deleteValue(tx, v)
}

func hashKey[K comparable](
	key *K,
	buff []byte,
	level uint8,
) types.KeyHash {
	var hash types.KeyHash
	p := photon.NewFromValue[K](key)
	if level == 0 {
		hash = types.KeyHash(xxhash.Sum64(p.B))
	} else {
		buff[0] = level
		copy(buff[1:], p.B)
		hash = types.KeyHash(xxhash.Sum64(buff))
	}

	if types.IsTesting {
		hash = testHash(hash)
	}

	return hash
}

func testHash(hash types.KeyHash) types.KeyHash {
	return hash & 0x7fffffff
}

// Deallocate deallocates all nodes used by the space.
func Deallocate(
	spaceRoot *types.Pointer,
	volatilePool *alloc.Pool[types.VolatileAddress],
	persistentPool *alloc.Pool[types.PersistentAddress],
	state *alloc.State,
) {
	switch spaceRoot.State {
	case types.StateFree:
		return
	case types.StateData:
		volatilePool.Deallocate(spaceRoot.VolatileAddress)
		persistentPool.Deallocate(spaceRoot.PersistentAddress)
		return
	}

	deallocatePointerNode(spaceRoot, volatilePool, persistentPool, state)
}

func deallocatePointerNode(
	pointer *types.Pointer,
	volatilePool *alloc.Pool[types.VolatileAddress],
	persistentPool *alloc.Pool[types.PersistentAddress],
	state *alloc.State,
) {
	pointerNode := ProjectPointerNode(state.Node(pointer.VolatileAddress))
	for pi := range pointerNode.Pointers {
		p := &pointerNode.Pointers[pi]

		switch p.State {
		case types.StateData:
			volatilePool.Deallocate(pointer.VolatileAddress)
			persistentPool.Deallocate(p.PersistentAddress)
		case types.StatePointer:
			deallocatePointerNode(p, volatilePool, persistentPool, state)
		}
	}
	volatilePool.Deallocate(pointer.VolatileAddress)
	persistentPool.Deallocate(pointer.PersistentAddress)
}
