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
	"github.com/outofforest/quantum/pipeline"
	"github.com/outofforest/quantum/types"
)

const trials = 20

// Config stores space configuration.
type Config[K, V comparable] struct {
	SpaceRoot             *types.Pointer
	State                 *alloc.State
	PointerNodeAssistant  *NodeAssistant[types.Pointer]
	DataNodeAssistant     *NodeAssistant[types.DataItem[K, V]]
	MassEntry             *mass.Mass[Entry[K, V]]
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
		space:   s,
		pointer: s.config.SpaceRoot,
		storeRequest: pipeline.StoreRequest{
			ImmediateDeallocation: s.config.ImmediateDeallocation,
			PointersToStore:       1,
			Store:                 [pipeline.StoreCapacity]*types.Pointer{s.config.SpaceRoot},
		},
	}

	s.initSize = uint64(unsafe.Sizeof(defaultInit))
	// FIXME (wojciech): Calculate this number automatically
	s.initSize = 40
	s.defaultInit = make([]byte, s.initSize)
	copy(s.defaultInit, unsafe.Slice((*byte)(unsafe.Pointer(&defaultInit)), s.initSize))

	numOfItems := config.DataNodeAssistant.NumOfItems()
	s.trials = make([][trials]uint64, 0, numOfItems)
	for startIndex := range uint64(cap(s.trials)) {
		var indexes [trials]uint64
		for i := range uint64(trials) {
			indexes[i] = (startIndex + 1<<i + i) % numOfItems
		}
		s.trials = append(s.trials, indexes)
	}

	return s
}

// Space represents the substate where values V are stored by key K.
type Space[K, V comparable] struct {
	config      Config[K, V]
	hashBuff    []byte
	massPointer *mass.Mass[*types.Pointer]
	initSize    uint64
	defaultInit []byte
	trials      [][trials]uint64
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
) *Entry[K, V] {
	v := s.config.MassEntry.New()
	initBytes := unsafe.Slice((*byte)(unsafe.Pointer(v)), s.initSize)
	copy(initBytes, s.defaultInit)
	v.item.Hash = hashKey(&key, s.hashBuff, 0)
	v.item.Key = key

	s.find(v, pointerNode, nil)
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
	pointer *types.Pointer,
	yield func(item *types.DataItem[K, V]) bool,
) {
	switch pointer.State {
	case types.StatePointer, types.StatePointerWithHashMod:
		s.config.PointerNodeAssistant.Project(pointer.VolatileAddress, pointerNode)
		for item := range pointerNode.Iterator() {
			if pointer.State == types.StateFree {
				continue
			}

			s.iterate(pointerNode,
				dataNode,
				item,
				yield,
			)
		}
	case types.StateData:
		s.config.DataNodeAssistant.Project(pointer.VolatileAddress, dataNode)
		for item := range dataNode.Iterator() {
			if item.State != types.StateData {
				continue
			}
			if !yield(item) {
				return
			}
		}
	}
}

type pointerToAllocate struct {
	Level   uint64
	Pointer *types.Pointer
}

// AllocatePointers allocates specified levels of pointer nodes.
func (s *Space[K, V]) AllocatePointers(
	tx *pipeline.TransactionRequest,
	levels uint64,
	pool *alloc.Pool[types.VolatileAddress],
	pointerNode *Node[types.Pointer],
) error {
	if s.config.SpaceRoot.State != types.StateFree {
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

	sr := pipeline.StoreRequest{
		Store:           [pipeline.StoreCapacity]*types.Pointer{s.config.SpaceRoot},
		PointersToStore: 1,
	}

	stack := []pointerToAllocate{
		{
			Level:   1,
			Pointer: s.config.SpaceRoot,
		},
	}

	for {
		if len(stack) == 0 {
			if sr.PointersToStore > 0 {
				tx.AddStoreRequest(&sr)
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

		pToAllocate.Pointer.State = types.StatePointer
		pToAllocate.Pointer.VolatileAddress = pointerNodeAddress

		if pToAllocate.Level == levels {
			continue
		}

		pToAllocate.Level++
		for item := range pointerNode.Iterator() {
			sr.Store[sr.PointersToStore] = item
			sr.PointersToStore++

			if sr.PointersToStore == pipeline.StoreCapacity {
				tx.AddStoreRequest(lo.ToPtr(sr))
				sr.PointersToStore = 0
			}

			stack = append(stack, pointerToAllocate{
				Level:   pToAllocate.Level,
				Pointer: item,
			})
		}
	}
}

// Nodes returns list of nodes used by the space.
func (s *Space[K, V]) Nodes(pointerNode *Node[types.Pointer]) []types.VolatileAddress {
	switch s.config.SpaceRoot.State {
	case types.StateFree:
		return nil
	case types.StateData:
		return []types.VolatileAddress{s.config.SpaceRoot.VolatileAddress}
	}

	nodes := []types.VolatileAddress{}
	stack := []types.VolatileAddress{s.config.SpaceRoot.VolatileAddress}

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
		for pointer := range pointerNode.Iterator() {
			switch pointer.State {
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
	switch s.config.SpaceRoot.State {
	case types.StateFree:
		return 0, 0, 0, 0
	case types.StateData:
		return 1, 0, 1, 0
	}

	stack := []types.VolatileAddress{s.config.SpaceRoot.VolatileAddress}

	levels := map[types.VolatileAddress]uint64{
		s.config.SpaceRoot.VolatileAddress: 1,
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
		for pointer := range pointerNode.Iterator() {
			switch pointer.State {
			case types.StateFree:
			case types.StateData:
				dataNodes++
				if level > maxLevel {
					maxLevel = level
				}

				s.config.DataNodeAssistant.Project(pointer.VolatileAddress, dataNode)
				for dItem := range dataNode.Iterator() {
					dataSlots++
					if dItem.State == types.StateData {
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

func (s *Space[K, V]) valueExists(
	v *Entry[K, V],
	pointerNode *Node[types.Pointer],
	dataNode *Node[types.DataItem[K, V]],
) bool {
	if !v.dataNodeProcessed {
		s.find(v, pointerNode, dataNode)
	}

	return v.exists
}

func (s *Space[K, V]) readValue(
	v *Entry[K, V],
	pointerNode *Node[types.Pointer],
	dataNode *Node[types.DataItem[K, V]],
) V {
	if !v.dataNodeProcessed {
		s.find(v, pointerNode, dataNode)
	}

	return v.item.Value
}

func (s *Space[K, V]) deleteValue(
	tx *pipeline.TransactionRequest,
	v *Entry[K, V],
	pointerNode *Node[types.Pointer],
	dataNode *Node[types.DataItem[K, V]],
) error {
	if v.pointer.State == types.StatePointer || v.pointer.State == types.StatePointerWithHashMod || !v.dataNodeProcessed {
		s.find(v, pointerNode, dataNode)
	}

	if v.pointer.State == types.StateFree {
		return nil
	}

	if v.itemP == nil || v.itemP.State <= types.StateDeleted {
		return nil
	}
	if v.itemP.Hash == v.item.Hash && v.itemP.Key == v.item.Key {
		v.item.State = types.StateDeleted
		v.itemP.State = types.StateDeleted

		tx.AddStoreRequest(&v.storeRequest)

		return nil
	}
	return nil
}

func (s *Space[K, V]) setValue(
	tx *pipeline.TransactionRequest,
	v *Entry[K, V],
	value V,
	pool *alloc.Pool[types.VolatileAddress],
	pointerNode *Node[types.Pointer],
	dataNode *Node[types.DataItem[K, V]],
) error {
	if v.pointer.State == types.StatePointer || v.pointer.State == types.StatePointerWithHashMod || !v.dataNodeProcessed {
		s.find(v, pointerNode, dataNode)
	}

	v.item.Value = value

	if v.pointer.State == types.StateData && v.itemP != nil {
		if v.item.State <= types.StateDeleted {
			v.item.State = types.StateData
			*v.itemP = v.item
			v.exists = true

			tx.AddStoreRequest(&v.storeRequest)

			return nil
		}
		if v.itemP.Hash == v.item.Hash && v.itemP.Key == v.item.Key {
			v.itemP.Value = value

			tx.AddStoreRequest(&v.storeRequest)

			return nil
		}
	}

	return s.set(tx, v, pool, pointerNode, dataNode)
}

func (s *Space[K, V]) set(
	tx *pipeline.TransactionRequest,
	v *Entry[K, V],
	pool *alloc.Pool[types.VolatileAddress],
	pointerNode *Node[types.Pointer],
	dataNode *Node[types.DataItem[K, V]],
) error {
	v.dataNodeProcessed = true

	for {
		switch v.pointer.State {
		case types.StateFree:
			dataNodeAddress, err := pool.Allocate()
			if err != nil {
				return err
			}

			s.config.DataNodeAssistant.Project(dataNodeAddress, dataNode)

			v.pointer.VolatileAddress = dataNodeAddress
			v.pointer.State = types.StateData

			index := s.config.DataNodeAssistant.Index(v.item.Hash + 1)
			item := dataNode.Item(index)

			v.item.State = types.StateData
			*item = v.item

			v.itemP = item
			v.exists = true

			tx.AddStoreRequest(&v.storeRequest)

			return nil
		case types.StateData:
			s.config.DataNodeAssistant.Project(v.pointer.VolatileAddress, dataNode)

			var conflict bool
			startIndex := s.config.DataNodeAssistant.Index(v.item.Hash)
			for i, indexP := 0, unsafe.Pointer(&s.trials[startIndex]); i < trials; i, indexP = i+1,
				unsafe.Add(indexP, types.UInt64Length) {
				item := dataNode.Item(*(*uint64)(indexP))

				if item.State <= types.StateDeleted {
					v.item.State = types.StateData
					*item = v.item

					v.itemP = item
					v.exists = true

					tx.AddStoreRequest(&v.storeRequest)

					return nil
				}

				if v.item.Hash == item.Hash {
					if v.item.Key == item.Key {
						item.Value = v.item.Value

						v.itemP = item
						v.exists = true

						tx.AddStoreRequest(&v.storeRequest)

						return nil
					}

					conflict = true
				}
			}

			return s.redistributeAndSet(
				tx,
				v,
				conflict,
				pool,
				pointerNode,
				dataNode,
			)
		default:
			s.config.PointerNodeAssistant.Project(v.pointer.VolatileAddress,
				pointerNode)
			if v.pointer.State == types.StatePointerWithHashMod {
				v.item.Hash = hashKey(&v.item.Key, s.hashBuff, v.pointer.VolatileAddress)
			}

			index := s.config.PointerNodeAssistant.Index(v.item.Hash)
			pointer := pointerNode.Item(index)
			v.item.Hash = s.config.PointerNodeAssistant.Shift(v.item.Hash)
			v.pointer = pointer

			// FIXME (wojciech): What if by any chance number of pointers exceeds 10?
			v.storeRequest.Store[v.storeRequest.PointersToStore] = pointer
			v.storeRequest.PointersToStore++
		}
	}
}

func (s *Space[K, V]) redistributeAndSet(
	tx *pipeline.TransactionRequest,
	v *Entry[K, V],
	conflict bool,
	pool *alloc.Pool[types.VolatileAddress],
	pointerNode *Node[types.Pointer],
	dataNode *Node[types.DataItem[K, V]],
) error {
	s.config.DataNodeAssistant.Project(v.pointer.VolatileAddress, dataNode)

	pointerNodeAddress, err := pool.Allocate()
	if err != nil {
		return err
	}
	s.config.PointerNodeAssistant.Project(pointerNodeAddress, pointerNode)

	v.pointer.VolatileAddress = pointerNodeAddress
	if conflict {
		v.pointer.State = types.StatePointerWithHashMod
	} else {
		v.pointer.State = types.StatePointer
	}

	// Persistent address stays the same, so data node will be reused for pointer node if both are
	// created in the same snapshot, or data node will be deallocated otherwise.

	// FIXME (wojciech): volatile address of the data node must be deallocated.

	for item := range dataNode.Iterator() {
		if item.State != types.StateData {
			continue
		}

		if conflict {
			item.Hash = hashKey(&item.Key, s.hashBuff, pointerNodeAddress)
		}

		index := s.config.PointerNodeAssistant.Index(item.Hash)
		pointer := pointerNode.Item(index)
		item.Hash = s.config.PointerNodeAssistant.Shift(item.Hash)

		if err := s.set(tx,
			&Entry[K, V]{
				space:   s,
				pointer: pointer,
				storeRequest: pipeline.StoreRequest{
					Store:           [pipeline.StoreCapacity]*types.Pointer{pointer},
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
	v.pointer = pointerNode.Item(index)

	// FIXME (wojciech): What if by any chance number of pointers exceeds 10?
	v.storeRequest.Store[v.storeRequest.PointersToStore] = v.pointer
	v.storeRequest.PointersToStore++

	return s.set(tx, v, pool, pointerNode, dataNode)
}

func (s *Space[K, V]) find(
	v *Entry[K, V],
	pointerNode *Node[types.Pointer],
	dataNode *Node[types.DataItem[K, V]],
) {
	for {
		switch v.pointer.State {
		case types.StatePointer, types.StatePointerWithHashMod:
			s.config.PointerNodeAssistant.Project(v.pointer.VolatileAddress, pointerNode)
			if v.pointer.State == types.StatePointerWithHashMod {
				v.item.Hash = hashKey(&v.item.Key, s.hashBuff, v.pointer.VolatileAddress)
			}

			index := s.config.PointerNodeAssistant.Index(v.item.Hash)
			pointer := pointerNode.Item(index)
			v.item.Hash = s.config.PointerNodeAssistant.Shift(v.item.Hash)
			v.pointer = pointer

			v.storeRequest.Store[v.storeRequest.PointersToStore] = pointer
			v.storeRequest.PointersToStore++
		case types.StateData:
			if dataNode == nil {
				return
			}

			v.dataNodeProcessed = true

			s.config.DataNodeAssistant.Project(v.pointer.VolatileAddress, dataNode)
			startIndex := s.config.DataNodeAssistant.Index(v.item.Hash)
			for i, indexP := 0, unsafe.Pointer(&s.trials[startIndex]); i < trials; i, indexP = i+1,
				unsafe.Add(indexP, types.UInt64Length) {
				item := dataNode.Item(*(*uint64)(indexP))

				switch item.State {
				case types.StateFree:
					if v.itemP == nil {
						v.itemP = item
					}
					return
				case types.StateData:
					if item.Hash == v.item.Hash && item.Key == v.item.Key {
						v.exists = true
						v.itemP = item
						v.item.Value = item.Value

						return
					}
				default:
					if v.itemP == nil {
						v.itemP = item
					}
				}
			}
			return
		case types.StateFree:
			v.dataNodeProcessed = true
			return
		default:
			// When `Find` method tries to read the state while the other goroutine changes it from free to data
			// (new allocation) or from data to pointer (redistribution), it might happen that we read a value which is
			// temporarily illegal. It's not a problem. We just return with `dataNodeProcessed = false` and the rest of
			// the job will be done in the other goroutine when it tries to read or set the value.
			return
		}
	}
}

// Entry represents entry in the space.
type Entry[K, V comparable] struct {
	space        *Space[K, V]
	pointer      *types.Pointer
	storeRequest pipeline.StoreRequest

	itemP             *types.DataItem[K, V]
	item              types.DataItem[K, V]
	exists            bool
	dataNodeProcessed bool
}

// Value returns the value from entry.
func (v *Entry[K, V]) Value(
	pointerNode *Node[types.Pointer],
	dataNode *Node[types.DataItem[K, V]],
) V {
	return v.space.readValue(v, pointerNode, dataNode)
}

// Key returns the key from entry.
func (v *Entry[K, V]) Key() K {
	return v.item.Key
}

// Exists returns true if entry exists in the space.
func (v *Entry[K, V]) Exists(
	pointerNode *Node[types.Pointer],
	dataNode *Node[types.DataItem[K, V]],
) bool {
	return v.space.valueExists(v, pointerNode, dataNode)
}

// Set sts value for entry.
func (v *Entry[K, V]) Set(
	value V,
	tx *pipeline.TransactionRequest,
	pool *alloc.Pool[types.VolatileAddress],
	pointerNode *Node[types.Pointer],
	dataNode *Node[types.DataItem[K, V]],
) error {
	return v.space.setValue(tx, v, value, pool, pointerNode, dataNode)
}

// Delete deletes the entry.
func (v *Entry[K, V]) Delete(
	tx *pipeline.TransactionRequest,
	pointerNode *Node[types.Pointer],
	dataNode *Node[types.DataItem[K, V]],
) error {
	return v.space.deleteValue(tx, v, pointerNode, dataNode)
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
	spaceRoot *types.Pointer,
	volatilePool *alloc.Pool[types.VolatileAddress],
	persistentPool *alloc.Pool[types.PersistentAddress],
	pointerNodeAssistant *NodeAssistant[types.Pointer],
	pointerNode *Node[types.Pointer],
) {
	switch spaceRoot.State {
	case types.StateFree:
		return
	case types.StateData:
		volatilePool.Deallocate(spaceRoot.VolatileAddress)
		persistentPool.Deallocate(spaceRoot.PersistentAddress)
		return
	}

	deallocatePointerNode(spaceRoot, volatilePool, persistentPool, pointerNodeAssistant, pointerNode)
}

func deallocatePointerNode(
	pointer *types.Pointer,
	volatilePool *alloc.Pool[types.VolatileAddress],
	persistentPool *alloc.Pool[types.PersistentAddress],
	pointerNodeAssistant *NodeAssistant[types.Pointer],
	pointerNode *Node[types.Pointer],
) {
	pointerNodeAssistant.Project(pointer.VolatileAddress, pointerNode)
	for p := range pointerNode.Iterator() {
		switch p.State {
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
