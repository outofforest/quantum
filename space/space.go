package space

import (
	"math"
	"math/bits"
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
		config:         config,
		hashBuff:       make([]byte, unsafe.Sizeof(k)+1),
		numOfDataItems: config.DataNodeAssistant.NumOfItems(),
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

	s.trials = make([][trials]uint64, 0, s.numOfDataItems)
	// FIXME (wojciech): Prevent duplicates in the row and generate uniform distribution of each index across the rows.
	for startIndex := range uint64(cap(s.trials)) {
		var offsets [trials]uint64
		for i := range uint64(trials) {
			offsets[i] = config.DataNodeAssistant.ItemOffset((startIndex + 1<<i + i) % s.numOfDataItems)
		}
		s.trials = append(s.trials, offsets)
	}

	return s
}

// Space represents the substate where values V are stored by key K.
type Space[K, V comparable] struct {
	config         Config[K, V]
	hashBuff       []byte
	initSize       uint64
	defaultInit    []byte
	defaultValue   V
	trials         [][trials]uint64
	numOfDataItems uint64
}

// Find locates key in the space.
func (s *Space[K, V]) Find(key K) *Entry[K, V] {
	v := s.config.MassEntry.New()
	initBytes := unsafe.Slice((*byte)(unsafe.Pointer(v)), s.initSize)
	copy(initBytes, s.defaultInit)
	v.item.KeyHash = hashKey(&key, nil, 0)
	v.item.Key = key
	v.dataNodeIndex = dataNodeIndex(v.item.KeyHash, s.numOfDataItems)

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
	pointer := v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer
	if v.storeRequest.PointersToStore > 1 && pointer.State == types.StateData && pointer.Revision != v.revision {
		v.storeRequest.PointersToStore--
		v.level--
	}

	s.find(v, true)

	return v.exists
}

func (s *Space[K, V]) readValue(v *Entry[K, V]) V {
	pointer := v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer
	if v.storeRequest.PointersToStore > 1 && pointer.State == types.StateData && pointer.Revision != v.revision {
		v.storeRequest.PointersToStore--
		v.level--
	}

	s.find(v, true)

	return v.item.Value
}

func (s *Space[K, V]) deleteValue(tx *pipeline.TransactionRequest, v *Entry[K, V]) error {
	pointer := v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer
	if v.storeRequest.PointersToStore > 1 && pointer.State == types.StateData && pointer.Revision != v.revision {
		v.storeRequest.PointersToStore--
		v.level--
	}

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

	pointer := v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer
	if v.storeRequest.PointersToStore > 1 && pointer.State == types.StateData && pointer.Revision != v.revision {
		v.storeRequest.PointersToStore--
		v.level--
	}

	return s.set(tx, v, pool)
}

func (s *Space[K, V]) find(v *Entry[K, V], processDataNode bool) {
	s.walkPointers(v, processDataNode)

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
	s.walkPointers(v, true)

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

		v.revision = v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.Revision

		if v.item.State == types.StateData {
			v.itemP.Value = v.item.Value
			return nil
		}

		v.item.State = types.StateData
		*v.itemP = v.item
		return nil
	}

	// Try to split data node.
	if v.storeRequest.PointersToStore > 1 {
		newIndex, mask := s.splitToIndex(
			v.storeRequest.Store[v.storeRequest.PointersToStore-2].Pointer.VolatileAddress,
			v.parentIndex,
		)

		if newIndex != v.parentIndex {
			newDataNodeAddress, err := pool.Allocate()
			if err != nil {
				return err
			}

			s.splitDataNode(
				tx,
				v.parentIndex,
				newIndex,
				mask,
				v.storeRequest.Store[v.storeRequest.PointersToStore-2].Pointer.VolatileAddress,
				v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.VolatileAddress,
				newDataNodeAddress,
				v.level,
			)

			// This must be done because the item to set might go to the newly created data node.
			v.storeRequest.PointersToStore--
			v.level--

			return s.set(tx, v, pool)
		}
	}

	// Add pointer node.
	if err := s.addPointerNode(tx, v, conflict, pool); err != nil {
		return err
	}

	return s.set(tx, v, pool)
}

func (s *Space[K, V]) splitToIndex(parentNodeAddress types.VolatileAddress, index uint64) (uint64, uint64) {
	trailingZeros := bits.TrailingZeros64(NumOfPointers)
	if trailingZeros2 := bits.TrailingZeros64(index); trailingZeros2 < trailingZeros {
		trailingZeros = trailingZeros2
	}
	mask := uint64(1) << trailingZeros

	parentNode := ProjectPointerNode(s.config.State.Node(parentNodeAddress))
	for mask > 1 {
		mask >>= 1
		newIndex := index | mask
		if parentNode.Pointers[newIndex].State == types.StateFree {
			index = newIndex
			break
		}
	}

	return index, uint64(math.MaxUint64) << bits.TrailingZeros64(mask)
}

func (s *Space[K, V]) splitDataNode(
	tx *pipeline.TransactionRequest,
	index uint64,
	newIndex uint64,
	mask uint64,
	parentNodeAddress types.VolatileAddress,
	existingNodeAddress, newNodeAddress types.VolatileAddress,
	level uint8,
) {
	parentNode := ProjectPointerNode(s.config.State.Node(parentNodeAddress))
	newDataNode := s.config.State.Node(newNodeAddress)

	var itemI uint64
	for item := range s.config.DataNodeAssistant.Iterator(s.config.State.Node(existingNodeAddress)) {
		itemI++
		if item.State == types.StateFree {
			continue
		}

		newDataNodeItem := s.config.DataNodeAssistant.Item(newDataNode, s.config.DataNodeAssistant.ItemOffset(itemI-1))

		if item.State == types.StateDeleted {
			newDataNodeItem.State = types.StateDeleted
			continue
		}

		itemIndex := PointerIndex(item.KeyHash, level-1)
		if itemIndex&mask != newIndex {
			newDataNodeItem.State = types.StateDeleted
			continue
		}

		// FIXME (wojciech): By doing this, following accesses must do a lot of hops to determine free slot.
		// Consider rearranging items in the block to make slots to free instead of deleted.
		*newDataNodeItem = *item
		item.State = types.StateDeleted
	}

	parentNode.Pointers[newIndex].VolatileAddress = newNodeAddress
	parentNode.Pointers[newIndex].State = types.StateData

	tx.AddStoreRequest(&pipeline.StoreRequest{
		Store: [pipeline.StoreCapacity]types.NodeRoot{
			{
				Hash:    &parentNode.Hashes[index],
				Pointer: &parentNode.Pointers[index],
			},
			{
				Hash:    &parentNode.Hashes[newIndex],
				Pointer: &parentNode.Pointers[newIndex],
			},
		},
		PointersToStore:       2,
		ImmediateDeallocation: s.config.ImmediateDeallocation,
	})
}

func (s *Space[K, V]) addPointerNode(
	tx *pipeline.TransactionRequest,
	v *Entry[K, V],
	conflict bool,
	pool *alloc.Pool[types.VolatileAddress],
) error {
	pointerNodeAddress, err := pool.Allocate()
	if err != nil {
		return err
	}

	newDataNodeAddress, err := pool.Allocate()
	if err != nil {
		return err
	}

	pointerNode := ProjectPointerNode(s.config.State.Node(pointerNodeAddress))
	pointerNode.Pointers[0] = *v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer

	pointerNodeRoot := &v.storeRequest.Store[v.storeRequest.PointersToStore-1]

	pointerNodeRoot.Pointer.VolatileAddress = pointerNodeAddress
	pointerNodeRoot.Pointer.State = types.StatePointer
	pointerNodeRoot.Pointer.PersistentAddress = 0
	pointerNodeRoot.Pointer.SnapshotID = 0

	if conflict {
		pointerNodeRoot.Pointer.Flags = pointerNodeRoot.Pointer.Flags.Set(types.FlagHashMod)
	}

	newIndex, mask := s.splitToIndex(pointerNodeAddress, 0)

	s.splitDataNode(
		tx,
		0,
		newIndex,
		mask,
		pointerNodeAddress,
		pointerNode.Pointers[0].VolatileAddress,
		newDataNodeAddress,
		v.level+1,
	)

	return nil
}

var pointerHops = [NumOfPointers][]uint64{
	{},
	{0x0},
	{0x0},
	{0x2, 0x0},
	{0x0},
	{0x4, 0x0},
	{0x4, 0x0},
	{0x6, 0x4, 0x0},
	{0x0},
	{0x8, 0x0},
	{0x8, 0x0},
	{0xa, 0x8, 0x0},
	{0x8, 0x0},
	{0xc, 0x8, 0x0},
	{0xc, 0x8, 0x0},
	{0xe, 0xc, 0x8, 0x0},
	{0x0},
	{0x10, 0x0},
	{0x10, 0x0},
	{0x12, 0x10, 0x0},
	{0x10, 0x0},
	{0x14, 0x10, 0x0},
	{0x14, 0x10, 0x0},
	{0x16, 0x14, 0x10, 0x0},
	{0x10, 0x0},
	{0x18, 0x10, 0x0},
	{0x18, 0x10, 0x0},
	{0x1a, 0x18, 0x10, 0x0},
	{0x18, 0x10, 0x0},
	{0x1c, 0x18, 0x10, 0x0},
	{0x1c, 0x18, 0x10, 0x0},
	{0x1e, 0x1c, 0x18, 0x10, 0x0},
	{},
	{0x20},
	{0x20},
	{0x22, 0x20},
	{0x20},
	{0x24, 0x20},
	{0x24, 0x20},
	{0x26, 0x24, 0x20},
	{0x20},
	{0x28, 0x20},
	{0x28, 0x20},
	{0x2a, 0x28, 0x20},
	{0x28, 0x20},
	{0x2c, 0x28, 0x20},
	{0x2c, 0x28, 0x20},
	{0x2e, 0x2c, 0x28, 0x20},
	{0x20},
	{0x30, 0x20},
	{0x30, 0x20},
	{0x32, 0x30, 0x20},
	{0x30, 0x20},
	{0x34, 0x30, 0x20},
	{0x34, 0x30, 0x20},
	{0x36, 0x34, 0x30, 0x20},
	{0x30, 0x20},
	{0x38, 0x30, 0x20},
	{0x38, 0x30, 0x20},
	{0x3a, 0x38, 0x30, 0x20},
	{0x38, 0x30, 0x20},
	{0x3c, 0x38, 0x30, 0x20},
	{0x3c, 0x38, 0x30, 0x20},
	{0x3e, 0x3c, 0x38, 0x30, 0x20},
}

func (s *Space[K, V]) walkPointers(v *Entry[K, V], processDataNode bool) {
	for v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.State == types.StatePointer {
		if v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.Flags.IsSet(types.FlagHashMod) {
			v.item.KeyHash = hashKey(&v.item.Key, s.hashBuff, v.level)
		}

		pointerNode := ProjectPointerNode(s.config.State.Node(
			v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.VolatileAddress,
		))
		index := PointerIndex(v.item.KeyHash, v.level)
		state := pointerNode.Pointers[index].State

		switch state {
		case types.StateFree:
			if !processDataNode {
				return
			}

			hops := pointerHops[index]
			var dataFound bool
			for {
				hopIndex := len(hops) / 2
				newIndex := hops[hopIndex]

				switch pointerNode.Pointers[newIndex].State {
				case types.StateFree:
					if !dataFound {
						index = newIndex
					}
					hops = hops[hopIndex+1:]
				case types.StateData:
					index = newIndex
					hops = hops[:hopIndex]
					dataFound = true
				case types.StatePointer:
					hops = hops[:hopIndex]
				}

				if len(hops) == 0 {
					break
				}
			}
		case types.StatePointer:
		case types.StateData:
			if !processDataNode {
				return
			}
		default:
			return
		}

		v.level++
		v.storeRequest.Store[v.storeRequest.PointersToStore].Hash = &pointerNode.Hashes[index]
		v.storeRequest.Store[v.storeRequest.PointersToStore].Pointer = &pointerNode.Pointers[index]
		v.storeRequest.PointersToStore++
		v.parentIndex = index
		v.revision = pointerNode.Pointers[index].Revision
	}
}

func (s *Space[K, V]) walkDataItems(v *Entry[K, V]) bool {
	if v.itemP != nil && v.revision == v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.Revision {
		return false
	}

	v.item.State = types.StateFree
	v.itemP = nil
	v.exists = false

	var conflict bool
	node := s.config.State.Node(v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.VolatileAddress)
	for i, offsetP := 0, unsafe.Pointer(&s.trials[v.dataNodeIndex]); i < trials; i, offsetP = i+1,
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

	itemP         *types.DataItem[K, V]
	item          types.DataItem[K, V]
	parentIndex   uint64
	dataNodeIndex uint64
	revision      uint32
	exists        bool
	level         uint8
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

func hashKey[K comparable](key *K, buff []byte, level uint8) types.KeyHash {
	var hash types.KeyHash
	p := photon.NewFromValue[K](key)
	if buff == nil {
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

func dataNodeIndex(keyHash types.KeyHash, numOfDataItems uint64) uint64 {
	return (bits.RotateLeft64(uint64(keyHash), types.UInt64Length/2) ^ uint64(keyHash)) % numOfDataItems
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
