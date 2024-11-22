package space

import (
	"math"
	"math/bits"
	"sort"
	"unsafe"

	"github.com/cespare/xxhash"
	"github.com/samber/lo"

	"github.com/outofforest/mass"
	"github.com/outofforest/photon"
	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/pipeline"
	"github.com/outofforest/quantum/space/compare"
	"github.com/outofforest/quantum/types"
	"github.com/outofforest/quantum/wal"
)

// Config stores space configuration.
type Config[K, V comparable] struct {
	SpaceRoot         types.NodeRoot
	State             *alloc.State
	DataNodeAssistant *DataNodeAssistant[K, V]
	MassEntry         *mass.Mass[Entry[K, V]]
	NoSnapshots       bool
}

// New creates new space.
func New[K, V comparable](config Config[K, V]) *Space[K, V] {
	s := &Space[K, V]{
		config:         config,
		numOfDataItems: config.DataNodeAssistant.NumOfItems(),
	}

	defaultInit := Entry[K, V]{
		space:             s,
		nextDataNodeState: lo.ToPtr(types.StateFree),
		storeRequest: pipeline.StoreRequest{
			NoSnapshots:     s.config.NoSnapshots,
			PointersToStore: 1,
			Store:           [pipeline.StoreCapacity]types.NodeRoot{s.config.SpaceRoot},
		},
	}

	s.initSize = uint64(uintptr(unsafe.Pointer(&defaultInit.storeRequest.Store[1])) -
		uintptr(unsafe.Pointer(&defaultInit)))
	s.defaultInit = make([]byte, s.initSize)
	copy(s.defaultInit, unsafe.Slice((*byte)(unsafe.Pointer(&defaultInit)), s.initSize))

	return s
}

// Space represents the substate where values V are stored by key K.
type Space[K, V comparable] struct {
	config         Config[K, V]
	initSize       uint64
	defaultInit    []byte
	defaultValue   V
	numOfDataItems uint64
}

// NewHashBuff allocates buffer required to compute key hash.
func (s *Space[K, V]) NewHashBuff() []byte {
	var k K
	return make([]byte, unsafe.Sizeof(k)+1)
}

// NewHashMatches allocates new slice used to match hashes.
func (s *Space[K, V]) NewHashMatches() []uint64 {
	return make([]uint64, s.numOfDataItems)
}

// Find locates key in the space.
func (s *Space[K, V]) Find(key K, hashBuff []byte, hashMatches []uint64) *Entry[K, V] {
	v := s.config.MassEntry.New()
	initBytes := unsafe.Slice((*byte)(unsafe.Pointer(v)), s.initSize)
	copy(initBytes, s.defaultInit)
	v.keyHash = hashKey(&key, nil, 0)
	v.item.Key = key
	v.dataItemIndex = dataItemIndex(v.keyHash, s.numOfDataItems)

	s.find(v, hashBuff, hashMatches)
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
		for _, item := range s.config.DataNodeAssistant.Iterator(s.config.State.Node(pointer.VolatileAddress)) {
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
	var maxLevel, pointerNodes, dataNodes, dataItems uint64

	for {
		if len(stack) == 0 {
			return maxLevel, pointerNodes, dataNodes, float64(dataItems) / float64(dataNodes*s.numOfDataItems)
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
				//nolint:gofmt,revive // looks like a bug in linter
				for _, _ = range s.config.DataNodeAssistant.Iterator(s.config.State.Node(
					pointerNode.Pointers[pi].VolatileAddress,
				)) {
					dataItems++
				}
			case types.StatePointer:
				stack = append(stack, pointerNode.Pointers[pi].VolatileAddress)
				levels[pointerNode.Pointers[pi].VolatileAddress] = level
			}
		}
	}
}

func (s *Space[K, V]) valueExists(v *Entry[K, V], hashBuff []byte, hashMatches []uint64) bool {
	detectUpdate(v)

	s.find(v, hashBuff, hashMatches)

	return v.exists
}

func (s *Space[K, V]) readValue(v *Entry[K, V], hashBuff []byte, hashMatches []uint64) V {
	detectUpdate(v)

	s.find(v, hashBuff, hashMatches)

	if v.keyHashP == nil || *v.keyHashP == 0 {
		return s.defaultValue
	}

	return v.itemP.Value
}

func (s *Space[K, V]) deleteValue(
	tx *pipeline.TransactionRequest,
	walRecorder *wal.Recorder,
	v *Entry[K, V],
	hashBuff []byte,
	hashMatches []uint64,
) error {
	detectUpdate(v)

	s.find(v, hashBuff, hashMatches)

	switch {
	case v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.State == types.StateFree:
	case v.keyHashP == nil || *v.keyHashP == 0:
	default:
		// If we are here it means `s.find` found the slot with matching key so don't need to check hash and key again.

		*v.keyHashP = 0

		if _, err := walRecorder.Set8(tx, unsafe.Pointer(v.keyHashP)); err != nil {
			return err
		}

		tx.AddStoreRequest(&v.storeRequest)
	}

	return nil
}

func (s *Space[K, V]) setValue(
	tx *pipeline.TransactionRequest,
	walRecorder *wal.Recorder,
	v *Entry[K, V],
	value V,
	pool *alloc.Pool[types.VolatileAddress],
	hashBuff []byte,
	hashMatches []uint64,
) error {
	v.item.Value = value

	detectUpdate(v)

	return s.set(tx, walRecorder, v, pool, hashBuff, hashMatches)
}

func (s *Space[K, V]) find(v *Entry[K, V], hashBuff []byte, hashMatches []uint64) {
	s.walkPointers(v, hashBuff)

	if v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.State != types.StateData {
		v.keyHashP = nil
		v.itemP = nil
		v.exists = false

		return
	}

	s.walkDataItems(v, hashMatches)
}

func (s *Space[K, V]) set(
	tx *pipeline.TransactionRequest,
	walRecorder *wal.Recorder,
	v *Entry[K, V],
	pool *alloc.Pool[types.VolatileAddress],
	hashBuff []byte,
	hashMatches []uint64,
) error {
	s.walkPointers(v, hashBuff)

	if v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.State == types.StateFree {
		dataNodeAddress, err := pool.Allocate()
		if err != nil {
			return err
		}

		v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.VolatileAddress = dataNodeAddress
		v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.State = types.StateData
	}

	// Starting from here the data node is allocated.

	conflict := s.walkDataItems(v, hashMatches)

	//nolint:nestif
	if v.keyHashP != nil {
		if *v.keyHashP == 0 {
			*v.keyHashP = v.keyHash
			*v.itemP = v.item

			if _, err := walRecorder.Set8(tx, unsafe.Pointer(v.keyHashP)); err != nil {
				return err
			}
			if _, err := walRecorder.Set(tx, unsafe.Pointer(v.itemP), unsafe.Sizeof(v.item)); err != nil {
				return err
			}
		} else {
			v.itemP.Value = v.item.Value

			if _, err := walRecorder.Set(tx, unsafe.Pointer(&v.itemP.Value), unsafe.Sizeof(v.item.Value)); err != nil {
				return err
			}
		}

		tx.AddStoreRequest(&v.storeRequest)

		return nil
	}

	// Try to split data node.
	//nolint:nestif
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

			if err := s.splitDataNode(
				tx,
				walRecorder,
				v.parentIndex,
				newIndex,
				mask,
				v.storeRequest.Store[v.storeRequest.PointersToStore-2].Pointer.VolatileAddress,
				v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.VolatileAddress,
				newDataNodeAddress,
				v.level,
			); err != nil {
				return err
			}

			// This must be done because the item to set might go to the newly created data node.
			v.storeRequest.PointersToStore--
			v.level--

			return s.set(tx, walRecorder, v, pool, hashBuff, hashMatches)
		}
	}

	// Add pointer node.
	if err := s.addPointerNode(tx, walRecorder, v, conflict, pool); err != nil {
		return err
	}

	return s.set(tx, walRecorder, v, pool, hashBuff, hashMatches)
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
	walRecorder *wal.Recorder,
	index uint64,
	newIndex uint64,
	mask uint64,
	parentNodeAddress types.VolatileAddress,
	existingNodeAddress, newNodeAddress types.VolatileAddress,
	level uint8,
) error {
	parentNode := ProjectPointerNode(s.config.State.Node(parentNodeAddress))
	newDataNode := s.config.State.Node(newNodeAddress)

	node := s.config.State.Node(existingNodeAddress)
	keyHashes := s.config.DataNodeAssistant.KeyHashes(node)
	newKeyHashes := s.config.DataNodeAssistant.KeyHashes(newDataNode)
	for i, item := range s.config.DataNodeAssistant.Iterator(node) {
		itemIndex := PointerIndex(keyHashes[i], level-1)
		if itemIndex&mask != newIndex {
			continue
		}

		newDataNodeItem := s.config.DataNodeAssistant.Item(newDataNode, s.config.DataNodeAssistant.ItemOffset(i))
		*newDataNodeItem = *item
		newKeyHashes[i] = keyHashes[i]
		keyHashes[i] = 0

		if _, err := walRecorder.Set(tx, unsafe.Pointer(newDataNodeItem),
			unsafe.Sizeof(types.DataItem[K, V]{})); err != nil {
			return err
		}
		if _, err := walRecorder.Set8(tx, unsafe.Pointer(&newKeyHashes[i])); err != nil {
			return err
		}
		if _, err := walRecorder.Set8(tx, unsafe.Pointer(&keyHashes[i])); err != nil {
			return err
		}
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
		PointersToStore: 2,
		NoSnapshots:     s.config.NoSnapshots,
	})

	return nil
}

func (s *Space[K, V]) addPointerNode(
	tx *pipeline.TransactionRequest,
	walRecorder *wal.Recorder,
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

	dataPointer := v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer
	pointerNode := ProjectPointerNode(s.config.State.Node(pointerNodeAddress))
	pointerNode.Pointers[0] = types.Pointer{
		VolatileAddress: dataPointer.VolatileAddress,
		State:           types.StateData,
	}

	if _, err := walRecorder.Set8(tx, unsafe.Pointer(&pointerNode.Pointers[0].VolatileAddress)); err != nil {
		return err
	}
	if _, err := walRecorder.Set1(tx, unsafe.Pointer(&pointerNode.Pointers[0].State)); err != nil {
		return err
	}

	pointerNodeRoot := &v.storeRequest.Store[v.storeRequest.PointersToStore-1]

	pointerNodeRoot.Pointer.VolatileAddress = pointerNodeAddress
	pointerNodeRoot.Pointer.State = types.StatePointer

	if _, err := walRecorder.Set8(tx, unsafe.Pointer(&pointerNodeRoot.Pointer.VolatileAddress)); err != nil {
		return err
	}
	if _, err := walRecorder.Set1(tx, unsafe.Pointer(&pointerNodeRoot.Pointer.State)); err != nil {
		return err
	}

	if conflict {
		pointerNodeRoot.Pointer.Flags = pointerNodeRoot.Pointer.Flags.Set(types.FlagHashMod)

		if _, err := walRecorder.Set1(tx, unsafe.Pointer(&pointerNodeRoot.Pointer.Flags)); err != nil {
			return err
		}
	}

	newIndex, mask := s.splitToIndex(pointerNodeAddress, 0)

	return s.splitDataNode(
		tx,
		walRecorder,
		0,
		newIndex,
		mask,
		pointerNodeAddress,
		pointerNode.Pointers[0].VolatileAddress,
		newDataNodeAddress,
		v.level+1,
	)
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

func (s *Space[K, V]) walkPointers(v *Entry[K, V], hashBuff []byte) {
	for v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.State == types.StatePointer {
		if v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.Flags.IsSet(types.FlagHashMod) {
			v.keyHash = hashKey(&v.item.Key, hashBuff, v.level)
		}

		pointerNode := ProjectPointerNode(s.config.State.Node(
			v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.VolatileAddress,
		))
		index := PointerIndex(v.keyHash, v.level)
		state := pointerNode.Pointers[index].State
		nextIndex := index
		originalIndex := index

		switch state {
		case types.StateFree:
			hops := pointerHops[index]
			hopStart := 0
			hopEnd := len(hops)

			var dataFound bool
			for hopEnd > hopStart {
				hopIndex := (hopEnd-hopStart)/2 + hopStart
				newIndex := hops[hopIndex]

				switch pointerNode.Pointers[newIndex].State {
				case types.StateFree:
					if !dataFound {
						index = newIndex
						if hopIndex == 0 {
							nextIndex = originalIndex
						} else {
							nextIndex = hops[hopIndex-1]
						}
					}
					hopStart = hopIndex + 1
				case types.StateData:
					index = newIndex
					if hopIndex == 0 {
						nextIndex = originalIndex
					} else {
						nextIndex = hops[hopIndex-1]
					}
					hopEnd = hopIndex
					dataFound = true
				case types.StatePointer:
					hopEnd = hopIndex
				}
			}
		case types.StatePointer:
		case types.StateData:
		default:
			return
		}

		v.level++
		v.storeRequest.Store[v.storeRequest.PointersToStore].Hash = &pointerNode.Hashes[index]
		v.storeRequest.Store[v.storeRequest.PointersToStore].Pointer = &pointerNode.Pointers[index]
		v.storeRequest.PointersToStore++
		v.parentIndex = index
		if nextIndex == index {
			v.nextDataNodeState = lo.ToPtr(types.StateFree)
		} else {
			v.nextDataNodeState = &pointerNode.Pointers[nextIndex].State
		}
	}
}

func (s *Space[K, V]) walkDataItems(v *Entry[K, V], hashMatches []uint64) bool {
	if v.keyHashP != nil {
		return false
	}

	v.exists = false

	var conflict bool
	node := s.config.State.Node(v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.VolatileAddress)
	keyHashes := s.config.DataNodeAssistant.KeyHashes(node)

	zeroIndex, numOfMatches := compare.Compare(uint64(v.keyHash), (*uint64)(&keyHashes[0]), &hashMatches[0],
		s.numOfDataItems)
	for i := range numOfMatches {
		index := hashMatches[i]
		item := s.config.DataNodeAssistant.Item(node, s.config.DataNodeAssistant.ItemOffset(index))
		if item.Key == v.item.Key {
			v.exists = true
			v.keyHashP = &keyHashes[index]
			v.itemP = item

			return conflict
		}
		conflict = true
	}

	if zeroIndex == math.MaxUint64 {
		return conflict
	}

	v.keyHashP = &keyHashes[zeroIndex]
	v.itemP = s.config.DataNodeAssistant.Item(node, s.config.DataNodeAssistant.ItemOffset(zeroIndex))

	return conflict
}

// Entry represents entry in the space.
type Entry[K, V comparable] struct {
	space             *Space[K, V]
	nextDataNodeState *types.State
	storeRequest      pipeline.StoreRequest

	itemP         *types.DataItem[K, V]
	keyHashP      *types.KeyHash
	keyHash       types.KeyHash
	item          types.DataItem[K, V]
	parentIndex   uint64
	dataItemIndex uint64
	exists        bool
	level         uint8
}

// Value returns the value from entry.
func (v *Entry[K, V]) Value(hashBuff []byte, hashMatches []uint64) V {
	return v.space.readValue(v, hashBuff, hashMatches)
}

// Key returns the key from entry.
func (v *Entry[K, V]) Key() K {
	return v.item.Key
}

// Exists returns true if entry exists in the space.
func (v *Entry[K, V]) Exists(hashBuff []byte, hashMatches []uint64) bool {
	return v.space.valueExists(v, hashBuff, hashMatches)
}

// Set sts value for entry.
func (v *Entry[K, V]) Set(
	value V,
	tx *pipeline.TransactionRequest,
	walRecorder *wal.Recorder,
	pool *alloc.Pool[types.VolatileAddress],
	hashBuff []byte,
	hashMatches []uint64,
) error {
	return v.space.setValue(tx, walRecorder, v, value, pool, hashBuff, hashMatches)
}

// Delete deletes the entry.
func (v *Entry[K, V]) Delete(
	tx *pipeline.TransactionRequest,
	walRecorder *wal.Recorder,
	hashBuff []byte,
	hashMatches []uint64,
) error {
	return v.space.deleteValue(tx, walRecorder, v, hashBuff, hashMatches)
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

	if hash == 0 {
		hash = 1 // FIXME (wojciech): Do sth smarter here.
	}

	if types.IsTesting {
		hash = testHash(hash)
	}

	return hash
}

func testHash(hash types.KeyHash) types.KeyHash {
	return hash & 0x7fffffff
}

func dataItemIndex(keyHash types.KeyHash, numOfDataItems uint64) uint64 {
	return (bits.RotateLeft64(uint64(keyHash), types.UInt64Length/2) ^ uint64(keyHash)) % numOfDataItems
}

func detectUpdate[K, V comparable](v *Entry[K, V]) {
	switch {
	case *v.nextDataNodeState != types.StateFree:
		v.storeRequest.PointersToStore--
		v.level--

		v.keyHashP = nil
		v.itemP = nil
	case v.keyHashP != nil && (v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.State != types.StateData ||
		(*v.keyHashP != 0 && (*v.keyHashP != v.keyHash || v.itemP.Key != v.item.Key))):
		v.keyHashP = nil
		v.itemP = nil
	}
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
