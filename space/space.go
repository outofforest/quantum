package space

import (
	"math"
	"math/bits"
	"sync/atomic"
	"unsafe"

	"github.com/cespare/xxhash"

	"github.com/outofforest/photon"
	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/pipeline"
	"github.com/outofforest/quantum/space/compare"
	"github.com/outofforest/quantum/types"
)

// Stage constants.
const (
	StagePointer0 uint8 = iota
	StagePointer1
	StageData
)

// Config stores space configuration.
type Config[K, V comparable] struct {
	SpaceRoot         types.NodeRoot
	State             *alloc.State
	DataNodeAssistant *DataNodeAssistant[K, V]
	DeletionCounter   *uint64
	NoSnapshots       bool
}

// New creates new space.
func New[K, V comparable](config Config[K, V]) *Space[K, V] {
	s := &Space[K, V]{
		config:         config,
		numOfDataItems: config.DataNodeAssistant.NumOfItems(),
	}

	defaultInit := Entry[K, V]{
		space: s,
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
func (s *Space[K, V]) Find(
	v *Entry[K, V],
	key K,
	stage uint8,
	hashBuff []byte,
	hashMatches []uint64,
) {
	if v.space == nil {
		keyHash := hashKey(&key, nil, 0)
		s.initEntry(v, key, keyHash, stage)
	}

	s.find(v, hashBuff, hashMatches, hashKey)
}

// Query queries the key.
func (s *Space[K, V]) Query(key K, hashBuff []byte, hashMatches []uint64) (V, bool) {
	keyHash := hashKey(&key, nil, 0)
	return s.query(key, keyHash, hashBuff, hashMatches, hashKey)
}

// Stats returns space-related statistics.
func (s *Space[K, V]) Stats() (uint64, uint64, uint64, float64) {
	switch {
	case isFree(s.config.SpaceRoot.Pointer.VolatileAddress):
		return 0, 0, 0, 0
	case !isPointer(s.config.SpaceRoot.Pointer.VolatileAddress):
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
			volatileAddress := types.Load(&pointerNode.Pointers[pi].VolatileAddress)
			switch {
			case isFree(volatileAddress):
			case isPointer(volatileAddress):
				stack = append(stack, volatileAddress)
				levels[volatileAddress] = level
			default:
				dataNodes++
				if level > maxLevel {
					maxLevel = level
				}
				for _, kh := range s.config.DataNodeAssistant.KeyHashes(s.config.State.Node(volatileAddress)) {
					if kh != 0 {
						dataItems++
					}
				}
			}
		}
	}
}

func (s *Space[K, V]) query(
	key K,
	keyHash types.KeyHash,
	hashBuff []byte,
	hashMatches []uint64,
	hashKeyFunc func(key *K, buff []byte, level uint8) types.KeyHash,
) (V, bool) {
	volatileAddress := s.config.SpaceRoot.Pointer.VolatileAddress
	var level uint8

	for isPointer(volatileAddress) {
		if volatileAddress.IsSet(flagHashMod) {
			keyHash = hashKeyFunc(&key, hashBuff, level)
		}

		pointerNode := ProjectPointerNode(s.config.State.Node(volatileAddress))
		index, _ := reducePointerSlot(pointerNode, PointerIndex(keyHash, level))

		level++
		volatileAddress = pointerNode.Pointers[index].VolatileAddress
	}

	if isFree(volatileAddress) {
		return s.defaultValue, false
	}

	node := s.config.State.Node(volatileAddress)
	keyHashes := s.config.DataNodeAssistant.KeyHashes(node)

	_, numOfMatches := compare.Compare(uint64(keyHash), (*uint64)(&keyHashes[0]), &hashMatches[0],
		s.numOfDataItems)
	for i := range numOfMatches {
		index := hashMatches[i]
		item := s.config.DataNodeAssistant.Item(node, s.config.DataNodeAssistant.ItemOffset(index))
		if item.Key == key {
			return item.Value, true
		}
	}

	return s.defaultValue, false
}

func (s *Space[K, V]) initEntry(
	v *Entry[K, V],
	key K,
	keyHash types.KeyHash,
	stage uint8,
) {
	initBytes := unsafe.Slice((*byte)(unsafe.Pointer(v)), s.initSize)
	copy(initBytes, s.defaultInit)
	v.keyHash = keyHash
	v.item.Key = key
	v.stage = stage
}

func (s *Space[K, V]) keyExists(
	v *Entry[K, V],
	hashBuff []byte,
	hashMatches []uint64,
	hashKeyFunc func(key *K, buff []byte, level uint8) types.KeyHash,
) bool {
	s.detectUpdate(v)

	s.find(v, hashBuff, hashMatches, hashKeyFunc)

	return v.exists
}

func (s *Space[K, V]) readKey(
	v *Entry[K, V],
	hashBuff []byte,
	hashMatches []uint64,
	hashKeyFunc func(key *K, buff []byte, level uint8) types.KeyHash,
) V {
	s.detectUpdate(v)

	s.find(v, hashBuff, hashMatches, hashKeyFunc)

	if v.keyHashP == nil || *v.keyHashP == 0 {
		return s.defaultValue
	}

	return v.itemP.Value
}

func (s *Space[K, V]) deleteKey(
	v *Entry[K, V],
	tx *pipeline.TransactionRequest,
	hashBuff []byte,
	hashMatches []uint64,
	hashKeyFunc func(key *K, buff []byte, level uint8) types.KeyHash,
) {
	s.detectUpdate(v)

	s.find(v, hashBuff, hashMatches, hashKeyFunc)

	if v.keyHashP != nil && *v.keyHashP != 0 {
		// If we are here it means `s.find` found the slot with matching key so don't need to check hash and key again.
		*v.keyHashP = 0
		v.exists = false

		atomic.AddUint64(s.config.DeletionCounter, 1)

		tx.AddStoreRequest(&v.storeRequest)
	}
}

func (s *Space[K, V]) setKey(
	v *Entry[K, V],
	tx *pipeline.TransactionRequest,
	allocator *alloc.Allocator[types.VolatileAddress],
	value V,
	hashBuff []byte,
	hashMatches []uint64,
	hashKeyFunc func(key *K, buff []byte, level uint8) types.KeyHash,
) error {
	v.item.Value = value

	s.detectUpdate(v)

	return s.set(v, tx, allocator, hashBuff, hashMatches, hashKeyFunc)
}

func (s *Space[K, V]) find(
	v *Entry[K, V],
	hashBuff []byte,
	hashMatches []uint64,
	hashKeyFunc func(key *K, buff []byte, level uint8) types.KeyHash,
) {
	s.walkPointers(v, hashBuff, hashKeyFunc)

	switch {
	case v.stage == StageData:
	case v.stage == StagePointer0:
		v.stage = StagePointer1
		return
	case v.stage == StagePointer1:
		v.stage = StageData
		return
	}

	volatileAddress := types.Load(&v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.VolatileAddress)
	if !isData(volatileAddress) {
		v.keyHashP = nil
		v.itemP = nil
		v.exists = false

		return
	}

	s.walkDataItems(v, hashMatches)
}

func (s *Space[K, V]) set(
	v *Entry[K, V],
	tx *pipeline.TransactionRequest,
	allocator *alloc.Allocator[types.VolatileAddress],
	hashBuff []byte,
	hashMatches []uint64,
	hashKeyFunc func(key *K, buff []byte, level uint8) types.KeyHash,
) error {
	s.walkPointers(v, hashBuff, hashKeyFunc)

	volatileAddress := types.Load(&v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.VolatileAddress)

	if isFree(volatileAddress) {
		dataNodeVolatileAddress, err := allocator.Allocate()
		if err != nil {
			return err
		}
		s.config.State.Clear(dataNodeVolatileAddress)

		types.Store(&v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.VolatileAddress,
			dataNodeVolatileAddress)
	}

	// Starting from here the data node is allocated.

	conflict := s.walkDataItems(v, hashMatches)

	if v.keyHashP != nil {
		if *v.keyHashP == 0 {
			*v.keyHashP = v.keyHash
			*v.itemP = v.item
		} else {
			v.itemP.Value = v.item.Value
		}
		v.exists = true

		tx.AddStoreRequest(&v.storeRequest)

		return nil
	}

	// Try to split data node.
	if v.storeRequest.PointersToStore > 1 {
		splitDone, err := s.splitDataNodeWithoutConflict(tx, allocator, v.parentIndex,
			v.storeRequest.Store[v.storeRequest.PointersToStore-2].Pointer, v.level)
		if err != nil {
			return err
		}
		if splitDone {
			// This must be done because the item to set might go to the newly created data node.
			v.storeRequest.PointersToStore--
			v.level--
			v.nextDataNode = nil

			return s.set(v, tx, allocator, hashBuff, hashMatches, hashKeyFunc)
		}
	}

	// Add pointer node.
	if err := s.addPointerNode(v, tx, allocator, conflict, hashBuff, hashKeyFunc); err != nil {
		return err
	}

	return s.set(v, tx, allocator, hashBuff, hashMatches, hashKeyFunc)
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
		if isFree(types.Load(&parentNode.Pointers[newIndex].VolatileAddress)) {
			index = newIndex
			break
		}
	}

	return index, uint64(math.MaxUint64) << bits.TrailingZeros64(mask)
}

func (s *Space[K, V]) splitDataNodeWithoutConflict(
	tx *pipeline.TransactionRequest,
	allocator *alloc.Allocator[types.VolatileAddress],
	index uint64,
	parentNodePointer *types.Pointer,
	level uint8,
) (bool, error) {
	newIndex, mask := s.splitToIndex(types.Load(&parentNodePointer.VolatileAddress), index)
	if newIndex == index {
		return false, nil
	}

	newNodeVolatileAddress, err := allocator.Allocate()
	if err != nil {
		return false, err
	}
	s.config.State.Clear(newNodeVolatileAddress)

	parentNode := ProjectPointerNode(s.config.State.Node(types.Load(&parentNodePointer.VolatileAddress)))
	existingNodePointer := &parentNode.Pointers[index]
	existingDataNode := s.config.State.Node(types.Load(&existingNodePointer.VolatileAddress))
	newDataNode := s.config.State.Node(newNodeVolatileAddress)
	keyHashes := s.config.DataNodeAssistant.KeyHashes(existingDataNode)
	newKeyHashes := s.config.DataNodeAssistant.KeyHashes(newDataNode)

	for i, kh := range keyHashes {
		if kh == 0 {
			continue
		}

		itemIndex := PointerIndex(kh, level-1)
		if itemIndex&mask != newIndex {
			continue
		}

		offset := s.config.DataNodeAssistant.ItemOffset(uint64(i))
		*s.config.DataNodeAssistant.Item(newDataNode, offset) = *s.config.DataNodeAssistant.Item(existingDataNode, offset)
		newKeyHashes[i] = kh
		keyHashes[i] = 0
	}

	types.Store(&parentNode.Pointers[newIndex].VolatileAddress, newNodeVolatileAddress)

	tx.AddStoreRequest(&pipeline.StoreRequest{
		Store: [pipeline.StoreCapacity]types.NodeRoot{
			{
				Hash:    &parentNode.Hashes[index],
				Pointer: &parentNode.Pointers[index],
			},
		},
		PointersToStore: 1,
		NoSnapshots:     s.config.NoSnapshots,
	})
	tx.AddStoreRequest(&pipeline.StoreRequest{
		Store: [pipeline.StoreCapacity]types.NodeRoot{
			{
				Hash:    &parentNode.Hashes[newIndex],
				Pointer: &parentNode.Pointers[newIndex],
			},
		},
		PointersToStore: 1,
		NoSnapshots:     s.config.NoSnapshots,
	})

	return true, nil
}

func (s *Space[K, V]) splitDataNodeWithConflict(
	tx *pipeline.TransactionRequest,
	allocator *alloc.Allocator[types.VolatileAddress],
	index uint64,
	parentNodePointer *types.Pointer,
	level uint8,
	hashBuff []byte,
	hashKeyFunc func(key *K, buff []byte, level uint8) types.KeyHash,
) (bool, error) {
	newIndex, mask := s.splitToIndex(types.Load(&parentNodePointer.VolatileAddress), index)
	if newIndex == index {
		return false, nil
	}

	newNodeVolatileAddress, err := allocator.Allocate()
	if err != nil {
		return false, err
	}
	s.config.State.Clear(newNodeVolatileAddress)

	parentNode := ProjectPointerNode(s.config.State.Node(types.Load(&parentNodePointer.VolatileAddress)))
	existingNodePointer := &parentNode.Pointers[index]
	existingDataNode := s.config.State.Node(types.Load(&existingNodePointer.VolatileAddress))
	newDataNode := s.config.State.Node(newNodeVolatileAddress)
	keyHashes := s.config.DataNodeAssistant.KeyHashes(existingDataNode)
	newKeyHashes := s.config.DataNodeAssistant.KeyHashes(newDataNode)

	for i, kh := range keyHashes {
		if kh == 0 {
			continue
		}

		offset := s.config.DataNodeAssistant.ItemOffset(uint64(i))
		item := s.config.DataNodeAssistant.Item(existingDataNode, offset)

		kh = hashKeyFunc(&item.Key, hashBuff, level-1)
		itemIndex := PointerIndex(kh, level-1)
		if itemIndex&mask != newIndex {
			keyHashes[i] = kh
			continue
		}

		*s.config.DataNodeAssistant.Item(newDataNode, offset) = *item
		newKeyHashes[i] = kh
		keyHashes[i] = 0
	}

	types.Store(&parentNode.Pointers[newIndex].VolatileAddress, newNodeVolatileAddress)

	tx.AddStoreRequest(&pipeline.StoreRequest{
		Store: [pipeline.StoreCapacity]types.NodeRoot{
			{
				Hash:    &parentNode.Hashes[index],
				Pointer: &parentNode.Pointers[index],
			},
		},
		PointersToStore: 1,
		NoSnapshots:     s.config.NoSnapshots,
	})
	tx.AddStoreRequest(&pipeline.StoreRequest{
		Store: [pipeline.StoreCapacity]types.NodeRoot{
			{
				Hash:    &parentNode.Hashes[newIndex],
				Pointer: &parentNode.Pointers[newIndex],
			},
		},
		PointersToStore: 1,
		NoSnapshots:     s.config.NoSnapshots,
	})

	return true, nil
}

func (s *Space[K, V]) addPointerNode(
	v *Entry[K, V],
	tx *pipeline.TransactionRequest,
	allocator *alloc.Allocator[types.VolatileAddress],
	conflict bool,
	hashBuff []byte,
	hashKeyFunc func(key *K, buff []byte, level uint8) types.KeyHash,
) error {
	pointerNodeVolatileAddress, err := allocator.Allocate()
	if err != nil {
		return err
	}
	s.config.State.Clear(pointerNodeVolatileAddress)

	dataPointer := v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer
	pointerNode := ProjectPointerNode(s.config.State.Node(pointerNodeVolatileAddress))
	pointerNodeRoot := &v.storeRequest.Store[v.storeRequest.PointersToStore-1]

	pointerNode.Pointers[0].SnapshotID = dataPointer.SnapshotID
	pointerNode.Pointers[0].PersistentAddress = dataPointer.PersistentAddress
	types.Store(&pointerNode.Pointers[0].VolatileAddress, dataPointer.VolatileAddress)

	pointerNodeVolatileAddress = pointerNodeVolatileAddress.Set(flagPointerNode)
	if conflict {
		pointerNodeVolatileAddress = pointerNodeVolatileAddress.Set(flagHashMod)
	}
	types.Store(&pointerNodeRoot.Pointer.VolatileAddress, pointerNodeVolatileAddress)

	if conflict {
		_, err = s.splitDataNodeWithConflict(tx, allocator, 0, pointerNodeRoot.Pointer,
			v.level+1, hashBuff, hashKeyFunc)
	} else {
		_, err = s.splitDataNodeWithoutConflict(tx, allocator, 0, pointerNodeRoot.Pointer, v.level+1)
	}
	return err
}

func (s *Space[K, V]) walkPointers(
	v *Entry[K, V],
	hashBuff []byte,
	hashKeyFunc func(key *K, buff []byte, level uint8) types.KeyHash,
) {
	if v.nextDataNode != nil {
		if isFree(types.Load(v.nextDataNode)) {
			return
		}

		v.storeRequest.PointersToStore--
		v.level--
		v.nextDataNode = nil

		v.keyHashP = nil
		v.itemP = nil
	}

	for {
		if !s.walkOnePointer(v, hashBuff, hashKeyFunc) {
			return
		}
	}
}

func (s *Space[K, V]) walkOnePointer(
	v *Entry[K, V],
	hashBuff []byte,
	hashKeyFunc func(key *K, buff []byte, level uint8) types.KeyHash,
) bool {
	volatileAddress := types.Load(&v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.VolatileAddress)
	if !isPointer(volatileAddress) {
		return false
	}
	if volatileAddress.IsSet(flagHashMod) {
		v.keyHash = hashKeyFunc(&v.item.Key, hashBuff, v.level)
	}

	pointerNode := ProjectPointerNode(s.config.State.Node(volatileAddress))
	index, nextIndex := reducePointerSlot(pointerNode, PointerIndex(v.keyHash, v.level))

	v.level++
	v.storeRequest.Store[v.storeRequest.PointersToStore].Hash = &pointerNode.Hashes[index]
	v.storeRequest.Store[v.storeRequest.PointersToStore].Pointer = &pointerNode.Pointers[index]
	v.storeRequest.PointersToStore++
	v.parentIndex = index
	if nextIndex != index {
		v.nextDataNode = &pointerNode.Pointers[nextIndex].VolatileAddress

		// If we are here it means that we should stop following potential next pointer nodes because we are in the
		// non-final branch. A better data node might be added concurrently at any time.
		return false
	}

	if v.stage == StagePointer0 && v.level == 3 {
		return false
	}

	return true
}

func (s *Space[K, V]) walkDataItems(v *Entry[K, V], hashMatches []uint64) bool {
	if v.keyHashP != nil {
		return false
	}

	v.exists = false

	var conflict bool
	node := s.config.State.Node(types.Load(
		&v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.VolatileAddress))
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
			v.deletionCounter = atomic.LoadUint64(s.config.DeletionCounter)

			return conflict
		}
		conflict = true
	}

	if zeroIndex == math.MaxUint64 {
		return conflict
	}

	v.keyHashP = &keyHashes[zeroIndex]
	v.itemP = s.config.DataNodeAssistant.Item(node, s.config.DataNodeAssistant.ItemOffset(zeroIndex))
	v.deletionCounter = atomic.LoadUint64(s.config.DeletionCounter)

	return conflict
}

func (s *Space[K, V]) detectUpdate(v *Entry[K, V]) {
	v.stage = StageData

	if v.keyHashP == nil {
		return
	}

	if isPointer(types.Load(&v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.VolatileAddress)) ||
		*s.config.DeletionCounter != v.deletionCounter ||
		(*v.keyHashP != 0 && (*v.keyHashP != v.keyHash || v.itemP.Key != v.item.Key)) {
		v.keyHashP = nil
		v.itemP = nil
	}
}

// Entry represents entry in the space.
type Entry[K, V comparable] struct {
	space        *Space[K, V]
	storeRequest pipeline.StoreRequest

	itemP           *DataItem[K, V]
	keyHashP        *types.KeyHash
	keyHash         types.KeyHash
	item            DataItem[K, V]
	parentIndex     uint64
	dataItemIndex   uint64
	nextDataNode    *types.VolatileAddress
	deletionCounter uint64
	exists          bool
	stage           uint8
	level           uint8
}

// Value returns the value from entry.
func (v *Entry[K, V]) Value(
	hashBuff []byte,
	hashMatches []uint64,
) V {
	return v.space.readKey(v, hashBuff, hashMatches, hashKey[K])
}

// Key returns the key from entry.
func (v *Entry[K, V]) Key() K {
	return v.item.Key
}

// Exists returns true if entry exists in the space.
func (v *Entry[K, V]) Exists(
	hashBuff []byte,
	hashMatches []uint64,
) bool {
	return v.space.keyExists(v, hashBuff, hashMatches, hashKey[K])
}

// Set sts value for entry.
func (v *Entry[K, V]) Set(
	tx *pipeline.TransactionRequest,
	allocator *alloc.Allocator[types.VolatileAddress],
	value V,
	hashBuff []byte,
	hashMatches []uint64,
) error {
	return v.space.setKey(v, tx, allocator, value, hashBuff, hashMatches, hashKey[K])
}

// Delete deletes the entry.
func (v *Entry[K, V]) Delete(
	tx *pipeline.TransactionRequest,
	hashBuff []byte,
	hashMatches []uint64,
) {
	v.space.deleteKey(v, tx, hashBuff, hashMatches, hashKey[K])
}

// IteratorAndDeallocator iterates over items and deallocates space.
func IteratorAndDeallocator[K, V comparable](
	spaceRoot types.Pointer,
	state *alloc.State,
	dataNodeAssistant *DataNodeAssistant[K, V],
	volatileDeallocator *alloc.Deallocator[types.VolatileAddress],
	persistentDeallocator *alloc.Deallocator[types.PersistentAddress],
) func(func(item *DataItem[K, V]) bool) {
	return func(yield func(item *DataItem[K, V]) bool) {
		if isFree(spaceRoot.VolatileAddress) {
			return
		}

		// FIXME (wojciech): What if tree is deeper?
		stack := [pipeline.StoreCapacity * NumOfPointers]types.Pointer{
			spaceRoot,
		}
		stackCount := 1

		for {
			if stackCount == 0 {
				return
			}

			stackCount--
			pointer := stack[stackCount]

			// It is safe to do deallocations here because nodes are not reallocated until commit is finalized.
			volatileDeallocator.Deallocate(pointer.VolatileAddress.Naked())
			persistentDeallocator.Deallocate(pointer.PersistentAddress)

			switch {
			case isPointer(pointer.VolatileAddress):
				pointerNode := ProjectPointerNode(state.Node(pointer.VolatileAddress))
				for pi := range pointerNode.Pointers {
					if isFree(pointerNode.Pointers[pi].VolatileAddress) {
						continue
					}

					stack[stackCount] = pointerNode.Pointers[pi]
					stackCount++
				}
			case !isFree(pointer.VolatileAddress):
				n := state.Node(pointer.VolatileAddress)
				keyHashes := dataNodeAssistant.KeyHashes(n)

				for i, kh := range keyHashes {
					if kh != 0 && !yield(dataNodeAssistant.Item(n, dataNodeAssistant.ItemOffset(uint64(i)))) {
						return
					}
				}
			}
		}
	}
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

	return hash
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

func reducePointerSlot(pointerNode *PointerNode, index uint64) (uint64, uint64) {
	if !isFree(types.Load(&pointerNode.Pointers[index].VolatileAddress)) {
		return index, index
	}

	originalIndex := index
	nextIndex := index

	hops := pointerHops[index]
	hopStart := 0
	hopEnd := len(hops)

	var dataFound bool
	for hopEnd > hopStart {
		hopIndex := (hopEnd-hopStart)/2 + hopStart
		newIndex := hops[hopIndex]

		volatileAddress := types.Load(&pointerNode.Pointers[newIndex].VolatileAddress)
		switch {
		case isFree(volatileAddress):
			if !dataFound {
				index = newIndex
				if hopIndex == 0 {
					nextIndex = originalIndex
				} else {
					nextIndex = hops[hopIndex-1]
				}
			}
			hopStart = hopIndex + 1
		case isPointer(volatileAddress):
			hopEnd = hopIndex
		default:
			index = newIndex
			if hopIndex == 0 {
				nextIndex = originalIndex
			} else {
				nextIndex = hops[hopIndex-1]
			}
			hopEnd = hopIndex
			dataFound = true
		}
	}

	return index, nextIndex
}
