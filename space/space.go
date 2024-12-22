package space

import (
	"math"
	"math/bits"
	"sync/atomic"
	"unsafe"

	"github.com/cespare/xxhash"

	"github.com/outofforest/photon"
	"github.com/outofforest/quantum/pipeline"
	"github.com/outofforest/quantum/space/compare"
	"github.com/outofforest/quantum/state"
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
	State             *state.State
	DataNodeAssistant *DataNodeAssistant[K, V]
	DeletionCounter   *uint64
	NoSnapshots       bool
}

// New creates new space.
func New[K, V comparable](config Config[K, V]) *Space[K, V] {
	var k K

	s := &Space[K, V]{
		config:      config,
		hashKeyFunc: hashKey[K],
		hashBuff:    make([]byte, unsafe.Sizeof(k)+1),
		hashMatches: make([]uint64, config.DataNodeAssistant.NumOfItems()),
	}

	defaultInit := Entry[K, V]{
		storeRequest: pipeline.StoreRequest{
			NoSnapshots:     s.config.NoSnapshots,
			PointersToStore: 1,
			Store: [pipeline.StoreCapacity]types.ToStore{
				{
					Hash:    s.config.SpaceRoot.Hash,
					Pointer: s.config.SpaceRoot.Pointer,
				},
			},
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
	config       Config[K, V]
	initSize     uint64
	defaultInit  []byte
	defaultValue V
	hashKeyFunc  func(key *K, buff []byte, level uint8) types.KeyHash
	hashBuff     []byte
	hashMatches  []uint64
}

// Find locates key in the space.
func (s *Space[K, V]) Find(v *Entry[K, V], key K, stage uint8) {
	if v.keyHash == 0 {
		// Hash is computed here using hashKey function, not s.hashKeyFunc. It's because calling it using variable
		// causes weird allocation on every call, and here we don't need this functionality for testing.
		keyHash := hashKey(&key, nil, 0)
		s.initEntry(v, key, keyHash, stage)
	}

	s.find(v, load(&v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.VolatileAddress))
}

// Query queries the key.
func (s *Space[K, V]) Query(key K) (V, bool) {
	// Hash is computed here using hashKey function, not s.hashKeyFunc. It's because calling it using variable
	// causes weird allocation on every call, and here we don't need this functionality for testing.
	keyHash := hashKey(&key, nil, 0)
	return s.query(key, keyHash)
}

// KeyExists checks if key exists in the space.
func (s *Space[K, V]) KeyExists(
	v *Entry[K, V],
) bool {
	volatileAddress := v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.VolatileAddress
	s.detectUpdate(v, volatileAddress)

	s.find(v, volatileAddress)

	return v.exists
}

// ReadKey retrieves value of a key.
func (s *Space[K, V]) ReadKey(
	v *Entry[K, V],
) V {
	volatileAddress := v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.VolatileAddress
	s.detectUpdate(v, volatileAddress)

	s.find(v, volatileAddress)

	if v.keyHashP == nil || *v.keyHashP == 0 {
		return s.defaultValue
	}

	return v.itemP.Value
}

// DeleteKey deletes the key from the space.
func (s *Space[K, V]) DeleteKey(
	v *Entry[K, V],
	tx *pipeline.TransactionRequest,
) {
	volatileAddress := v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.VolatileAddress
	s.detectUpdate(v, volatileAddress)

	s.find(v, volatileAddress)

	if v.keyHashP != nil && *v.keyHashP != 0 {
		// If we are here it means `s.find` found the slot with matching key so don't need to check hash and key again.
		*v.keyHashP = 0
		v.exists = false

		atomic.AddUint64(s.config.DeletionCounter, 1)

		tx.AddStoreRequest(&v.storeRequest)
	}
}

// SetKey sets key in the space.
func (s *Space[K, V]) SetKey(
	v *Entry[K, V],
	tx *pipeline.TransactionRequest,
	allocator *state.Allocator[types.VolatileAddress],
	value V,
) error {
	v.item.Value = value

	volatileAddress := v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.VolatileAddress
	s.detectUpdate(v, volatileAddress)

	return s.set(v, volatileAddress, tx, allocator)
}

// Stats returns space-related statistics.
func (s *Space[K, V]) Stats() (uint64, uint64, uint64, uint64, float64) {
	if isFree(s.config.SpaceRoot.Pointer.VolatileAddress) {
		return 0, 0, 0, 0, 0
	}

	stack := []types.VolatileAddress{s.config.SpaceRoot.Pointer.VolatileAddress}

	levels := map[types.VolatileAddress]uint64{
		s.config.SpaceRoot.Pointer.VolatileAddress: 0,
	}
	var maxLevel, pointerNodes, dataNodes, dataItems uint64

	for {
		if len(stack) == 0 {
			return maxLevel, pointerNodes, dataNodes, dataItems,
				float64(dataItems) / float64(dataNodes*uint64(len(s.hashMatches)))
		}

		n := stack[len(stack)-1]
		level := levels[n] + 1
		stack = stack[:len(stack)-1]

		switch {
		case isPointer(n):
			pointerNodes++
			pointerNode := ProjectPointerNode(s.config.State.Node(n))
			for pi := range pointerNode.Pointers {
				volatileAddress := pointerNode.Pointers[pi].VolatileAddress
				if isFree(volatileAddress) {
					continue
				}

				stack = append(stack, volatileAddress)
				levels[volatileAddress] = level
			}
		case !isFree(n):
			dataNodes++
			if level > maxLevel {
				maxLevel = level
			}
			for _, kh := range s.config.DataNodeAssistant.KeyHashes(s.config.State.Node(n)) {
				if kh != 0 {
					dataItems++
				}
			}
		}
	}
}

func (s *Space[K, V]) query(key K, keyHash types.KeyHash) (V, bool) {
	volatileAddress := s.config.SpaceRoot.Pointer.VolatileAddress
	var level uint8

	for isPointer(volatileAddress) {
		if volatileAddress.IsSet(flagHashMod) {
			keyHash = s.hashKeyFunc(&key, s.hashBuff, level)
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

	_, numOfMatches := compare.Compare(uint64(keyHash), (*uint64)(&keyHashes[0]), &s.hashMatches[0],
		uint64(len(s.hashMatches)))
	for i := range numOfMatches {
		index := s.hashMatches[i]
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
	v.storeRequest.Store[0].VolatileAddress = s.config.SpaceRoot.Pointer.VolatileAddress
	v.keyHash = keyHash
	v.item.Key = key
	v.stage = stage
}

func (s *Space[K, V]) find(v *Entry[K, V], volatileAddress types.VolatileAddress) {
	volatileAddress = s.walkPointers(v, volatileAddress)

	switch {
	case v.stage == StageData:
	case v.stage == StagePointer0:
		v.stage = StagePointer1
		return
	case v.stage == StagePointer1:
		v.stage = StageData
		return
	}

	if !isData(volatileAddress) {
		v.keyHashP = nil
		v.itemP = nil
		v.exists = false

		return
	}

	s.walkDataItems(v, volatileAddress)
}

func (s *Space[K, V]) set(
	v *Entry[K, V],
	volatileAddress types.VolatileAddress,
	tx *pipeline.TransactionRequest,
	volatileAllocator *state.Allocator[types.VolatileAddress],
) error {
	volatileAddress = s.walkPointers(v, volatileAddress)

	if isFree(volatileAddress) {
		var err error
		volatileAddress, err = volatileAllocator.Allocate()
		if err != nil {
			return err
		}
		s.config.State.Clear(volatileAddress)

		store(&v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.VolatileAddress, volatileAddress)
	}

	// Starting from here the data node is allocated.

	conflict := s.walkDataItems(v, volatileAddress)

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
		splitDone, err := s.splitDataNodeWithoutConflict(tx, volatileAllocator, v.parentIndex,
			v.storeRequest.Store[v.storeRequest.PointersToStore-2].Pointer.VolatileAddress, v.level)
		if err != nil {
			return err
		}
		if splitDone {
			// This must be done because the item to set might go to the newly created data node.
			v.storeRequest.PointersToStore--
			v.level--
			v.nextDataNode = nil

			volatileAddress = v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.VolatileAddress
			return s.set(v, volatileAddress, tx, volatileAllocator)
		}
	}

	// Add pointer node.
	volatileAddress, err := s.addPointerNode(v, tx, volatileAllocator, conflict)
	if err != nil {
		return err
	}

	return s.set(v, volatileAddress, tx, volatileAllocator)
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
		if isFree(parentNode.Pointers[newIndex].VolatileAddress) {
			index = newIndex
			break
		}
	}

	return index, uint64(math.MaxUint64) << bits.TrailingZeros64(mask)
}

func (s *Space[K, V]) splitDataNodeWithoutConflict(
	tx *pipeline.TransactionRequest,
	volatileAllocator *state.Allocator[types.VolatileAddress],
	index uint64,
	parentNodeAddress types.VolatileAddress,
	level uint8,
) (bool, error) {
	newIndex, mask := s.splitToIndex(parentNodeAddress, index)
	if newIndex == index {
		return false, nil
	}

	newNodeVolatileAddress, err := volatileAllocator.Allocate()
	if err != nil {
		return false, err
	}
	s.config.State.Clear(newNodeVolatileAddress)

	parentNode := ProjectPointerNode(s.config.State.Node(parentNodeAddress))
	existingNodePointer := &parentNode.Pointers[index]
	existingDataNode := s.config.State.Node(existingNodePointer.VolatileAddress)
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

	store(&parentNode.Pointers[newIndex].VolatileAddress, newNodeVolatileAddress)

	tx.AddStoreRequest(&pipeline.StoreRequest{
		Store: [pipeline.StoreCapacity]types.ToStore{
			{
				Hash:            &parentNode.Hashes[index],
				Pointer:         &parentNode.Pointers[index],
				VolatileAddress: parentNode.Pointers[index].VolatileAddress,
			},
		},
		PointersToStore: 1,
		NoSnapshots:     s.config.NoSnapshots,
	})
	tx.AddStoreRequest(&pipeline.StoreRequest{
		Store: [pipeline.StoreCapacity]types.ToStore{
			{
				Hash:            &parentNode.Hashes[newIndex],
				Pointer:         &parentNode.Pointers[newIndex],
				VolatileAddress: newNodeVolatileAddress,
			},
		},
		PointersToStore: 1,
		NoSnapshots:     s.config.NoSnapshots,
	})

	return true, nil
}

func (s *Space[K, V]) splitDataNodeWithConflict(
	tx *pipeline.TransactionRequest,
	volatileAllocator *state.Allocator[types.VolatileAddress],
	index uint64,
	parentNodeAddress types.VolatileAddress,
	level uint8,
) (bool, error) {
	newIndex, mask := s.splitToIndex(parentNodeAddress, index)
	if newIndex == index {
		return false, nil
	}

	newNodeVolatileAddress, err := volatileAllocator.Allocate()
	if err != nil {
		return false, err
	}
	s.config.State.Clear(newNodeVolatileAddress)

	parentNode := ProjectPointerNode(s.config.State.Node(parentNodeAddress))
	existingNodePointer := &parentNode.Pointers[index]
	existingDataNode := s.config.State.Node(existingNodePointer.VolatileAddress)
	newDataNode := s.config.State.Node(newNodeVolatileAddress)
	keyHashes := s.config.DataNodeAssistant.KeyHashes(existingDataNode)
	newKeyHashes := s.config.DataNodeAssistant.KeyHashes(newDataNode)

	for i, kh := range keyHashes {
		if kh == 0 {
			continue
		}

		offset := s.config.DataNodeAssistant.ItemOffset(uint64(i))
		item := s.config.DataNodeAssistant.Item(existingDataNode, offset)

		kh = s.hashKeyFunc(&item.Key, s.hashBuff, level-1)
		itemIndex := PointerIndex(kh, level-1)
		if itemIndex&mask != newIndex {
			keyHashes[i] = kh
			continue
		}

		*s.config.DataNodeAssistant.Item(newDataNode, offset) = *item
		newKeyHashes[i] = kh
		keyHashes[i] = 0
	}

	store(&parentNode.Pointers[newIndex].VolatileAddress, newNodeVolatileAddress)

	tx.AddStoreRequest(&pipeline.StoreRequest{
		Store: [pipeline.StoreCapacity]types.ToStore{
			{
				Hash:            &parentNode.Hashes[index],
				Pointer:         &parentNode.Pointers[index],
				VolatileAddress: parentNode.Pointers[index].VolatileAddress,
			},
		},
		PointersToStore: 1,
		NoSnapshots:     s.config.NoSnapshots,
	})
	tx.AddStoreRequest(&pipeline.StoreRequest{
		Store: [pipeline.StoreCapacity]types.ToStore{
			{
				Hash:            &parentNode.Hashes[newIndex],
				Pointer:         &parentNode.Pointers[newIndex],
				VolatileAddress: newNodeVolatileAddress,
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
	volatileAllocator *state.Allocator[types.VolatileAddress],
	conflict bool,
) (types.VolatileAddress, error) {
	pointerNodeVolatileAddress, err := volatileAllocator.Allocate()
	if err != nil {
		return 0, err
	}
	s.config.State.Clear(pointerNodeVolatileAddress)

	dataPointer := v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer
	pointerNode := ProjectPointerNode(s.config.State.Node(pointerNodeVolatileAddress))
	pointerNodeRoot := &v.storeRequest.Store[v.storeRequest.PointersToStore-1]

	pointerNode.Pointers[0].SnapshotID = dataPointer.SnapshotID
	pointerNode.Pointers[0].PersistentAddress = dataPointer.PersistentAddress
	store(&pointerNode.Pointers[0].VolatileAddress, dataPointer.VolatileAddress)

	pointerNodeVolatileAddress = pointerNodeVolatileAddress.Set(flagPointerNode)
	if conflict {
		pointerNodeVolatileAddress = pointerNodeVolatileAddress.Set(flagHashMod)
	}
	store(&pointerNodeRoot.Pointer.VolatileAddress, pointerNodeVolatileAddress)
	pointerNodeRoot.VolatileAddress = pointerNodeVolatileAddress
	pointerNodeRoot.Pointer.PersistentAddress = 0
	pointerNodeRoot.Pointer.SnapshotID = 0

	if conflict {
		_, err = s.splitDataNodeWithConflict(tx, volatileAllocator, 0, pointerNodeVolatileAddress, v.level+1)
	} else {
		_, err = s.splitDataNodeWithoutConflict(tx, volatileAllocator, 0, pointerNodeVolatileAddress, v.level+1)
	}
	if err != nil {
		return 0, err
	}
	return pointerNodeVolatileAddress, nil
}

func (s *Space[K, V]) walkPointers(v *Entry[K, V], volatileAddress types.VolatileAddress) types.VolatileAddress {
	if v.nextDataNode != nil {
		if isFree(load(v.nextDataNode)) {
			return volatileAddress
		}

		v.storeRequest.PointersToStore--
		v.level--
		v.nextDataNode = nil

		v.keyHashP = nil
		v.itemP = nil

		volatileAddress = load(&v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.VolatileAddress)
	}

	var stop bool
	for {
		if volatileAddress, stop = s.walkOnePointer(v, volatileAddress); stop {
			return volatileAddress
		}
	}
}

func (s *Space[K, V]) walkOnePointer(
	v *Entry[K, V],
	volatileAddress types.VolatileAddress,
) (types.VolatileAddress, bool) {
	if !isPointer(volatileAddress) {
		return volatileAddress, true
	}
	if volatileAddress.IsSet(flagHashMod) {
		v.keyHash = s.hashKeyFunc(&v.item.Key, s.hashBuff, v.level)
	}

	pointerNode := ProjectPointerNode(s.config.State.Node(volatileAddress))
	index, nextIndex := reducePointerSlot(pointerNode, PointerIndex(v.keyHash, v.level))

	v.level++
	v.storeRequest.Store[v.storeRequest.PointersToStore].Hash = &pointerNode.Hashes[index]
	v.storeRequest.Store[v.storeRequest.PointersToStore].Pointer = &pointerNode.Pointers[index]
	v.storeRequest.Store[v.storeRequest.PointersToStore].VolatileAddress = pointerNode.Pointers[index].VolatileAddress
	v.storeRequest.PointersToStore++
	v.parentIndex = index
	if nextIndex != index {
		v.nextDataNode = &pointerNode.Pointers[nextIndex].VolatileAddress

		// If we are here it means that we should stop following potential next pointer nodes because we are in the
		// non-final branch. A better data node might be added concurrently at any time.
		return pointerNode.Pointers[index].VolatileAddress, true
	}

	if v.stage == StagePointer0 && v.level == 3 {
		return pointerNode.Pointers[index].VolatileAddress, true
	}

	return pointerNode.Pointers[index].VolatileAddress, false
}

func (s *Space[K, V]) walkDataItems(v *Entry[K, V], dataNodeAddress types.VolatileAddress) bool {
	if v.keyHashP != nil {
		return false
	}

	v.exists = false

	var conflict bool
	node := s.config.State.Node(dataNodeAddress)
	keyHashes := s.config.DataNodeAssistant.KeyHashes(node)

	zeroIndex, numOfMatches := compare.Compare(uint64(v.keyHash), (*uint64)(&keyHashes[0]), &s.hashMatches[0],
		uint64(len(s.hashMatches)))
	for i := range numOfMatches {
		index := s.hashMatches[i]
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

func (s *Space[K, V]) detectUpdate(v *Entry[K, V], volatileAddress types.VolatileAddress) {
	v.stage = StageData

	if v.keyHashP == nil {
		return
	}

	if isPointer(volatileAddress) ||
		*s.config.DeletionCounter != v.deletionCounter ||
		(*v.keyHashP != 0 && (*v.keyHashP != v.keyHash || v.itemP.Key != v.item.Key)) {
		v.keyHashP = nil
		v.itemP = nil
	}
}

// Entry represents entry in the space.
type Entry[K, V comparable] struct {
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

// IteratorAndDeallocator iterates over items and deallocates space.
func IteratorAndDeallocator[K, V comparable](
	spaceRoot types.Pointer,
	appState *state.State,
	dataNodeAssistant *DataNodeAssistant[K, V],
	volatileDeallocator *state.Deallocator[types.VolatileAddress],
	persistentDeallocator *state.Deallocator[types.PersistentAddress],
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

			// It is safe to deallocate here because nodes are not reallocated until commit is finalized.
			volatileDeallocator.Deallocate(pointer.VolatileAddress)
			persistentDeallocator.Deallocate(pointer.PersistentAddress)

			switch {
			case isPointer(pointer.VolatileAddress):
				pointerNode := ProjectPointerNode(appState.Node(pointer.VolatileAddress))
				for pi := range pointerNode.Pointers {
					if isFree(pointerNode.Pointers[pi].VolatileAddress) {
						continue
					}

					stack[stackCount] = pointerNode.Pointers[pi]
					stackCount++
				}
			case !isFree(pointer.VolatileAddress):
				n := appState.Node(pointer.VolatileAddress)
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
	if !isFree(load(&pointerNode.Pointers[index].VolatileAddress)) {
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

		volatileAddress := load(&pointerNode.Pointers[newIndex].VolatileAddress)
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
