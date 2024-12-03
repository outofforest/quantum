package space

import (
	"math"
	"math/bits"
	"sort"
	"unsafe"

	"github.com/cespare/xxhash"
	"github.com/samber/lo"

	"github.com/outofforest/photon"
	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/pipeline"
	"github.com/outofforest/quantum/space/compare"
	"github.com/outofforest/quantum/types"
	"github.com/outofforest/quantum/wal"
)

// Stage constants.
const (
	StagePointer0 uint8 = iota
	StagePointer1
	StageData
)

var (
	zeroAddressPtr = lo.ToPtr[types.NodeAddress](0)
	zeroKeyHashPtr = lo.ToPtr[types.KeyHash](0)
)

// Config stores space configuration.
type Config[K, V comparable] struct {
	SpaceRoot         types.NodeRoot
	State             *alloc.State
	DataNodeAssistant *DataNodeAssistant[K, V]
	NoSnapshots       bool
}

// New creates new space.
func New[K, V comparable](config Config[K, V]) *Space[K, V] {
	s := &Space[K, V]{
		config:         config,
		numOfDataItems: config.DataNodeAssistant.NumOfItems(),
	}

	defaultInit := Entry[K, V]{
		space:        s,
		nextDataNode: zeroAddressPtr,
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
	snapshotID types.SnapshotID,
	tx *pipeline.TransactionRequest,
	walRecorder *wal.Recorder,
	allocator *alloc.Allocator,
	key K,
	stage uint8,
	hashBuff []byte,
	hashMatches []uint64,
) error {
	if v.space == nil {
		keyHash := hashKey(&key, nil, 0)
		if err := s.initEntry(v, snapshotID, tx, walRecorder, allocator, key, keyHash, stage); err != nil {
			return err
		}
	}

	return s.find(snapshotID, tx, walRecorder, allocator, v, hashBuff, hashMatches)
}

// Query queries the key.
func (s *Space[K, V]) Query(key K, hashBuff []byte, hashMatches []uint64) (V, bool) {
	volatileAddress := types.Load(&s.config.SpaceRoot.Pointer.VolatileAddress)
	var level uint8
	keyHash := hashKey(&key, nil, level)

	for volatileAddress.IsSet(types.FlagPointerNode) {
		if volatileAddress.IsSet(types.FlagHashMod) {
			keyHash = hashKey(&key, hashBuff, level)
		}

		pointerNode := ProjectPointerNode(s.config.State.Node(volatileAddress.Naked()))
		index := PointerIndex(keyHash, level)
		candidateAddress := types.Load(&pointerNode.Pointers[index].VolatileAddress)

		switch candidateAddress.State() {
		case types.StateFree:
			hops := pointerHops[index]
			hopStart := 0
			hopEnd := len(hops)

			var dataFound bool
			for hopEnd > hopStart {
				hopIndex := (hopEnd-hopStart)/2 + hopStart
				newIndex := hops[hopIndex]

				nextCandidateAddress := types.Load(&pointerNode.Pointers[newIndex].VolatileAddress)
				switch nextCandidateAddress.State() {
				case types.StateFree:
					if !dataFound {
						index = newIndex
					}
					hopStart = hopIndex + 1
				case types.StateData:
					index = newIndex
					hopEnd = hopIndex
					dataFound = true
				case types.StatePointer:
					hopEnd = hopIndex
				}
			}
		case types.StatePointer, types.StateData:
		}

		level++
		volatileAddress = types.Load(&pointerNode.Pointers[index].VolatileAddress)
	}

	if volatileAddress == types.FreeAddress {
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

// Iterator returns iterator iterating over items in space.
func (s *Space[K, V]) Iterator() func(func(item *types.DataItem[K, V]) bool) {
	return func(yield func(item *types.DataItem[K, V]) bool) {
		s.iterate(s.config.SpaceRoot.Pointer, yield)
	}
}

func (s *Space[K, V]) iterate(pointer *types.Pointer, yield func(item *types.DataItem[K, V]) bool) {
	volatileAddress := types.Load(&pointer.VolatileAddress)
	switch volatileAddress.State() {
	case types.StatePointer:
		pointerNode := ProjectPointerNode(s.config.State.Node(volatileAddress.Naked()))
		for pi := range pointerNode.Pointers {
			p := &pointerNode.Pointers[pi]
			if types.Load(&p.VolatileAddress).State() == types.StateFree {
				continue
			}

			s.iterate(p, yield)
		}
	case types.StateData:
		for _, item := range s.config.DataNodeAssistant.Iterator(s.config.State.Node(volatileAddress)) {
			if !yield(item) {
				return
			}
		}
	}
}

// Nodes returns list of nodes used by the space.
func (s *Space[K, V]) Nodes() []types.NodeAddress {
	volatileAddress := types.Load(&s.config.SpaceRoot.Pointer.VolatileAddress)
	switch volatileAddress.State() {
	case types.StateFree:
		return nil
	case types.StateData:
		return []types.NodeAddress{volatileAddress}
	}

	nodes := []types.NodeAddress{}
	stack := []types.NodeAddress{volatileAddress.Naked()}

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

		pointerNode := ProjectPointerNode(s.config.State.Node(pointerNodeAddress.Naked()))
		for pi := range pointerNode.Pointers {
			volatileAddress := types.Load(&pointerNode.Pointers[pi].VolatileAddress)
			switch volatileAddress.State() {
			case types.StateFree:
			case types.StateData:
				nodes = append(nodes, volatileAddress)
			case types.StatePointer:
				stack = append(stack, volatileAddress.Naked())
			}
		}
	}
}

// Stats returns stats about the space.
func (s *Space[K, V]) Stats() (uint64, uint64, uint64, float64) {
	volatileAddress := types.Load(&s.config.SpaceRoot.Pointer.VolatileAddress)
	switch volatileAddress.State() {
	case types.StateFree:
		return 0, 0, 0, 0
	case types.StateData:
		return 1, 0, 1, 0
	}

	stack := []types.NodeAddress{volatileAddress.Naked()}

	levels := map[types.NodeAddress]uint64{
		volatileAddress.Naked(): 1,
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

		pointerNode := ProjectPointerNode(s.config.State.Node(n.Naked()))
		for pi := range pointerNode.Pointers {
			volatileAddress := types.Load(&pointerNode.Pointers[pi].VolatileAddress)
			switch volatileAddress.State() {
			case types.StateFree:
			case types.StateData:
				dataNodes++
				if level > maxLevel {
					maxLevel = level
				}
				//nolint:gofmt,revive // looks like a bug in linter
				for _, _ = range s.config.DataNodeAssistant.Iterator(s.config.State.Node(
					volatileAddress,
				)) {
					dataItems++
				}
			case types.StatePointer:
				stack = append(stack, volatileAddress.Naked())
				levels[volatileAddress.Naked()] = level
			}
		}
	}
}

func (s *Space[K, V]) initEntry(
	v *Entry[K, V],
	snapshotID types.SnapshotID,
	tx *pipeline.TransactionRequest,
	walRecorder *wal.Recorder,
	allocator *alloc.Allocator,
	key K,
	keyHash types.KeyHash,
	stage uint8,
) error {
	initBytes := unsafe.Slice((*byte)(unsafe.Pointer(v)), s.initSize)
	copy(initBytes, s.defaultInit)
	v.keyHash = keyHash
	v.item.Key = key
	v.dataItemIndex = dataItemIndex(v.keyHash, s.numOfDataItems)
	v.stage = stage

	if types.Load(&s.config.SpaceRoot.Pointer.VolatileAddress) != types.FreeAddress &&
		s.config.SpaceRoot.Pointer.SnapshotID != snapshotID {
		persistentAddress, err := allocator.Allocate()
		if err != nil {
			return err
		}

		if err := wal.Deallocate(walRecorder, tx, s.config.SpaceRoot.Pointer, persistentAddress,
			s.config.NoSnapshots); err != nil {
			return err
		}

		// This is not stored in WAL because space roots are stored separately on commit.
		s.config.SpaceRoot.Pointer.SnapshotID = snapshotID
		s.config.SpaceRoot.Pointer.PersistentAddress = persistentAddress
	}

	return nil
}

func (s *Space[K, V]) valueExists(
	snapshotID types.SnapshotID,
	tx *pipeline.TransactionRequest,
	walRecorder *wal.Recorder,
	allocator *alloc.Allocator,
	v *Entry[K, V],
	hashBuff []byte,
	hashMatches []uint64,
) (bool, error) {
	detectUpdate(v)

	if err := s.find(snapshotID, tx, walRecorder, allocator, v, hashBuff, hashMatches); err != nil {
		return false, err
	}

	return v.exists, nil
}

func (s *Space[K, V]) readValue(
	snapshotID types.SnapshotID,
	tx *pipeline.TransactionRequest,
	walRecorder *wal.Recorder,
	allocator *alloc.Allocator,
	v *Entry[K, V],
	hashBuff []byte,
	hashMatches []uint64,
) (V, error) {
	detectUpdate(v)

	if err := s.find(snapshotID, tx, walRecorder, allocator, v, hashBuff, hashMatches); err != nil {
		return s.defaultValue, err
	}

	if v.keyHashP == nil || *v.keyHashP == 0 {
		return s.defaultValue, nil
	}

	return v.itemP.Value, nil
}

func (s *Space[K, V]) deleteValue(
	snapshotID types.SnapshotID,
	tx *pipeline.TransactionRequest,
	walRecorder *wal.Recorder,
	allocator *alloc.Allocator,
	v *Entry[K, V],
	hashBuff []byte,
	hashMatches []uint64,
) error {
	detectUpdate(v)

	if err := s.find(snapshotID, tx, walRecorder, allocator, v, hashBuff, hashMatches); err != nil {
		return err
	}

	switch {
	case types.Load(&v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.
		VolatileAddress) == types.FreeAddress:
	case v.keyHashP == nil || *v.keyHashP == 0:
	default:
		// If we are here it means `s.find` found the slot with matching key so don't need to check hash and key again.
		if err := wal.Set1(walRecorder, tx,
			v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.PersistentAddress,
			v.keyHashP, zeroKeyHashPtr,
		); err != nil {
			return err
		}

		tx.AddStoreRequest(&v.storeRequest)
	}

	return nil
}

func (s *Space[K, V]) setValue(
	snapshotID types.SnapshotID,
	tx *pipeline.TransactionRequest,
	walRecorder *wal.Recorder,
	allocator *alloc.Allocator,
	v *Entry[K, V],
	value V,
	hashBuff []byte,
	hashMatches []uint64,
) error {
	v.item.Value = value

	detectUpdate(v)

	return s.set(snapshotID, tx, walRecorder, allocator, v, hashBuff, hashMatches)
}

func (s *Space[K, V]) find(
	snapshotID types.SnapshotID,
	tx *pipeline.TransactionRequest,
	walRecorder *wal.Recorder,
	allocator *alloc.Allocator,
	v *Entry[K, V],
	hashBuff []byte,
	hashMatches []uint64,
) error {
	if err := s.walkPointers(snapshotID, tx, walRecorder, allocator, v, hashBuff); err != nil {
		return err
	}

	switch {
	case v.stage == StageData:
	case v.stage == StagePointer0:
		v.stage = StagePointer1
		return nil
	case v.stage == StagePointer1:
		v.stage = StageData
		return nil
	}

	if types.Load(&v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.VolatileAddress).
		State() != types.StateData {
		v.keyHashP = nil
		v.itemP = nil
		v.exists = false

		return nil
	}

	s.walkDataItems(v, hashMatches)

	return nil
}

func (s *Space[K, V]) set(
	snapshotID types.SnapshotID,
	tx *pipeline.TransactionRequest,
	walRecorder *wal.Recorder,
	allocator *alloc.Allocator,
	v *Entry[K, V],
	hashBuff []byte,
	hashMatches []uint64,
) error {
	if err := s.walkPointers(snapshotID, tx, walRecorder, allocator, v, hashBuff); err != nil {
		return err
	}

	volatileAddress := types.Load(&v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.VolatileAddress)

	//nolint:nestif
	if volatileAddress == types.FreeAddress {
		dataNodeVolatileAddress, err := allocator.Allocate()
		if err != nil {
			return err
		}
		s.config.State.Clear(dataNodeVolatileAddress)

		dataNodePersistentAddress, err := allocator.Allocate()
		if err != nil {
			return err
		}
		s.config.State.Clear(dataNodePersistentAddress)

		if v.storeRequest.PointersToStore > 1 {
			if err := wal.Set2(walRecorder, tx,
				v.storeRequest.Store[v.storeRequest.PointersToStore-2].Pointer.PersistentAddress,
				&v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.SnapshotID, &snapshotID,
				&v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.PersistentAddress, &dataNodePersistentAddress,
			); err != nil {
				return err
			}

			if err := wal.SetAddressAtomically(walRecorder, tx,
				v.storeRequest.Store[v.storeRequest.PointersToStore-2].Pointer.PersistentAddress,
				&v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.VolatileAddress, &dataNodeVolatileAddress,
			); err != nil {
				return err
			}
		} else {
			v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.SnapshotID = snapshotID
			v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.PersistentAddress = dataNodePersistentAddress
			types.Store(&v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.VolatileAddress, dataNodeVolatileAddress)
		}
	}

	// Starting from here the data node is allocated.

	conflict := s.walkDataItems(v, hashMatches)

	if v.keyHashP != nil {
		if *v.keyHashP == 0 {
			if err := wal.Set2(walRecorder, tx,
				v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.PersistentAddress,
				v.keyHashP, &v.keyHash,
				v.itemP, &v.item,
			); err != nil {
				return err
			}
		} else if err := wal.Set1(walRecorder, tx,
			v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.PersistentAddress,
			&v.itemP.Value, &v.item.Value,
		); err != nil {
			return err
		}

		tx.AddStoreRequest(&v.storeRequest)

		return nil
	}

	// Try to split data node.
	if v.storeRequest.PointersToStore > 1 {
		splitDone, err := s.splitDataNode(snapshotID, tx, walRecorder, allocator, v.parentIndex,
			v.storeRequest.Store[v.storeRequest.PointersToStore-2].Pointer, v.level)
		if err != nil {
			return err
		}
		if splitDone {
			// This must be done because the item to set might go to the newly created data node.
			v.storeRequest.PointersToStore--
			v.level--

			return s.set(snapshotID, tx, walRecorder, allocator, v, hashBuff, hashMatches)
		}
	}

	// Add pointer node.
	if err := s.addPointerNode(snapshotID, tx, walRecorder, allocator, v, conflict); err != nil {
		return err
	}

	return s.set(snapshotID, tx, walRecorder, allocator, v, hashBuff, hashMatches)
}

func (s *Space[K, V]) splitToIndex(parentNodeAddress types.NodeAddress, index uint64) (uint64, uint64) {
	trailingZeros := bits.TrailingZeros64(NumOfPointers)
	if trailingZeros2 := bits.TrailingZeros64(index); trailingZeros2 < trailingZeros {
		trailingZeros = trailingZeros2
	}
	mask := uint64(1) << trailingZeros

	parentNode := ProjectPointerNode(s.config.State.Node(parentNodeAddress.Naked()))
	for mask > 1 {
		mask >>= 1
		newIndex := index | mask
		if types.Load(&parentNode.Pointers[newIndex].VolatileAddress) == types.FreeAddress {
			index = newIndex
			break
		}
	}

	return index, uint64(math.MaxUint64) << bits.TrailingZeros64(mask)
}

func (s *Space[K, V]) splitDataNode(
	snapshotID types.SnapshotID,
	tx *pipeline.TransactionRequest,
	walRecorder *wal.Recorder,
	allocator *alloc.Allocator,
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

	newNodePersistentAddress, err := allocator.Allocate()
	if err != nil {
		return false, err
	}
	s.config.State.Clear(newNodePersistentAddress)

	parentNode := ProjectPointerNode(s.config.State.Node(types.Load(&parentNodePointer.VolatileAddress).Naked()))
	existingNodePointer := &parentNode.Pointers[index]
	existingDataNode := s.config.State.Node(types.Load(&existingNodePointer.VolatileAddress))
	newDataNode := s.config.State.Node(newNodeVolatileAddress)
	keyHashes := s.config.DataNodeAssistant.KeyHashes(existingDataNode)
	newKeyHashes := s.config.DataNodeAssistant.KeyHashes(newDataNode)

	for i, item := range s.config.DataNodeAssistant.Iterator(existingDataNode) {
		itemIndex := PointerIndex(keyHashes[i], level-1)
		if itemIndex&mask != newIndex {
			continue
		}

		newDataNodeItem := s.config.DataNodeAssistant.Item(newDataNode, s.config.DataNodeAssistant.ItemOffset(i))
		if err := wal.Copy(walRecorder, tx, newNodePersistentAddress, existingNodePointer.PersistentAddress,
			newDataNodeItem, item,
		); err != nil {
			return false, err
		}
		if err := wal.Set1(walRecorder, tx, newNodePersistentAddress,
			&newKeyHashes[i], &keyHashes[i],
		); err != nil {
			return false, err
		}
		if err := wal.Set1(walRecorder, tx, existingNodePointer.PersistentAddress,
			&keyHashes[i], zeroKeyHashPtr,
		); err != nil {
			return false, err
		}
	}

	if err := wal.Set2(walRecorder, tx, parentNodePointer.PersistentAddress,
		&parentNode.Pointers[newIndex].SnapshotID, &snapshotID,
		&parentNode.Pointers[newIndex].PersistentAddress, &newNodePersistentAddress,
	); err != nil {
		return false, err
	}

	if err := wal.SetAddressAtomically(walRecorder, tx, parentNodePointer.PersistentAddress,
		&parentNode.Pointers[newIndex].VolatileAddress, &newNodeVolatileAddress,
	); err != nil {
		return false, err
	}

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
	snapshotID types.SnapshotID,
	tx *pipeline.TransactionRequest,
	walRecorder *wal.Recorder,
	allocator *alloc.Allocator,
	v *Entry[K, V],
	conflict bool,
) error {
	pointerNodeVolatileAddress, err := allocator.Allocate()
	if err != nil {
		return err
	}
	s.config.State.Clear(pointerNodeVolatileAddress)

	pointerNodePersistentAddress, err := allocator.Allocate()
	if err != nil {
		return err
	}
	s.config.State.Clear(pointerNodePersistentAddress)

	dataPointer := v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer
	pointerNode := ProjectPointerNode(s.config.State.Node(pointerNodeVolatileAddress))
	pointerNodeRoot := &v.storeRequest.Store[v.storeRequest.PointersToStore-1]

	if err := wal.Set2(walRecorder, tx, pointerNodePersistentAddress,
		&pointerNode.Pointers[0].SnapshotID, &dataPointer.SnapshotID,
		&pointerNode.Pointers[0].PersistentAddress, &dataPointer.PersistentAddress,
	); err != nil {
		return err
	}

	if err := wal.SetAddressAtomically(walRecorder, tx, pointerNodePersistentAddress,
		&pointerNode.Pointers[0].VolatileAddress, &dataPointer.VolatileAddress,
	); err != nil {
		return err
	}

	pointerNodeVolatileAddress = pointerNodeVolatileAddress.Set(types.FlagPointerNode)
	if conflict {
		pointerNodeVolatileAddress = pointerNodeVolatileAddress.Set(types.FlagHashMod)
	}

	if v.storeRequest.PointersToStore > 1 {
		if err := wal.Set1(walRecorder, tx,
			v.storeRequest.Store[v.storeRequest.PointersToStore-2].Pointer.PersistentAddress,
			&pointerNodeRoot.Pointer.PersistentAddress, &pointerNodePersistentAddress,
		); err != nil {
			return err
		}

		if err := wal.SetAddressAtomically(walRecorder, tx,
			v.storeRequest.Store[v.storeRequest.PointersToStore-2].Pointer.PersistentAddress,
			&pointerNodeRoot.Pointer.VolatileAddress, &pointerNodeVolatileAddress,
		); err != nil {
			return err
		}
	} else {
		pointerNodeRoot.Pointer.PersistentAddress = pointerNodePersistentAddress
		types.Store(&pointerNodeRoot.Pointer.VolatileAddress, pointerNodeVolatileAddress)
	}

	_, err = s.splitDataNode(snapshotID, tx, walRecorder, allocator, 0, pointerNodeRoot.Pointer, v.level+1)
	return err
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

func (s *Space[K, V]) walkPointers(
	snapshotID types.SnapshotID,
	tx *pipeline.TransactionRequest,
	walRecorder *wal.Recorder,
	allocator *alloc.Allocator,
	v *Entry[K, V],
	hashBuff []byte,
) error {
	for {
		more, err := s.walkOnePointer(snapshotID, tx, walRecorder, allocator, v, hashBuff)
		if err != nil || !more {
			return err
		}
	}
}

func (s *Space[K, V]) walkOnePointer(
	snapshotID types.SnapshotID,
	tx *pipeline.TransactionRequest,
	walRecorder *wal.Recorder,
	allocator *alloc.Allocator,
	v *Entry[K, V],
	hashBuff []byte,
) (bool, error) {
	volatileAddress := types.Load(&v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.VolatileAddress)
	if !volatileAddress.IsSet(types.FlagPointerNode) {
		return false, nil
	}
	if volatileAddress.IsSet(types.FlagHashMod) {
		v.keyHash = hashKey(&v.item.Key, hashBuff, v.level)
	}

	pointerNode := ProjectPointerNode(s.config.State.Node(volatileAddress.Naked()))
	index := PointerIndex(v.keyHash, v.level)
	state := types.Load(&pointerNode.Pointers[index].VolatileAddress).State()
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

			switch types.Load(&pointerNode.Pointers[newIndex].VolatileAddress).State() {
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
	case types.StatePointer, types.StateData:
	default:
		return false, nil
	}

	//nolint:nestif
	if types.Load(&pointerNode.Pointers[index].VolatileAddress) != types.FreeAddress &&
		pointerNode.Pointers[index].SnapshotID != snapshotID {
		persistentAddress, err := allocator.Allocate()
		if err != nil {
			return false, err
		}

		if err := wal.Deallocate(walRecorder, tx, &pointerNode.Pointers[index], persistentAddress,
			s.config.NoSnapshots); err != nil {
			return false, err
		}

		if v.storeRequest.PointersToStore > 0 {
			if err := wal.Set2(walRecorder, tx,
				v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.PersistentAddress,
				&pointerNode.Pointers[index].SnapshotID, &snapshotID,
				&pointerNode.Pointers[index].PersistentAddress, &persistentAddress,
			); err != nil {
				return false, err
			}
		} else {
			pointerNode.Pointers[index].SnapshotID = snapshotID
			pointerNode.Pointers[index].PersistentAddress = persistentAddress
		}
	}

	v.level++
	v.storeRequest.Store[v.storeRequest.PointersToStore].Hash = &pointerNode.Hashes[index]
	v.storeRequest.Store[v.storeRequest.PointersToStore].Pointer = &pointerNode.Pointers[index]
	v.storeRequest.PointersToStore++
	v.parentIndex = index
	if nextIndex == index {
		v.nextDataNode = zeroAddressPtr
	} else {
		v.nextDataNode = &pointerNode.Pointers[nextIndex].VolatileAddress
	}

	if v.stage == StagePointer0 && v.level == 3 {
		return false, nil
	}

	return true, nil
}

func (s *Space[K, V]) walkDataItems(v *Entry[K, V], hashMatches []uint64) bool {
	if v.keyHashP != nil {
		return false
	}

	v.exists = false

	var conflict bool
	node := s.config.State.Node(types.Load(
		&v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.VolatileAddress),
	)
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
	space        *Space[K, V]
	nextDataNode *types.NodeAddress
	storeRequest pipeline.StoreRequest

	itemP         *types.DataItem[K, V]
	keyHashP      *types.KeyHash
	keyHash       types.KeyHash
	item          types.DataItem[K, V]
	parentIndex   uint64
	dataItemIndex uint64
	exists        bool
	stage         uint8
	level         uint8
}

// Value returns the value from entry.
func (v *Entry[K, V]) Value(
	snapshotID types.SnapshotID,
	tx *pipeline.TransactionRequest,
	walRecorder *wal.Recorder,
	allocator *alloc.Allocator,
	hashBuff []byte,
	hashMatches []uint64,
) (V, error) {
	return v.space.readValue(snapshotID, tx, walRecorder, allocator, v, hashBuff, hashMatches)
}

// Key returns the key from entry.
func (v *Entry[K, V]) Key() K {
	return v.item.Key
}

// Exists returns true if entry exists in the space.
func (v *Entry[K, V]) Exists(
	snapshotID types.SnapshotID,
	tx *pipeline.TransactionRequest,
	walRecorder *wal.Recorder,
	allocator *alloc.Allocator,
	hashBuff []byte,
	hashMatches []uint64,
) (bool, error) {
	return v.space.valueExists(snapshotID, tx, walRecorder, allocator, v, hashBuff, hashMatches)
}

// Set sts value for entry.
func (v *Entry[K, V]) Set(
	snapshotID types.SnapshotID,
	tx *pipeline.TransactionRequest,
	walRecorder *wal.Recorder,
	allocator *alloc.Allocator,
	value V,
	hashBuff []byte,
	hashMatches []uint64,
) error {
	return v.space.setValue(snapshotID, tx, walRecorder, allocator, v, value, hashBuff, hashMatches)
}

// Delete deletes the entry.
func (v *Entry[K, V]) Delete(
	snapshotID types.SnapshotID,
	tx *pipeline.TransactionRequest,
	walRecorder *wal.Recorder,
	allocator *alloc.Allocator,
	hashBuff []byte,
	hashMatches []uint64,
) error {
	return v.space.deleteValue(snapshotID, tx, walRecorder, allocator, v, hashBuff, hashMatches)
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
	case *v.nextDataNode != types.FreeAddress:
		v.storeRequest.PointersToStore--
		v.level--

		v.keyHashP = nil
		v.itemP = nil
	case v.keyHashP != nil && (types.Load(&v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.
		VolatileAddress).State() != types.StateData ||
		(*v.keyHashP != 0 && (*v.keyHashP != v.keyHash || v.itemP.Key != v.item.Key))):
		v.keyHashP = nil
		v.itemP = nil
	}
}

// Deallocate deallocates all nodes used by the space.
func Deallocate(
	spaceRoot *types.Pointer,
	deallocator *alloc.Deallocator,
	state *alloc.State,
) {
	volatileAddress := types.Load(&spaceRoot.VolatileAddress)
	switch volatileAddress.State() {
	case types.StateFree:
		return
	case types.StateData:
		deallocator.Deallocate(volatileAddress)
		deallocator.Deallocate(spaceRoot.PersistentAddress)
		return
	}

	deallocatePointerNode(spaceRoot, deallocator, state)
}

func deallocatePointerNode(
	pointer *types.Pointer,
	deallocator *alloc.Deallocator,
	state *alloc.State,
) {
	volatileAddress := types.Load(&pointer.VolatileAddress)
	pointerNode := ProjectPointerNode(state.Node(volatileAddress.Naked()))
	for pi := range pointerNode.Pointers {
		p := &pointerNode.Pointers[pi]

		volatileAddress := types.Load(&p.VolatileAddress)
		switch volatileAddress.State() {
		case types.StateData:
			deallocator.Deallocate(volatileAddress)
			deallocator.Deallocate(p.PersistentAddress)
		case types.StatePointer:
			deallocatePointerNode(p, deallocator, state)
		}
	}
	deallocator.Deallocate(volatileAddress)
	deallocator.Deallocate(pointer.PersistentAddress)
}
