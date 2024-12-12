// Github actions run on machines not supporting AVX-512 instructions.
//go:build nogithub

package space

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/quantum/alloc"
	txtypes "github.com/outofforest/quantum/tx/types"
	"github.com/outofforest/quantum/types"
)

const (
	nodesPerGroup = 100
	stateSize     = 100 * nodesPerGroup * types.NodeLength
)

// TestKeyHashes verifies that different inputs produce different key hashes.
func TestKeyHashes(t *testing.T) {
	key1 := [16]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f}
	key2 := [16]byte{0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f}
	buff := make([]byte, len(key1)+1)

	hashes := map[types.KeyHash]struct{}{
		hashKey(&key1, nil, 0):  {},
		hashKey(&key1, buff, 0): {},
		hashKey(&key1, buff, 1): {},
		hashKey(&key1, buff, 2): {},
		hashKey(&key1, buff, 3): {},
		hashKey(&key2, nil, 0):  {},
		hashKey(&key2, buff, 0): {},
		hashKey(&key2, buff, 1): {},
		hashKey(&key2, buff, 2): {},
		hashKey(&key2, buff, 3): {},
	}

	require.Len(t, hashes, 10)
}

// TestPointerSlotReductionUsingDataNodes verifies the sequence of pointer slot reduction chain if slots are occupied
// by data nodes only.
func TestPointerSlotReductionUsingDataNodes(t *testing.T) {
	requireT := require.New(t)

	for i := range uint64(NumOfPointers) {
		pointerNode := &PointerNode{}
		hops := pointerHops[i]

		index, nextIndex := reducePointerSlot(pointerNode, i)
		if len(hops) > 0 {
			requireT.Equal(hops[len(hops)-1], index)
		} else {
			requireT.Equal(i, index)
		}
		if len(hops) > 1 {
			requireT.Equal(hops[len(hops)-2], nextIndex)
		} else {
			requireT.Equal(i, nextIndex)
		}

		for hi := len(hops) - 1; hi >= 0; hi-- {
			pointerNode.Pointers[hops[hi]].VolatileAddress = 1 // To mark it as data node.
			index, nextIndex := reducePointerSlot(pointerNode, i)
			requireT.Equal(hops[hi], index)
			if hi > 0 {
				requireT.Equal(hops[hi-1], nextIndex)
			} else {
				requireT.Equal(i, nextIndex)
			}
		}

		pointerNode.Pointers[i].VolatileAddress = 1 // To mark it as data node.
		index, nextIndex = reducePointerSlot(pointerNode, i)
		requireT.Equal(i, index)
		requireT.Equal(i, nextIndex)
	}
}

// TestPointerSlotReductionUsingPointerNodes verifies the sequence of pointer slot reduction chain if slots are occupied
// by pointer nodes and one data node.
func TestPointerSlotReductionUsingPointerNodes(t *testing.T) {
	requireT := require.New(t)

	for i := range uint64(NumOfPointers) {
		pointerNode := &PointerNode{}
		hops := pointerHops[i]

		index, nextIndex := reducePointerSlot(pointerNode, i)
		if len(hops) > 0 {
			requireT.Equal(hops[len(hops)-1], index)
		} else {
			requireT.Equal(i, index)
		}
		if len(hops) > 1 {
			requireT.Equal(hops[len(hops)-2], nextIndex)
		} else {
			requireT.Equal(i, nextIndex)
		}

		for hi := len(hops) - 1; hi >= 0; hi-- {
			pointerNode.Pointers[hops[hi]].VolatileAddress = 1 // To mark it as data node.
			if hi < len(hops)-1 {
				pointerNode.Pointers[hops[hi+1]].VolatileAddress = flagPointerNode // To mark it as pointer node.
			}
			index, nextIndex := reducePointerSlot(pointerNode, i)
			requireT.Equal(hops[hi], index)
			if hi > 0 {
				requireT.Equal(hops[hi-1], nextIndex)
			} else {
				requireT.Equal(i, nextIndex)
			}
		}

		pointerNode.Pointers[i].VolatileAddress = 1 // To mark it as data node.
		if len(hops) > 0 {
			pointerNode.Pointers[hops[0]].VolatileAddress = flagPointerNode // To mark it as pointer node.
		}
		index, nextIndex = reducePointerSlot(pointerNode, i)
		requireT.Equal(i, index)
		requireT.Equal(i, nextIndex)

		pointerNode.Pointers[i].VolatileAddress = flagPointerNode // To mark it as pointer node.
		index, nextIndex = reducePointerSlot(pointerNode, i)
		requireT.Equal(i, index)
		requireT.Equal(i, nextIndex)
	}
}

// TestCRUDOnRootDataNode tests basic CRUD operations using one data item on single data node being the root
// of the space.
func TestCRUDOnRootDataNode(t *testing.T) {
	requireT := require.New(t)

	const amount txtypes.Amount = 100
	var (
		account = TestKey[txtypes.Account]{
			Key:     txtypes.Account{0x01},
			KeyHash: 1,
		}
	)

	state, err := alloc.RunInTest(t, stateSize, nodesPerGroup)
	requireT.NoError(err)

	s := NewSpaceTest[txtypes.Account, txtypes.Amount](t, state, hashKey)

	// Read non-existing.

	balance, exists := s.Query(account)
	requireT.False(exists)
	requireT.Equal(txtypes.Amount(0), balance)

	v := s.NewEntry(account, StageData)

	requireT.False(s.KeyExists(v))
	requireT.Equal(txtypes.Amount(0), s.ReadKey(v))

	// Create.

	requireT.NoError(s.SetKey(v, amount))

	// Read existing.

	balance, exists = s.Query(account)
	requireT.True(exists)
	requireT.Equal(amount, balance)

	requireT.True(s.KeyExists(v))
	requireT.Equal(amount, s.ReadKey(v))

	v2 := s.NewEntry(account, StageData)

	requireT.True(s.KeyExists(v2))
	requireT.Equal(amount, s.ReadKey(v2))

	v3 := s.NewEntry(account, StagePointer0)

	requireT.True(s.KeyExists(v3))
	requireT.Equal(amount, s.ReadKey(v3))

	// Update 1.

	requireT.NoError(s.SetKey(v3, amount+1))

	requireT.True(s.KeyExists(v3))
	requireT.Equal(amount+1, s.ReadKey(v3))

	balance, exists = s.Query(account)
	requireT.True(exists)
	requireT.Equal(amount+1, balance)

	v4 := s.NewEntry(account, StagePointer0)

	requireT.True(s.KeyExists(v4))
	requireT.Equal(amount+1, s.ReadKey(v4))

	// Update 2.

	requireT.NoError(s.SetKey(v4, amount+2))

	requireT.True(s.KeyExists(v4))
	requireT.Equal(amount+2, s.ReadKey(v4))

	balance, exists = s.Query(account)
	requireT.True(exists)
	requireT.Equal(amount+2, balance)

	v5 := s.NewEntry(account, StagePointer0)

	requireT.True(s.KeyExists(v5))
	requireT.Equal(amount+2, s.ReadKey(v5))

	// Delete 1.

	s.DeleteKey(v5)

	requireT.False(s.KeyExists(v5))
	requireT.Equal(txtypes.Amount(0), s.ReadKey(v5))

	balance, exists = s.Query(account)
	requireT.False(exists)
	requireT.Equal(txtypes.Amount(0), balance)

	// Recreate.

	v = s.NewEntry(account, StagePointer0)

	requireT.NoError(s.SetKey(v, amount))

	balance, exists = s.Query(account)
	requireT.True(exists)
	requireT.Equal(amount, balance)

	// Delete 2.

	v2 = s.NewEntry(account, StagePointer0)

	s.DeleteKey(v2)

	requireT.False(s.KeyExists(v2))
	requireT.Equal(txtypes.Amount(0), s.ReadKey(v2))

	balance, exists = s.Query(account)
	requireT.False(exists)
	requireT.Equal(txtypes.Amount(0), balance)
}

// TestSetConflictingHashesOnRootDataNode sets many keys using the same hash to verify that they are not overwritten
// by each other.
func TestSetConflictingHashesOnRootDataNode(t *testing.T) {
	const (
		numOfItems               = 50
		keyHash    types.KeyHash = 1 // Same key hash is intentionally used for all the items to test conflicts.
	)

	requireT := require.New(t)

	state, err := alloc.RunInTest(t, stateSize, nodesPerGroup)
	requireT.NoError(err)

	s := NewSpaceTest[txtypes.Account, txtypes.Amount](t, state, hashKey)

	// Create.

	for i := range uint8(numOfItems) {
		v := s.NewEntry(TestKey[txtypes.Account]{
			Key:     txtypes.Account{i},
			KeyHash: keyHash,
		}, StagePointer0)
		requireT.NoError(s.SetKey(v, txtypes.Amount(i)))
	}

	// Verify items exist.

	for i := range uint8(numOfItems) {
		key := TestKey[txtypes.Account]{
			Key:     txtypes.Account{i},
			KeyHash: keyHash,
		}
		amount := txtypes.Amount(i)

		v := s.NewEntry(key, StagePointer0)

		requireT.True(s.KeyExists(v))
		requireT.Equal(amount, s.ReadKey(v))

		balance, exists := s.Query(key)
		requireT.True(exists)
		requireT.Equal(amount, balance)
	}

	// Update every second item.

	for i := uint8(0); i < numOfItems; i += 2 {
		key := TestKey[txtypes.Account]{
			Key:     txtypes.Account{i},
			KeyHash: keyHash,
		}
		amount := txtypes.Amount(10 * i)

		v := s.NewEntry(key, StagePointer0)
		requireT.NoError(s.SetKey(v, amount))

		v2 := s.NewEntry(key, StagePointer0)

		requireT.True(s.KeyExists(v2))
		requireT.Equal(amount, s.ReadKey(v2))

		balance, exists := s.Query(key)
		requireT.True(exists)
		requireT.Equal(amount, balance)
	}

	// Verify all the other items stay untouched.

	for i := uint8(1); i < numOfItems; i += 2 {
		key := TestKey[txtypes.Account]{
			Key:     txtypes.Account{i},
			KeyHash: keyHash,
		}
		amount := txtypes.Amount(i)

		v := s.NewEntry(key, StagePointer0)

		requireT.True(s.KeyExists(v))
		requireT.Equal(amount, s.ReadKey(v))

		balance, exists := s.Query(key)
		requireT.True(exists)
		requireT.Equal(amount, balance)
	}

	// Delete every second item.

	for i := uint8(0); i < numOfItems; i += 2 {
		key := TestKey[txtypes.Account]{
			Key:     txtypes.Account{i},
			KeyHash: keyHash,
		}

		v := s.NewEntry(key, StagePointer0)
		s.DeleteKey(v)

		v2 := s.NewEntry(key, StagePointer0)

		requireT.False(s.KeyExists(v2))
		requireT.Equal(txtypes.Amount(0), s.ReadKey(v2))

		balance, exists := s.Query(key)
		requireT.False(exists)
		requireT.Equal(txtypes.Amount(0), balance)
	}

	// Verify all the other items still exist.

	for i := uint8(1); i < numOfItems; i += 2 {
		key := TestKey[txtypes.Account]{
			Key:     txtypes.Account{i},
			KeyHash: keyHash,
		}
		amount := txtypes.Amount(i)

		v := s.NewEntry(key, StagePointer0)

		requireT.True(s.KeyExists(v))
		requireT.Equal(amount, s.ReadKey(v))

		balance, exists := s.Query(key)
		requireT.True(exists)
		requireT.Equal(amount, balance)
	}
}

// TestAddingPointerNodeWithoutConflictResolution verifies that all the data items with the same key hash stay
// in the same data node without key hash recalculation, if space is instructed to add new pointer node without
// conflict resolution.
func TestAddingPointerNodeWithoutConflictResolution(t *testing.T) {
	const (
		// It is selected this way to be sure that nothing is moved to the next data node.
		numOfItems               = NumOfPointers
		keyHash    types.KeyHash = 1 // Same key hash is intentionally used for all the items.
	)

	requireT := require.New(t)

	state, err := alloc.RunInTest(t, stateSize, nodesPerGroup)
	requireT.NoError(err)

	s := NewSpaceTest[txtypes.Account, txtypes.Amount](t, state, hashKey)

	// Create.

	for i := range uint8(numOfItems - 1) {
		v := s.NewEntry(TestKey[txtypes.Account]{
			Key:     txtypes.Account{i},
			KeyHash: keyHash,
		}, StagePointer0)
		requireT.NoError(s.SetKey(v, txtypes.Amount(i)))
		requireT.Equal(uint8(0), v.level)
	}

	// Add pointer node and set the last item.

	v := s.NewEntry(TestKey[txtypes.Account]{
		Key:     txtypes.Account{numOfItems - 1},
		KeyHash: keyHash,
	}, StageData)
	s.Find(v)

	// Store the address of the data node to be sure that no items have been moved.
	dataNodeAddress := v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.VolatileAddress

	requireT.NoError(s.AddPointerNode(v, false))
	requireT.NoError(s.SetKey(v, txtypes.Amount(numOfItems-1)))
	requireT.Equal(uint8(1), v.level)

	// Verify that all the items has been moved correctly without recomputing hashes.

	for i := range uint8(numOfItems) {
		key := TestKey[txtypes.Account]{
			Key:     txtypes.Account{i},
			KeyHash: keyHash,
		}
		amount := txtypes.Amount(i)

		v := s.NewEntry(key, StagePointer0)

		requireT.True(s.KeyExists(v))
		requireT.Equal(amount, s.ReadKey(v))

		balance, exists := s.Query(key)
		requireT.True(exists)
		requireT.Equal(amount, balance)

		requireT.Equal(uint8(1), v.level)
		requireT.Equal(keyHash, v.keyHash)
		requireT.Equal(keyHash, *v.keyHashP)
		requireT.Equal(dataNodeAddress, v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.VolatileAddress)
	}
}

// TestAddingPointerNodeWithConflictResolution verifies that key hashes are recomputed and data items are redistributed
// if space is instructed to add new pointer node with conflict resolution.
func TestAddingPointerNodeWithConflictResolution(t *testing.T) {
	const (
		// It is selected this way so half of the items is moved to another data node.
		numOfItems               = NumOfPointers
		keyHash    types.KeyHash = 1 // Same key hash is intentionally used for all the items.
	)

	hashKeyFunc := func(key *txtypes.Account, buff []byte, level uint8) types.KeyHash {
		return types.KeyHash(key[0]) + 1 // +1 to avoid 0
	}

	requireT := require.New(t)

	state, err := alloc.RunInTest(t, stateSize, nodesPerGroup)
	requireT.NoError(err)

	s := NewSpaceTest[txtypes.Account, txtypes.Amount](t, state, hashKeyFunc)

	// Create.

	for i := range uint8(numOfItems - 1) {
		v := s.NewEntry(TestKey[txtypes.Account]{
			Key:     txtypes.Account{i},
			KeyHash: keyHash,
		}, StagePointer0)
		requireT.NoError(s.SetKey(v, txtypes.Amount(i)))
		requireT.Equal(uint8(0), v.level)
	}

	// Add pointer node and set the last item.

	v := s.NewEntry(TestKey[txtypes.Account]{
		Key:     txtypes.Account{numOfItems - 1},
		KeyHash: keyHash,
	}, StageData)
	s.Find(v)

	// Store the address of the data node to be sure that half of the items is moved to another data node.
	dataNodeAddress := v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.VolatileAddress

	requireT.NoError(s.AddPointerNode(v, true))
	requireT.NoError(s.SetKey(v, txtypes.Amount(numOfItems-1)))
	requireT.Equal(uint8(1), v.level)

	// Verify that all the items has been moved correctly without recomputing hashes.

	dataNodes := map[types.VolatileAddress]uint64{}

	for i := range uint8(numOfItems) {
		key := TestKey[txtypes.Account]{
			Key:     txtypes.Account{i},
			KeyHash: keyHash,
		}
		amount := txtypes.Amount(i)

		v := s.NewEntry(key, StagePointer0)

		requireT.True(s.KeyExists(v))
		requireT.Equal(amount, s.ReadKey(v))

		balance, exists := s.Query(key)
		requireT.True(exists)
		requireT.Equal(amount, balance)

		requireT.Equal(uint8(1), v.level)
		requireT.Equal(types.KeyHash(i+1), v.keyHash)
		requireT.Equal(types.KeyHash(i+1), *v.keyHashP)

		dataNodes[v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.VolatileAddress]++
	}

	requireT.Len(dataNodes, 2)
	for _, n := range dataNodes {
		requireT.Equal(uint64(numOfItems/2), n)
	}
	requireT.Equal(uint64(numOfItems/2), dataNodes[dataNodeAddress])
}

// TestAddingPointerNodeForNonConflictingDataItems verifies that key hashes are not recomputed if there is no conflict
// and data items are redistributed if space is instructed to add new pointer node without conflict resolution.
func TestAddingPointerNodeForNonConflictingDataItems(t *testing.T) {
	// It is selected this way so half of the items is moved to another data node.
	const numOfItems = NumOfPointers

	requireT := require.New(t)

	state, err := alloc.RunInTest(t, stateSize, nodesPerGroup)
	requireT.NoError(err)

	s := NewSpaceTest[txtypes.Account, txtypes.Amount](t, state, hashKey)

	// Create.

	for i := range uint8(numOfItems - 1) {
		v := s.NewEntry(TestKey[txtypes.Account]{
			Key:     txtypes.Account{i},
			KeyHash: types.KeyHash(i + 1), // +1 to avoid 0
		}, StagePointer0)
		requireT.NoError(s.SetKey(v, txtypes.Amount(i)))
		requireT.Equal(uint8(0), v.level)
	}

	// Add pointer node and set the last item.

	v := s.NewEntry(TestKey[txtypes.Account]{
		Key:     txtypes.Account{numOfItems - 1},
		KeyHash: types.KeyHash(numOfItems),
	}, StageData)
	s.Find(v)

	// Store the address of the data node to be sure that half of the items is moved to another data node.
	dataNodeAddress := v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.VolatileAddress

	requireT.NoError(s.AddPointerNode(v, false))
	requireT.NoError(s.SetKey(v, txtypes.Amount(numOfItems-1)))
	requireT.Equal(uint8(1), v.level)

	// Verify that all the items has been moved correctly without recomputing hashes.

	dataNodes := map[types.VolatileAddress]uint64{}

	for i := range uint8(numOfItems) {
		key := TestKey[txtypes.Account]{
			Key:     txtypes.Account{i},
			KeyHash: types.KeyHash(i + 1), // +1 to avoid 0
		}
		amount := txtypes.Amount(i)

		v := s.NewEntry(key, StagePointer0)

		requireT.True(s.KeyExists(v))
		requireT.Equal(amount, s.ReadKey(v))

		balance, exists := s.Query(key)
		requireT.True(exists)
		requireT.Equal(amount, balance)

		requireT.Equal(uint8(1), v.level)
		requireT.Equal(types.KeyHash(i+1), v.keyHash)
		requireT.Equal(types.KeyHash(i+1), *v.keyHashP)

		dataNodes[v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.VolatileAddress]++
	}

	requireT.Len(dataNodes, 2)
	for _, n := range dataNodes {
		requireT.Equal(uint64(numOfItems/2), n)
	}
	requireT.Equal(uint64(numOfItems/2), dataNodes[dataNodeAddress])
}

// TestDataNodeSplitWithoutConflictResolution verifies that data nodes are allocated in the right order when there
// is a time to split them. This test assumes there are no key hash conflicts to be resolved.
func TestDataNodeSplitWithoutConflictResolution(t *testing.T) {
	// It is selected this way so at the end each pointer references a data node containing one data item.
	const numOfItems = NumOfPointers

	requireT := require.New(t)

	state, err := alloc.RunInTest(t, stateSize, nodesPerGroup)
	requireT.NoError(err)

	s := NewSpaceTest[txtypes.Account, txtypes.Amount](t, state, hashKey)

	// Store items in the root data node.

	for i := uint8(1); i <= numOfItems; i++ {
		v := s.NewEntry(TestKey[txtypes.Account]{
			Key:     txtypes.Account{i},
			KeyHash: types.KeyHash(i),
		}, StagePointer0)
		requireT.NoError(s.SetKey(v, txtypes.Amount(i)))
	}

	// Convert root node into pointer node.

	v := s.NewEntry(TestKey[txtypes.Account]{
		Key:     txtypes.Account{0x01},
		KeyHash: types.KeyHash(1), // +1 to avoid 0
	}, StageData)
	requireT.NoError(s.AddPointerNode(v, false))

	// Verify the current structure of the tree.
	// Root node should be a pointer node with two data node children at indexes 0 and NumOfPointers / 2.
	// Each data node should contain numOfItems / 2 items. First node should contain items with hashes 1-31 and 64,
	// Second node should contain items with hashes 32-63.

	pointerNode := ProjectPointerNode(state.Node(s.Root().VolatileAddress))
	dataNodeAIndex := 0
	dataNodeBIndex := NumOfPointers / 2
	for i, p := range pointerNode.Pointers {
		if i == dataNodeAIndex || i == dataNodeBIndex {
			requireT.NotEqual(types.FreeAddress, p.VolatileAddress)
		} else {
			requireT.Equal(types.FreeAddress, p.VolatileAddress)
		}
	}

	dataNodeAssistant := s.DataNodeAssistant()
	dataNodeAKeyHashes := dataNodeAssistant.KeyHashes(state.Node(pointerNode.Pointers[dataNodeAIndex].VolatileAddress))
	dataNodeBKeyHashes := dataNodeAssistant.KeyHashes(state.Node(pointerNode.Pointers[dataNodeBIndex].VolatileAddress))

	keyHashes := map[types.KeyHash]int{}
	for _, kh := range dataNodeAKeyHashes {
		if kh != 0 {
			_, exists := keyHashes[kh]
			requireT.False(exists)
			keyHashes[kh] = dataNodeAIndex
		}
	}
	for _, kh := range dataNodeBKeyHashes {
		if kh != 0 {
			_, exists := keyHashes[kh]
			requireT.False(exists)
			keyHashes[kh] = dataNodeBIndex
		}
	}

	requireT.Len(keyHashes, numOfItems)
	// Check that items 1-31 are in data node A.
	for i := types.KeyHash(1); i < numOfItems/2; i++ {
		index, exists := keyHashes[i]
		requireT.True(exists)
		requireT.Equal(dataNodeAIndex, index)
	}
	// Check that items 32-63 are in data node B.
	for i := types.KeyHash(numOfItems / 2); i < numOfItems; i++ {
		index, exists := keyHashes[i]
		requireT.True(exists)
		requireT.Equal(dataNodeBIndex, index)
	}
	// Check that items 64 is in data node A.
	index, exists := keyHashes[numOfItems]
	requireT.True(exists)
	requireT.Equal(dataNodeAIndex, index)

	// Tree structure verification succeeded.
	// Now we split the data nodes until there are `numOfItems` data nodes, each containing one item.

	for i := uint8(1); i <= numOfItems; i++ {
		for {
			v := s.NewEntry(TestKey[txtypes.Account]{
				Key:     txtypes.Account{i},
				KeyHash: types.KeyHash(i),
			}, StageData)
			s.Find(v)

			if v.nextDataNode == nil {
				break
			}

			requireT.NoError(s.SplitDataNode(v, false))
		}
	}

	// Verify that indeed, there are `numOfItems` data nodes, each containing one item.

	dataItems := map[types.KeyHash]struct{}{}
	for _, p := range pointerNode.Pointers {
		requireT.NotEqual(types.FreeAddress, p.VolatileAddress)

		keyHashes := dataNodeAssistant.KeyHashes(state.Node(p.VolatileAddress))
		var found bool
		for _, kh := range keyHashes {
			if kh != 0 {
				requireT.False(found)
				_, exists := dataItems[kh]
				requireT.False(exists)
				dataItems[kh] = struct{}{}
				found = true
			}
		}
		requireT.True(found)
	}
	requireT.Len(dataItems, numOfItems)
	for i := types.KeyHash(1); i <= numOfItems; i++ {
		_, exists := dataItems[i]
		requireT.True(exists)
	}

	// Verify that all the items have correct values.

	for i := uint8(1); i <= numOfItems; i++ {
		balance, exists := s.Query(TestKey[txtypes.Account]{
			Key:     txtypes.Account{i},
			KeyHash: types.KeyHash(i),
		})
		requireT.True(exists)
		requireT.Equal(txtypes.Amount(i), balance)
	}
}

// TestDataNodeSplitWithConflictResolution verifies that data nodes are allocated in the right order when there
// is a time to split them. This test assumes there are key hash conflicts to be resolved.
func TestDataNodeSplitWithConflictResolution(t *testing.T) {
	const (
		// It is selected this way so at the end each pointer references a data node containing one data item.
		numOfItems               = NumOfPointers
		keyHash    types.KeyHash = 1 // Same for all data items to create conflicts.
	)

	requireT := require.New(t)

	state, err := alloc.RunInTest(t, stateSize, nodesPerGroup)
	requireT.NoError(err)

	hashKeyFunc := func(key *txtypes.Account, buff []byte, level uint8) types.KeyHash {
		return types.KeyHash(key[0])
	}

	s := NewSpaceTest[txtypes.Account, txtypes.Amount](t, state, hashKeyFunc)

	// Store items in the root data node.

	for i := uint8(1); i <= numOfItems; i++ {
		v := s.NewEntry(TestKey[txtypes.Account]{
			Key:     txtypes.Account{i},
			KeyHash: keyHash,
		}, StagePointer0)
		requireT.NoError(s.SetKey(v, txtypes.Amount(i)))
	}

	// Convert root node into pointer node.

	v := s.NewEntry(TestKey[txtypes.Account]{
		Key:     txtypes.Account{0x01},
		KeyHash: types.KeyHash(1), // +1 to avoid 0
	}, StageData)
	requireT.NoError(s.AddPointerNode(v, true))

	// Verify the current structure of the tree.
	// Root node should be a pointer node with two data node children at indexes 0 and NumOfPointers / 2.
	// Each data node should contain numOfItems / 2 items. First node should contain items with hashes 1-31 and 64,
	// Second node should contain items with hashes 32-63.

	pointerNode := ProjectPointerNode(state.Node(s.Root().VolatileAddress))
	dataNodeAIndex := 0
	dataNodeBIndex := NumOfPointers / 2
	for i, p := range pointerNode.Pointers {
		if i == dataNodeAIndex || i == dataNodeBIndex {
			requireT.NotEqual(types.FreeAddress, p.VolatileAddress)
		} else {
			requireT.Equal(types.FreeAddress, p.VolatileAddress)
		}
	}

	dataNodeAssistant := s.DataNodeAssistant()
	dataNodeAKeyHashes := dataNodeAssistant.KeyHashes(state.Node(pointerNode.Pointers[dataNodeAIndex].VolatileAddress))
	dataNodeBKeyHashes := dataNodeAssistant.KeyHashes(state.Node(pointerNode.Pointers[dataNodeBIndex].VolatileAddress))

	keyHashes := map[types.KeyHash]int{}
	for _, kh := range dataNodeAKeyHashes {
		if kh != 0 {
			_, exists := keyHashes[kh]
			requireT.False(exists)
			keyHashes[kh] = dataNodeAIndex
		}
	}
	for _, kh := range dataNodeBKeyHashes {
		if kh != 0 {
			_, exists := keyHashes[kh]
			requireT.False(exists)
			keyHashes[kh] = dataNodeBIndex
		}
	}

	requireT.Len(keyHashes, numOfItems)
	// Check that items 1-31 are in data node A.
	for i := types.KeyHash(1); i < numOfItems/2; i++ {
		index, exists := keyHashes[i]
		requireT.True(exists)
		requireT.Equal(dataNodeAIndex, index)
	}
	// Check that items 32-63 are in data node B.
	for i := types.KeyHash(numOfItems / 2); i < numOfItems; i++ {
		index, exists := keyHashes[i]
		requireT.True(exists)
		requireT.Equal(dataNodeBIndex, index)
	}
	// Check that items 64 is in data node A.
	index, exists := keyHashes[numOfItems]
	requireT.True(exists)
	requireT.Equal(dataNodeAIndex, index)

	// Tree structure verification succeeded.
	// Now we split the data nodes until there are `numOfItems` data nodes, each containing one item.
	// First split is done with conflict resolution to generate non-conflicting key hashes.

	// Split everything without conflict resolution because hashes has been recomputed above.
	for i := uint8(1); i <= numOfItems; i++ {
		for {
			v := s.NewEntry(TestKey[txtypes.Account]{
				Key:     txtypes.Account{i},
				KeyHash: types.KeyHash(i),
			}, StageData)
			s.Find(v)

			if v.nextDataNode == nil {
				break
			}

			// We split with conflict resolving all the time to test that path of the logic, but provided hashing
			// function returns the same results all the time.
			requireT.NoError(s.SplitDataNode(v, true))
		}
	}

	// Verify that indeed, there are `numOfItems` data nodes, each containing one item.

	dataItems := map[types.KeyHash]struct{}{}
	for _, p := range pointerNode.Pointers {
		requireT.NotEqual(types.FreeAddress, p.VolatileAddress)

		keyHashes := dataNodeAssistant.KeyHashes(state.Node(p.VolatileAddress))
		var found bool
		for _, kh := range keyHashes {
			if kh != 0 {
				requireT.False(found)
				_, exists := dataItems[kh]
				requireT.False(exists)
				dataItems[kh] = struct{}{}
				found = true
			}
		}
		requireT.True(found)
	}
	requireT.Len(dataItems, numOfItems)
	for i := types.KeyHash(1); i <= numOfItems; i++ {
		_, exists := dataItems[i]
		requireT.True(exists)
	}

	// Verify that all the items have correct values.

	for i := uint8(1); i <= numOfItems; i++ {
		balance, exists := s.Query(TestKey[txtypes.Account]{
			Key:     txtypes.Account{i},
			KeyHash: types.KeyHash(i),
		})
		requireT.True(exists)
		requireT.Equal(txtypes.Amount(i), balance)
	}
}

// TestFindingAvailableFreeSlot verifies that free slot is found for non-existing item.
func TestFindingAvailableFreeSlot(t *testing.T) {
	requireT := require.New(t)

	state, err := alloc.RunInTest(t, stateSize, nodesPerGroup)
	requireT.NoError(err)

	s := NewSpaceTest[txtypes.Account, txtypes.Amount](t, state, hashKey)

	v1 := s.NewEntry(TestKey[txtypes.Account]{
		Key:     txtypes.Account{1},
		KeyHash: 1,
	}, StageData)
	requireT.NoError(s.SetKey(v1, 1))

	v2 := s.NewEntry(TestKey[txtypes.Account]{
		Key:     txtypes.Account{2},
		KeyHash: 2,
	}, StageData)
	s.Find(v2)
	requireT.NotNil(v2.keyHashP)
	requireT.Equal(types.KeyHash(0), *v2.keyHashP)
}

// TestFindStages verifies that locating data item is divided into three stages.
func TestFindStages(t *testing.T) {
	requireT := require.New(t)

	// This key hash means that item will always go to the pointer at index 0.
	const keyHash = 1 << 63

	state, err := alloc.RunInTest(t, stateSize, nodesPerGroup)
	requireT.NoError(err)

	s := NewSpaceTest[txtypes.Account, txtypes.Amount](t, state, hashKey)

	// Create levels in the tree.

	key := TestKey[txtypes.Account]{
		Key:     txtypes.Account{0x01},
		KeyHash: keyHash,
	}

	v := s.NewEntry(key, StageData)
	requireT.NoError(s.SetKey(v, txtypes.Amount(10)))

	for range 8 {
		requireT.NoError(s.AddPointerNode(v, false))
		s.Find(v)
	}

	requireT.Equal(uint8(8), v.level)

	// Test StagePointer0.

	v = s.NewEntry(key, StagePointer0)

	requireT.Equal(StagePointer0, v.stage)
	requireT.Equal(uint8(0), v.level)
	requireT.Nil(v.keyHashP)
	requireT.Nil(v.itemP)
	requireT.False(v.exists)

	s.Find(v)
	requireT.Equal(StagePointer1, v.stage)
	requireT.Equal(uint8(3), v.level)
	requireT.Nil(v.keyHashP)
	requireT.Nil(v.itemP)
	requireT.False(v.exists)

	s.Find(v)
	requireT.Equal(StageData, v.stage)
	requireT.Equal(uint8(8), v.level)
	requireT.Nil(v.keyHashP)
	requireT.Nil(v.itemP)
	requireT.False(v.exists)

	s.Find(v)
	requireT.Equal(StageData, v.stage)
	requireT.Equal(uint8(8), v.level)
	requireT.NotNil(v.keyHashP)
	requireT.NotNil(v.itemP)
	requireT.True(v.exists)

	requireT.Equal(key.Key, v.itemP.Key)
	requireT.Equal(txtypes.Amount(10), v.itemP.Value)
	requireT.Equal(key.KeyHash, *v.keyHashP)

	balance, exists := s.Query(key)
	requireT.True(exists)
	requireT.Equal(txtypes.Amount(10), balance)
}

// TestSwitchingFromMutableToImmutablePath verifies that when pointer node is added on mutable path, pending tree walk
// does not follow that pointer node but goes to the immutable path instead.
func TestSwitchingFromMutableToImmutablePath(t *testing.T) {
	requireT := require.New(t)

	// After first split this key hash stays in data node 0, but after second split it will go to the data node 16.
	const keyHash types.KeyHash = 16

	state, err := alloc.RunInTest(t, stateSize, nodesPerGroup)
	requireT.NoError(err)

	s := NewSpaceTest[txtypes.Account, txtypes.Amount](t, state, hashKey)

	// Create levels in the tree.

	key := TestKey[txtypes.Account]{
		Key:     txtypes.Account{0x01},
		KeyHash: keyHash,
	}

	v64 := s.NewEntry(TestKey[txtypes.Account]{
		Key:     txtypes.Account{0x01},
		KeyHash: types.KeyHash(64),
	}, StageData)
	requireT.NoError(s.SetKey(v64, txtypes.Amount(64)))

	v16 := s.NewEntry(key, StageData)
	requireT.NoError(s.SetKey(v16, txtypes.Amount(16)))
	requireT.NoError(s.AddPointerNode(v16, false))

	// Now there are data nodes 0 and 32. The key hashes 0 and 16 should be in data node 0.

	pointerNode := ProjectPointerNode(state.Node(s.Root().VolatileAddress))
	requireT.NotEqual(types.FreeAddress, pointerNode.Pointers[0].VolatileAddress)
	requireT.Equal(types.FreeAddress, pointerNode.Pointers[16].VolatileAddress)

	dataNodeAssistant := s.DataNodeAssistant()
	keyHashes := dataNodeAssistant.KeyHashes(state.Node(pointerNode.Pointers[0].VolatileAddress))

	requireT.Equal(types.KeyHash(64), keyHashes[0])
	requireT.Equal(keyHash, keyHashes[1])

	// Let's locate the key hash 16 in the data item 0.

	v16Read := s.NewEntry(key, StageData)
	s.Find(v16Read)
	// Verify the next expected data node address.
	requireT.Equal(&pointerNode.Pointers[16].VolatileAddress, v16Read.nextDataNode)

	v16Exists := s.NewEntry(key, StageData)
	s.Find(v16Exists)

	v16Delete := s.NewEntry(key, StageData)
	s.Find(v16Delete)

	// Now let's split data node 0 using key hash 64.

	v64 = s.NewEntry(TestKey[txtypes.Account]{
		Key:     txtypes.Account{0x01},
		KeyHash: types.KeyHash(64),
	}, StageData)
	s.Find(v64)
	requireT.NoError(s.SplitDataNode(v64, false))

	// The key hash 16 should be moved to the brand new data node 16.

	// Slot occupied by key hash 16 is free now.
	requireT.Equal(types.KeyHash(0), keyHashes[1])

	// Data node 16 has been created.
	requireT.NotEqual(types.FreeAddress, pointerNode.Pointers[16].VolatileAddress)

	// And key hash 16 has been moved there.
	keyHashes = dataNodeAssistant.KeyHashes(state.Node(pointerNode.Pointers[16].VolatileAddress))
	requireT.Equal(keyHash, keyHashes[1])

	// Now let's add a pointer node at the position of key hash 64.

	requireT.NoError(s.AddPointerNode(v64, false))
	requireT.True(pointerNode.Pointers[0].VolatileAddress.IsSet(flagPointerNode))
	requireT.True(v16Read.storeRequest.Store[v16Read.storeRequest.PointersToStore-1].Pointer.VolatileAddress.
		IsSet(flagPointerNode))

	// We are now in situation where key hash 16 is no longer in the place pointed to by v16.
	// When walking the tree now, it should not follow the current pointer node, but go back and switch
	// to the immutable path.

	requireT.Equal(txtypes.Amount(16), s.ReadKey(v16Read))
	requireT.Nil(v16Read.nextDataNode)

	requireT.True(s.KeyExists(v16Exists))
	requireT.Nil(v16Exists.nextDataNode)

	s.DeleteKey(v16Delete)
	requireT.Nil(v16Delete.nextDataNode)

	balance, exists := s.Query(key)
	requireT.False(exists)
	requireT.Equal(txtypes.Amount(0), balance)
}

// TestExistsReturnsFalseIfKeyHashIsDifferent verifies that after changing the key hash Exists returns false.
func TestExistsReturnsFalseIfKeyHashIsDifferent(t *testing.T) {
	requireT := require.New(t)

	// This is the key placed in the middle.
	var key = TestKey[txtypes.Account]{
		Key:     txtypes.Account{5},
		KeyHash: 5,
	}

	state, err := alloc.RunInTest(t, stateSize, nodesPerGroup)
	requireT.NoError(err)

	s := NewSpaceTest[txtypes.Account, txtypes.Amount](t, state, hashKey)

	for i := types.KeyHash(1); i <= 2*key.KeyHash; i++ {
		v := s.NewEntry(TestKey[txtypes.Account]{
			Key:     txtypes.Account{uint8(i)},
			KeyHash: i,
		}, StageData)
		requireT.NoError(s.SetKey(v, txtypes.Amount(i)))
	}

	v5 := s.NewEntry(key, StageData)
	s.Find(v5)

	v5A := s.NewEntry(key, StageData)
	s.Find(v5A)

	// Test that false is returned if hash is different.

	*v5.keyHashP = 1
	requireT.False(s.KeyExists(v5A))
	// v5A now points to first free slot.
	requireT.Equal(types.KeyHash(0), *v5A.keyHashP)
}

// TestExistsReturnsFalseIfKeyIsDifferent verifies that after changing the key Exists returns false.
func TestExistsReturnsFalseIfKeyIsDifferent(t *testing.T) {
	requireT := require.New(t)

	// This is the key placed in the middle.
	var key = TestKey[txtypes.Account]{
		Key:     txtypes.Account{5},
		KeyHash: 5,
	}

	state, err := alloc.RunInTest(t, stateSize, nodesPerGroup)
	requireT.NoError(err)

	s := NewSpaceTest[txtypes.Account, txtypes.Amount](t, state, hashKey)

	for i := types.KeyHash(1); i <= 2*key.KeyHash; i++ {
		v := s.NewEntry(TestKey[txtypes.Account]{
			Key:     txtypes.Account{uint8(i)},
			KeyHash: i,
		}, StageData)
		requireT.NoError(s.SetKey(v, txtypes.Amount(i)))
	}

	v5 := s.NewEntry(key, StageData)
	s.Find(v5)

	v5A := s.NewEntry(key, StageData)
	s.Find(v5A)

	// Test that false is returned if key is different.

	v5.itemP.Key = txtypes.Account{0x01}
	requireT.False(s.KeyExists(v5A))
	// v5A now points to first free slot.
	requireT.Equal(types.KeyHash(0), *v5A.keyHashP)
}

// TestExistsReturnsTrueAfterReplacingItem verifies that Exists returns true if the item is moved to another slot
// and the previous slot is now occupied by another item.
func TestExistsReturnsTrueAfterReplacingItem(t *testing.T) {
	requireT := require.New(t)

	// This is the key placed in the middle.
	var key = TestKey[txtypes.Account]{
		Key:     txtypes.Account{5},
		KeyHash: 5,
	}

	state, err := alloc.RunInTest(t, stateSize, nodesPerGroup)
	requireT.NoError(err)

	s := NewSpaceTest[txtypes.Account, txtypes.Amount](t, state, hashKey)

	for i := types.KeyHash(1); i <= 2*key.KeyHash; i++ {
		v := s.NewEntry(TestKey[txtypes.Account]{
			Key:     txtypes.Account{uint8(i)},
			KeyHash: i,
		}, StageData)
		requireT.NoError(s.SetKey(v, txtypes.Amount(i)))
	}

	v5 := s.NewEntry(key, StageData)
	s.Find(v5)

	v5A := s.NewEntry(key, StageData)
	s.Find(v5A)

	v5B := s.NewEntry(key, StageData)
	s.Find(v5B)

	// Test checking replaced item.

	// Delete v5A.
	s.DeleteKey(v5A)

	// This item is inserted on first free slot, which is the one previously occupied by v5A.
	key100 := TestKey[txtypes.Account]{
		Key:     txtypes.Account{100},
		KeyHash: 100,
	}
	v100 := s.NewEntry(key100, StageData)
	requireT.NoError(s.SetKey(v100, txtypes.Amount(100)))

	// Now v5B points to invalid item.
	requireT.Equal(types.KeyHash(100), *v5B.keyHashP)

	// Let's insert v5 on another free position.
	v5B2 := s.NewEntry(key, StageData)
	requireT.NoError(s.SetKey(v5B2, txtypes.Amount(5)))
	requireT.Equal(types.KeyHash(100), *v5B.keyHashP)

	// When checking, it will refer the right slot.
	requireT.True(s.KeyExists(v5B))
	requireT.Equal(key.KeyHash, *v5B.keyHashP)
	requireT.Equal(key.KeyHash, *v5B2.keyHashP)
	requireT.Equal(unsafe.Pointer(v5B.keyHashP), unsafe.Pointer(v5B2.keyHashP))
}

// TestExistsReturnsTrueAfterMovingItem verifies that Exists returns true if the item is moved to another slot
// and the previous slot remains free.
func TestExistsReturnsTrueAfterMovingItem(t *testing.T) {
	requireT := require.New(t)

	// This is the key placed in the middle.
	var key = TestKey[txtypes.Account]{
		Key:     txtypes.Account{5},
		KeyHash: 5,
	}

	state, err := alloc.RunInTest(t, stateSize, nodesPerGroup)
	requireT.NoError(err)

	s := NewSpaceTest[txtypes.Account, txtypes.Amount](t, state, hashKey)

	for i := types.KeyHash(1); i <= 2*key.KeyHash; i++ {
		v := s.NewEntry(TestKey[txtypes.Account]{
			Key:     txtypes.Account{uint8(i)},
			KeyHash: i,
		}, StageData)
		requireT.NoError(s.SetKey(v, txtypes.Amount(i)))
	}

	v5 := s.NewEntry(key, StageData)
	s.Find(v5)

	v5A := s.NewEntry(key, StageData)
	s.Find(v5A)

	v5B := s.NewEntry(key, StageData)
	s.Find(v5B)

	// Test checking moved item.

	// Delete v5A.
	s.DeleteKey(v5A)

	// This item is inserted on first free slot, which is the one previously occupied by v5A.
	key100 := TestKey[txtypes.Account]{
		Key:     txtypes.Account{100},
		KeyHash: 100,
	}
	v100 := s.NewEntry(key100, StageData)
	requireT.NoError(s.SetKey(v100, txtypes.Amount(100)))

	// Now v5B points to invalid item.
	requireT.Equal(types.KeyHash(100), *v5B.keyHashP)

	// Let's insert v5 on another free position.
	v5B2 := s.NewEntry(key, StageData)
	requireT.NoError(s.SetKey(v5B2, txtypes.Amount(5)))
	requireT.Equal(types.KeyHash(100), *v5B.keyHashP)

	// Let's delete v100 to make the original slot free.
	s.DeleteKey(v100)
	requireT.Equal(types.KeyHash(0), *v100.keyHashP)
	requireT.Equal(types.KeyHash(0), *v5B.keyHashP)

	// When checking, it will refer the right slot.
	requireT.True(s.KeyExists(v5B))
	requireT.Equal(key.KeyHash, *v5B.keyHashP)
	requireT.Equal(key.KeyHash, *v5B2.keyHashP)
	requireT.Equal(unsafe.Pointer(v5B.keyHashP), unsafe.Pointer(v5B2.keyHashP))
}

// TestExistsReturnsFalseIfSlotHasNotBeenFound verifies that Exists returns false if slot has not been found.
func TestExistsReturnsFalseIfSlotHasNotBeenFound(t *testing.T) {
	requireT := require.New(t)

	state, err := alloc.RunInTest(t, stateSize, nodesPerGroup)
	requireT.NoError(err)

	s := NewSpaceTest[txtypes.Account, txtypes.Amount](t, state, hashKey)

	v := s.NewEntry(TestKey[txtypes.Account]{
		Key:     txtypes.Account{0x01},
		KeyHash: types.KeyHash(1),
	}, StageData)
	requireT.NoError(s.SetKey(v, txtypes.Amount(1)))

	// Check false is returned if we were not able to find slot for an item.

	// Set all slots as busy.
	dataNodeAssistant := s.DataNodeAssistant()
	keyHashes := dataNodeAssistant.KeyHashes(state.Node(s.Root().VolatileAddress))
	for i := range keyHashes {
		keyHashes[i] = 1
	}

	// Check on nil slot.
	key100 := TestKey[txtypes.Account]{
		Key:     txtypes.Account{100},
		KeyHash: 100,
	}
	v100 := s.NewEntry(key100, StageData)
	s.Find(v100)
	requireT.Nil(v100.keyHashP)
	requireT.False(s.KeyExists(v100))
}

// TestReadReturnsDefaultValueIfKeyHashIsDifferent verifies that after changing the key hash Read returns default value.
func TestReadReturnsDefaultValueIfKeyHashIsDifferent(t *testing.T) {
	requireT := require.New(t)

	// This is the key placed in the middle.
	var key = TestKey[txtypes.Account]{
		Key:     txtypes.Account{5},
		KeyHash: 5,
	}

	state, err := alloc.RunInTest(t, stateSize, nodesPerGroup)
	requireT.NoError(err)

	s := NewSpaceTest[txtypes.Account, txtypes.Amount](t, state, hashKey)

	for i := types.KeyHash(1); i <= 2*key.KeyHash; i++ {
		v := s.NewEntry(TestKey[txtypes.Account]{
			Key:     txtypes.Account{uint8(i)},
			KeyHash: i,
		}, StageData)
		requireT.NoError(s.SetKey(v, txtypes.Amount(i)))
	}

	v5 := s.NewEntry(key, StageData)
	s.Find(v5)

	v5A := s.NewEntry(key, StageData)
	s.Find(v5A)

	// Test that default value is returned if hash is different.

	*v5.keyHashP = 1
	requireT.Equal(txtypes.Amount(0), s.ReadKey(v5A))
	// v5A now points to first free slot.
	requireT.Equal(types.KeyHash(0), *v5A.keyHashP)
}

// TestReadReturnsDefaultValueIfKeyIsDifferent verifies that after changing the key Read returns default value.
func TestReadReturnsDefaultValueIfKeyIsDifferent(t *testing.T) {
	requireT := require.New(t)

	// This is the key placed in the middle.
	var key = TestKey[txtypes.Account]{
		Key:     txtypes.Account{5},
		KeyHash: 5,
	}

	state, err := alloc.RunInTest(t, stateSize, nodesPerGroup)
	requireT.NoError(err)

	s := NewSpaceTest[txtypes.Account, txtypes.Amount](t, state, hashKey)

	for i := types.KeyHash(1); i <= 2*key.KeyHash; i++ {
		v := s.NewEntry(TestKey[txtypes.Account]{
			Key:     txtypes.Account{uint8(i)},
			KeyHash: i,
		}, StageData)
		requireT.NoError(s.SetKey(v, txtypes.Amount(i)))
	}

	v5 := s.NewEntry(key, StageData)
	s.Find(v5)

	v5A := s.NewEntry(key, StageData)
	s.Find(v5A)

	// Test that default value is returned if key is different.

	v5.itemP.Key = txtypes.Account{0x01}
	requireT.Equal(txtypes.Amount(0), s.ReadKey(v5A))
	// v5A now points to first free slot.
	requireT.Equal(types.KeyHash(0), *v5A.keyHashP)
}

// TestReadReturnsCorrectValueAfterReplacingItem verifies that Read returns expected value if the item is moved
// to another slot and the previous slot is now occupied by another item.
func TestReadReturnsCorrectValueAfterReplacingItem(t *testing.T) {
	requireT := require.New(t)

	// This is the key placed in the middle.
	var key = TestKey[txtypes.Account]{
		Key:     txtypes.Account{5},
		KeyHash: 5,
	}

	state, err := alloc.RunInTest(t, stateSize, nodesPerGroup)
	requireT.NoError(err)

	s := NewSpaceTest[txtypes.Account, txtypes.Amount](t, state, hashKey)

	for i := types.KeyHash(1); i <= 2*key.KeyHash; i++ {
		v := s.NewEntry(TestKey[txtypes.Account]{
			Key:     txtypes.Account{uint8(i)},
			KeyHash: i,
		}, StageData)
		requireT.NoError(s.SetKey(v, txtypes.Amount(i)))
	}

	v5 := s.NewEntry(key, StageData)
	s.Find(v5)

	v5A := s.NewEntry(key, StageData)
	s.Find(v5A)

	v5B := s.NewEntry(key, StageData)
	s.Find(v5B)

	// Test checking replaced item.

	// Delete v5A.
	s.DeleteKey(v5A)

	// This item is inserted on first free slot, which is the one previously occupied by v5A.
	key100 := TestKey[txtypes.Account]{
		Key:     txtypes.Account{100},
		KeyHash: 100,
	}
	v100 := s.NewEntry(key100, StageData)
	requireT.NoError(s.SetKey(v100, txtypes.Amount(100)))

	// Now v5B points to invalid item.
	requireT.Equal(types.KeyHash(100), *v5B.keyHashP)

	// Let's insert v5 on another free position.
	v5B2 := s.NewEntry(key, StageData)
	requireT.NoError(s.SetKey(v5B2, txtypes.Amount(5)))
	requireT.Equal(types.KeyHash(100), *v5B.keyHashP)

	// When checking, it will refer the right slot.
	requireT.Equal(txtypes.Amount(5), s.ReadKey(v5B))
	requireT.Equal(key.KeyHash, *v5B.keyHashP)
	requireT.Equal(key.KeyHash, *v5B2.keyHashP)
	requireT.Equal(unsafe.Pointer(v5B.keyHashP), unsafe.Pointer(v5B2.keyHashP))
}

// TestReadReturnsCorrectValueAfterMovingItem verifies that Read returns expected value if the item is moved
// to another slot and the previous slot remains free.
func TestReadReturnsCorrectValueAfterMovingItem(t *testing.T) {
	requireT := require.New(t)

	// This is the key placed in the middle.
	var key = TestKey[txtypes.Account]{
		Key:     txtypes.Account{5},
		KeyHash: 5,
	}

	state, err := alloc.RunInTest(t, stateSize, nodesPerGroup)
	requireT.NoError(err)

	s := NewSpaceTest[txtypes.Account, txtypes.Amount](t, state, hashKey)

	for i := types.KeyHash(1); i <= 2*key.KeyHash; i++ {
		v := s.NewEntry(TestKey[txtypes.Account]{
			Key:     txtypes.Account{uint8(i)},
			KeyHash: i,
		}, StageData)
		requireT.NoError(s.SetKey(v, txtypes.Amount(i)))
	}

	v5 := s.NewEntry(key, StageData)
	s.Find(v5)

	v5A := s.NewEntry(key, StageData)
	s.Find(v5A)

	v5B := s.NewEntry(key, StageData)
	s.Find(v5B)

	// Test checking moved item.

	// Delete v5A.
	s.DeleteKey(v5A)

	// This item is inserted on first free slot, which is the one previously occupied by v5A.
	key100 := TestKey[txtypes.Account]{
		Key:     txtypes.Account{100},
		KeyHash: 100,
	}
	v100 := s.NewEntry(key100, StageData)
	requireT.NoError(s.SetKey(v100, txtypes.Amount(100)))

	// Now v5B points to invalid item.
	requireT.Equal(types.KeyHash(100), *v5B.keyHashP)

	// Let's insert v5 on another free position.
	v5B2 := s.NewEntry(key, StageData)
	requireT.NoError(s.SetKey(v5B2, txtypes.Amount(5)))
	requireT.Equal(types.KeyHash(100), *v5B.keyHashP)

	// Let's delete v100 to make the original slot free.
	s.DeleteKey(v100)
	requireT.Equal(types.KeyHash(0), *v100.keyHashP)
	requireT.Equal(types.KeyHash(0), *v5B.keyHashP)

	// When checking, it will refer the right slot.
	requireT.Equal(txtypes.Amount(5), s.ReadKey(v5B))
	requireT.Equal(key.KeyHash, *v5B.keyHashP)
	requireT.Equal(key.KeyHash, *v5B2.keyHashP)
	requireT.Equal(unsafe.Pointer(v5B.keyHashP), unsafe.Pointer(v5B2.keyHashP))
}

// TestReadReturnsDefaultValueIfSlotHasNotBeenFound verifies that Read returns default value if slot has not been found.
func TestReadReturnsDefaultValueIfSlotHasNotBeenFound(t *testing.T) {
	requireT := require.New(t)

	state, err := alloc.RunInTest(t, stateSize, nodesPerGroup)
	requireT.NoError(err)

	s := NewSpaceTest[txtypes.Account, txtypes.Amount](t, state, hashKey)

	v := s.NewEntry(TestKey[txtypes.Account]{
		Key:     txtypes.Account{0x01},
		KeyHash: types.KeyHash(1),
	}, StageData)
	requireT.NoError(s.SetKey(v, txtypes.Amount(1)))

	// Check false is returned if we were not able to find slot for an item.

	// Set all slots as busy.
	dataNodeAssistant := s.DataNodeAssistant()
	keyHashes := dataNodeAssistant.KeyHashes(state.Node(s.Root().VolatileAddress))
	for i := range keyHashes {
		keyHashes[i] = 1
	}

	// Check on nil slot.
	key100 := TestKey[txtypes.Account]{
		Key:     txtypes.Account{100},
		KeyHash: 100,
	}
	v100 := s.NewEntry(key100, StageData)
	s.Find(v100)
	requireT.Nil(v100.keyHashP)
	requireT.Equal(txtypes.Amount(0), s.ReadKey(v100))
}

// TestDeletingOnEmptySpace verifies that deleting on empty space works and does nothing.
func TestDeletingOnEmptySpace(t *testing.T) {
	requireT := require.New(t)

	state, err := alloc.RunInTest(t, stateSize, nodesPerGroup)
	requireT.NoError(err)

	s := NewSpaceTest[txtypes.Account, txtypes.Amount](t, state, hashKey)

	v5 := s.NewEntry(TestKey[txtypes.Account]{
		Key:     txtypes.Account{5},
		KeyHash: 5,
	}, StageData)
	s.Find(v5)
	s.DeleteKey(v5)
	requireT.Equal(types.FreeAddress, s.Root().VolatileAddress)
}

// TestDeleteDoesNothingIfKeyHashIsDifferent verifies that after changing the key hash Delete does nothing.
func TestDeleteDoesNothingIfKeyHashIsDifferent(t *testing.T) {
	requireT := require.New(t)

	// This is the key placed in the middle.
	var key = TestKey[txtypes.Account]{
		Key:     txtypes.Account{5},
		KeyHash: 5,
	}

	state, err := alloc.RunInTest(t, stateSize, nodesPerGroup)
	requireT.NoError(err)

	s := NewSpaceTest[txtypes.Account, txtypes.Amount](t, state, hashKey)

	for i := types.KeyHash(1); i <= 2*key.KeyHash; i++ {
		v := s.NewEntry(TestKey[txtypes.Account]{
			Key:     txtypes.Account{uint8(i)},
			KeyHash: i,
		}, StageData)
		requireT.NoError(s.SetKey(v, txtypes.Amount(i)))
	}

	v5 := s.NewEntry(key, StageData)
	s.Find(v5)

	v5A := s.NewEntry(key, StageData)
	s.Find(v5A)

	// Test that nothing happens if hash is different.

	*v5.keyHashP = 1
	s.DeleteKey(v5A)
	requireT.Equal(types.KeyHash(1), *v5.keyHashP)
	*v5.keyHashP = key.KeyHash
	// v5A now points to first free slot.
	requireT.Equal(types.KeyHash(0), *v5A.keyHashP)

	for i := types.KeyHash(1); i <= 2*key.KeyHash; i++ {
		balance, exists := s.Query(TestKey[txtypes.Account]{
			Key:     txtypes.Account{uint8(i)},
			KeyHash: i,
		})
		requireT.True(exists)
		requireT.Equal(txtypes.Amount(i), balance)
	}
}

// TestDeleteDoesNothingIfKeyIsDifferent verifies that after changing the key Delete does nothing.
func TestDeleteDoesNothingIfKeyIsDifferent(t *testing.T) {
	requireT := require.New(t)

	// This is the key placed in the middle.
	var key = TestKey[txtypes.Account]{
		Key:     txtypes.Account{5},
		KeyHash: 5,
	}

	state, err := alloc.RunInTest(t, stateSize, nodesPerGroup)
	requireT.NoError(err)

	s := NewSpaceTest[txtypes.Account, txtypes.Amount](t, state, hashKey)

	for i := types.KeyHash(1); i <= 2*key.KeyHash; i++ {
		v := s.NewEntry(TestKey[txtypes.Account]{
			Key:     txtypes.Account{uint8(i)},
			KeyHash: i,
		}, StageData)
		requireT.NoError(s.SetKey(v, txtypes.Amount(i)))
	}

	v5 := s.NewEntry(key, StageData)
	s.Find(v5)

	v5A := s.NewEntry(key, StageData)
	s.Find(v5A)

	// Test that nothing happens if key is different.

	v5.itemP.Key = txtypes.Account{0x01}
	s.DeleteKey(v5A)
	requireT.Equal(key.KeyHash, *v5.keyHashP)
	requireT.Equal(txtypes.Account{0x01}, v5.itemP.Key)
	v5.itemP.Key = key.Key
	// v5A now points to first free slot.
	requireT.Equal(types.KeyHash(0), *v5A.keyHashP)

	for i := types.KeyHash(1); i <= 2*key.KeyHash; i++ {
		balance, exists := s.Query(TestKey[txtypes.Account]{
			Key:     txtypes.Account{uint8(i)},
			KeyHash: i,
		})
		requireT.True(exists)
		requireT.Equal(txtypes.Amount(i), balance)
	}
}

// TestDeleteDoesNothingIfSlotIsFree verifies Delete does nothing if slot is free.
func TestDeleteDoesNothingIfSlotIsFree(t *testing.T) {
	requireT := require.New(t)

	// This is the key placed in the middle.
	var key = TestKey[txtypes.Account]{
		Key:     txtypes.Account{5},
		KeyHash: 5,
	}

	state, err := alloc.RunInTest(t, stateSize, nodesPerGroup)
	requireT.NoError(err)

	s := NewSpaceTest[txtypes.Account, txtypes.Amount](t, state, hashKey)

	for i := types.KeyHash(1); i <= 2*key.KeyHash; i++ {
		v := s.NewEntry(TestKey[txtypes.Account]{
			Key:     txtypes.Account{uint8(i)},
			KeyHash: i,
		}, StageData)
		requireT.NoError(s.SetKey(v, txtypes.Amount(i)))
	}

	v5 := s.NewEntry(key, StageData)
	s.Find(v5)

	v5A := s.NewEntry(key, StageData)
	s.Find(v5A)

	// Test that nothing happens if slot is free.

	*v5.keyHashP = 0
	s.DeleteKey(v5A)
	requireT.Equal(types.KeyHash(0), *v5.keyHashP)
	*v5.keyHashP = key.KeyHash
	// v5C still points to the same slot.
	requireT.Equal(key.KeyHash, *v5A.keyHashP)

	for i := types.KeyHash(1); i <= 2*key.KeyHash; i++ {
		balance, exists := s.Query(TestKey[txtypes.Account]{
			Key:     txtypes.Account{uint8(i)},
			KeyHash: i,
		})
		requireT.True(exists)
		requireT.Equal(txtypes.Amount(i), balance)
	}
}

// TestDeleteOnReplacedItem verifies that item is correctly deleted after moving it to another slot and putting another
// item into the previous one.
func TestDeleteOnReplacedItem(t *testing.T) {
	requireT := require.New(t)

	// This is the key placed in the middle.
	var key = TestKey[txtypes.Account]{
		Key:     txtypes.Account{5},
		KeyHash: 5,
	}

	state, err := alloc.RunInTest(t, stateSize, nodesPerGroup)
	requireT.NoError(err)

	s := NewSpaceTest[txtypes.Account, txtypes.Amount](t, state, hashKey)

	for i := types.KeyHash(1); i <= 2*key.KeyHash; i++ {
		v := s.NewEntry(TestKey[txtypes.Account]{
			Key:     txtypes.Account{uint8(i)},
			KeyHash: i,
		}, StageData)
		requireT.NoError(s.SetKey(v, txtypes.Amount(i)))
	}

	v5 := s.NewEntry(key, StageData)
	s.Find(v5)

	v5A := s.NewEntry(key, StageData)
	s.Find(v5A)

	v5B := s.NewEntry(key, StageData)
	s.Find(v5B)

	// Test deleting replaced item.

	// Delete v5A.
	s.DeleteKey(v5A)

	// This item is inserted on first free slot, which is the one previously occupied by v5A.
	key100 := TestKey[txtypes.Account]{
		Key:     txtypes.Account{100},
		KeyHash: 100,
	}
	v100 := s.NewEntry(key100, StageData)
	requireT.NoError(s.SetKey(v100, txtypes.Amount(100)))

	// Now v5E points to invalid item.
	requireT.Equal(types.KeyHash(100), *v5B.keyHashP)

	// Let's insert v5 on another free position.
	v5B2 := s.NewEntry(key, StageData)
	requireT.NoError(s.SetKey(v5B2, txtypes.Amount(5)))
	requireT.Equal(types.KeyHash(100), *v5B.keyHashP)

	// When deleting, it will free the right slot.
	s.DeleteKey(v5B)
	requireT.Equal(types.KeyHash(0), *v5B.keyHashP)
	requireT.Equal(types.KeyHash(0), *v5B2.keyHashP)
	requireT.Equal(unsafe.Pointer(v5B.keyHashP), unsafe.Pointer(v5B2.keyHashP))

	// v100 still exists.
	balance, exists := s.Query(key100)
	requireT.True(exists)
	requireT.Equal(txtypes.Amount(100), balance)

	// v5 doesn't exist.
	balance, exists = s.Query(key)
	requireT.False(exists)
	requireT.Equal(txtypes.Amount(0), balance)
}

// TestDeleteOnMovedItem verifies that item is correctly deleted after moving it to another slot and leaving
// the previous one free.
func TestDeleteOnMovedItem(t *testing.T) {
	requireT := require.New(t)

	// This is the key placed in the middle.
	var key = TestKey[txtypes.Account]{
		Key:     txtypes.Account{5},
		KeyHash: 5,
	}

	state, err := alloc.RunInTest(t, stateSize, nodesPerGroup)
	requireT.NoError(err)

	s := NewSpaceTest[txtypes.Account, txtypes.Amount](t, state, hashKey)

	for i := types.KeyHash(1); i <= 2*key.KeyHash; i++ {
		v := s.NewEntry(TestKey[txtypes.Account]{
			Key:     txtypes.Account{uint8(i)},
			KeyHash: i,
		}, StageData)
		requireT.NoError(s.SetKey(v, txtypes.Amount(i)))
	}

	v5 := s.NewEntry(key, StageData)
	s.Find(v5)

	v5A := s.NewEntry(key, StageData)
	s.Find(v5A)

	v5B := s.NewEntry(key, StageData)
	s.Find(v5B)

	// Test deleting moved item.

	// Delete v5A.
	s.DeleteKey(v5A)

	// This item is inserted on first free slot, which is the one previously occupied by v5A.
	key100 := TestKey[txtypes.Account]{
		Key:     txtypes.Account{100},
		KeyHash: 100,
	}
	v100 := s.NewEntry(key100, StageData)
	requireT.NoError(s.SetKey(v100, txtypes.Amount(100)))

	// Now v5B points to invalid item.
	requireT.Equal(types.KeyHash(100), *v5B.keyHashP)

	// Let's insert v5 on another free position.
	v5B2 := s.NewEntry(key, StageData)
	requireT.NoError(s.SetKey(v5B2, txtypes.Amount(5)))
	requireT.Equal(types.KeyHash(100), *v5B.keyHashP)

	// Let's delete v100 to make the original slot free.
	s.DeleteKey(v100)
	requireT.Equal(types.KeyHash(0), *v100.keyHashP)
	requireT.Equal(types.KeyHash(0), *v5B.keyHashP)

	// When deleting, it will refer the right slot.
	s.DeleteKey(v5B)
	requireT.Equal(types.KeyHash(0), *v5B.keyHashP)
	requireT.Equal(types.KeyHash(0), *v5B2.keyHashP)
	requireT.Equal(unsafe.Pointer(v5B.keyHashP), unsafe.Pointer(v5B2.keyHashP))
}

// TestDeleteDoesNothingIfSlotHasNotBeenFound verifies that Delete does nothing if slot has not been found.
func TestDeleteDoesNothingIfSlotHasNotBeenFound(t *testing.T) {
	requireT := require.New(t)

	// This is the key placed in the middle.
	var key = TestKey[txtypes.Account]{
		Key:     txtypes.Account{5},
		KeyHash: 5,
	}

	state, err := alloc.RunInTest(t, stateSize, nodesPerGroup)
	requireT.NoError(err)

	s := NewSpaceTest[txtypes.Account, txtypes.Amount](t, state, hashKey)

	for i := types.KeyHash(1); i <= 2*key.KeyHash; i++ {
		v := s.NewEntry(TestKey[txtypes.Account]{
			Key:     txtypes.Account{uint8(i)},
			KeyHash: i,
		}, StageData)
		requireT.NoError(s.SetKey(v, txtypes.Amount(i)))
	}

	// Try to delete if we were not able to find slot for an item.

	// Set all slots as busy.
	dataNodeAssistant := s.DataNodeAssistant()
	keyHashes := dataNodeAssistant.KeyHashes(state.Node(s.Root().VolatileAddress))
	for i := range keyHashes {
		keyHashes[i] = 1
	}

	// Delete on nil slot.
	key100 := TestKey[txtypes.Account]{
		Key:     txtypes.Account{100},
		KeyHash: 100,
	}
	v100 := s.NewEntry(key100, StageData)
	s.Find(v100)
	requireT.Nil(v100.keyHashP)
	s.DeleteKey(v100)
	requireT.Nil(v100.keyHashP)

	// Verify that nothing has been deleted.
	for _, kh := range keyHashes {
		requireT.Equal(types.KeyHash(1), kh)
	}
}

// TestSetFindsAnotherSlotIfKeyHashIsDifferent verifies that new item is placed in different slot if the key hash
// on the current one differs.
func TestSetFindsAnotherSlotIfKeyHashIsDifferent(t *testing.T) {
	requireT := require.New(t)

	// This is the key placed in the middle.
	var key = TestKey[txtypes.Account]{
		Key:     txtypes.Account{5},
		KeyHash: 5,
	}

	state, err := alloc.RunInTest(t, stateSize, nodesPerGroup)
	requireT.NoError(err)

	s := NewSpaceTest[txtypes.Account, txtypes.Amount](t, state, hashKey)

	for i := types.KeyHash(1); i <= 2*key.KeyHash; i++ {
		v := s.NewEntry(TestKey[txtypes.Account]{
			Key:     txtypes.Account{uint8(i)},
			KeyHash: i,
		}, StageData)
		requireT.NoError(s.SetKey(v, txtypes.Amount(i)))
	}

	v5 := s.NewEntry(key, StageData)
	s.Find(v5)

	v5A := s.NewEntry(key, StageData)
	s.Find(v5A)

	*v5.keyHashP = 1
	requireT.NoError(s.SetKey(v5A, txtypes.Amount(10)))
	requireT.Equal(types.KeyHash(1), *v5.keyHashP)
	requireT.Equal(txtypes.Amount(5), v5.itemP.Value)
	// v5A now points to different slot.
	requireT.NotEqual(unsafe.Pointer(v5.keyHashP), unsafe.Pointer(v5A.keyHashP))
}

// TestSetFindsAnotherSlotIfKeyIsDifferent verifies that new item is placed in different slot if the key
// on the current one differs.
func TestSetFindsAnotherSlotIfKeyIsDifferent(t *testing.T) {
	requireT := require.New(t)

	// This is the key placed in the middle.
	var key = TestKey[txtypes.Account]{
		Key:     txtypes.Account{5},
		KeyHash: 5,
	}

	state, err := alloc.RunInTest(t, stateSize, nodesPerGroup)
	requireT.NoError(err)

	s := NewSpaceTest[txtypes.Account, txtypes.Amount](t, state, hashKey)

	for i := types.KeyHash(1); i <= 2*key.KeyHash; i++ {
		v := s.NewEntry(TestKey[txtypes.Account]{
			Key:     txtypes.Account{uint8(i)},
			KeyHash: i,
		}, StageData)
		requireT.NoError(s.SetKey(v, txtypes.Amount(i)))
	}

	v5 := s.NewEntry(key, StageData)
	s.Find(v5)

	v5A := s.NewEntry(key, StageData)
	s.Find(v5A)

	v5.itemP.Key = txtypes.Account{0x01}
	requireT.NoError(s.SetKey(v5A, txtypes.Amount(10)))
	requireT.Equal(key.KeyHash, *v5.keyHashP)
	requireT.Equal(txtypes.Account{0x01}, v5.itemP.Key)
	// v5A now points to another slot.
	requireT.Equal(txtypes.Amount(5), v5.itemP.Value)
	requireT.Equal(txtypes.Amount(10), v5A.itemP.Value)
	requireT.NotEqual(unsafe.Pointer(v5.keyHashP), unsafe.Pointer(v5A.keyHashP))
}

// TestSetOnReplacedItem verifies that item is correctly set after moving it to another slot and putting another
// item into the previous one.
func TestSetOnReplacedItem(t *testing.T) {
	requireT := require.New(t)

	// This is the key placed in the middle.
	var key = TestKey[txtypes.Account]{
		Key:     txtypes.Account{5},
		KeyHash: 5,
	}

	state, err := alloc.RunInTest(t, stateSize, nodesPerGroup)
	requireT.NoError(err)

	s := NewSpaceTest[txtypes.Account, txtypes.Amount](t, state, hashKey)

	for i := types.KeyHash(1); i <= 2*key.KeyHash; i++ {
		v := s.NewEntry(TestKey[txtypes.Account]{
			Key:     txtypes.Account{uint8(i)},
			KeyHash: i,
		}, StageData)
		requireT.NoError(s.SetKey(v, txtypes.Amount(i)))
	}

	v5 := s.NewEntry(key, StageData)
	s.Find(v5)

	v5A := s.NewEntry(key, StageData)
	s.Find(v5A)

	v5B := s.NewEntry(key, StageData)
	s.Find(v5B)

	// Test setting replaced item.

	// Delete v5A.
	s.DeleteKey(v5A)

	// This item is inserted on first free slot, which is the one previously occupied by v5A.
	key100 := TestKey[txtypes.Account]{
		Key:     txtypes.Account{100},
		KeyHash: 100,
	}
	v100 := s.NewEntry(key100, StageData)
	requireT.NoError(s.SetKey(v100, txtypes.Amount(100)))

	// Now v5E points to invalid item.
	requireT.Equal(types.KeyHash(100), *v5B.keyHashP)

	// Let's insert v5 on another free position.
	v5B2 := s.NewEntry(key, StageData)
	requireT.NoError(s.SetKey(v5B2, txtypes.Amount(5)))
	requireT.Equal(types.KeyHash(100), *v5B.keyHashP)

	// When setting, it will update the right slot.
	requireT.NoError(s.SetKey(v5B, txtypes.Amount(10)))
	requireT.Equal(key.KeyHash, *v5B.keyHashP)
	requireT.Equal(key.KeyHash, *v5B2.keyHashP)
	requireT.Equal(unsafe.Pointer(v5B.keyHashP), unsafe.Pointer(v5B2.keyHashP))

	// v100 still exists.
	balance, exists := s.Query(key100)
	requireT.True(exists)
	requireT.Equal(txtypes.Amount(100), balance)

	// v5 exists.
	balance, exists = s.Query(key)
	requireT.True(exists)
	requireT.Equal(txtypes.Amount(10), balance)
}

// TestSetOnMovedItem verifies that item is correctly set after moving it to another slot and leaving
// the previous one free.
func TestSetOnMovedItem(t *testing.T) {
	requireT := require.New(t)

	// This is the key placed in the middle.
	var key = TestKey[txtypes.Account]{
		Key:     txtypes.Account{5},
		KeyHash: 5,
	}

	state, err := alloc.RunInTest(t, stateSize, nodesPerGroup)
	requireT.NoError(err)

	s := NewSpaceTest[txtypes.Account, txtypes.Amount](t, state, hashKey)

	for i := types.KeyHash(1); i <= 2*key.KeyHash; i++ {
		v := s.NewEntry(TestKey[txtypes.Account]{
			Key:     txtypes.Account{uint8(i)},
			KeyHash: i,
		}, StageData)
		requireT.NoError(s.SetKey(v, txtypes.Amount(i)))
	}

	v5 := s.NewEntry(key, StageData)
	s.Find(v5)

	v5A := s.NewEntry(key, StageData)
	s.Find(v5A)

	v5B := s.NewEntry(key, StageData)
	s.Find(v5B)

	// Test setting moved item.

	// Delete v5A.
	s.DeleteKey(v5A)

	// This item is inserted on first free slot, which is the one previously occupied by v5A.
	key100 := TestKey[txtypes.Account]{
		Key:     txtypes.Account{100},
		KeyHash: 100,
	}
	v100 := s.NewEntry(key100, StageData)
	requireT.NoError(s.SetKey(v100, txtypes.Amount(100)))

	// Now v5B points to invalid item.
	requireT.Equal(types.KeyHash(100), *v5B.keyHashP)

	// Let's insert v5 on another free position.
	v5B2 := s.NewEntry(key, StageData)
	requireT.NoError(s.SetKey(v5B2, txtypes.Amount(5)))
	requireT.Equal(types.KeyHash(100), *v5B.keyHashP)

	// Let's delete v100 to make the original slot free.
	s.DeleteKey(v100)
	requireT.Equal(types.KeyHash(0), *v100.keyHashP)
	requireT.Equal(types.KeyHash(0), *v5B.keyHashP)

	// When setting, it will update the right slot.
	requireT.NoError(s.SetKey(v5B, txtypes.Amount(10)))
	requireT.Equal(key.KeyHash, *v5B.keyHashP)
	requireT.Equal(key.KeyHash, *v5B2.keyHashP)
	requireT.Equal(unsafe.Pointer(v5B.keyHashP), unsafe.Pointer(v5B2.keyHashP))

	// v5 exists.
	balance, exists := s.Query(key)
	requireT.True(exists)
	requireT.Equal(txtypes.Amount(10), balance)
}

// TestSetTheSameSlotTwiceUsingSameEntry verifies that the same slot set twice using the same entry is updated with
// second value.
func TestSetTheSameSlotTwiceUsingSameEntry(t *testing.T) {
	requireT := require.New(t)

	// This is the key placed in the middle.
	var key = TestKey[txtypes.Account]{
		Key:     txtypes.Account{5},
		KeyHash: 5,
	}

	state, err := alloc.RunInTest(t, stateSize, nodesPerGroup)
	requireT.NoError(err)

	s := NewSpaceTest[txtypes.Account, txtypes.Amount](t, state, hashKey)

	v5 := s.NewEntry(key, StageData)
	requireT.NoError(s.SetKey(v5, txtypes.Amount(5)))

	balance, exists := s.Query(key)
	requireT.True(exists)
	requireT.Equal(txtypes.Amount(5), balance)

	keyHashP := v5.keyHashP

	requireT.NoError(s.SetKey(v5, txtypes.Amount(10)))
	requireT.Equal(unsafe.Pointer(v5.keyHashP), unsafe.Pointer(keyHashP))

	balance, exists = s.Query(key)
	requireT.True(exists)
	requireT.Equal(txtypes.Amount(10), balance)
}

// TestSetTheSameSlotTwiceUsingDifferentEntry verifies that the same slot set twice using different entry is updated
// with second value.
func TestSetTheSameSlotTwiceUsingDifferentEntry(t *testing.T) {
	requireT := require.New(t)

	// This is the key placed in the middle.
	var key = TestKey[txtypes.Account]{
		Key:     txtypes.Account{5},
		KeyHash: 5,
	}

	state, err := alloc.RunInTest(t, stateSize, nodesPerGroup)
	requireT.NoError(err)

	s := NewSpaceTest[txtypes.Account, txtypes.Amount](t, state, hashKey)

	v5 := s.NewEntry(key, StageData)
	requireT.NoError(s.SetKey(v5, txtypes.Amount(5)))

	balance, exists := s.Query(key)
	requireT.True(exists)
	requireT.Equal(txtypes.Amount(5), balance)

	v5B := s.NewEntry(key, StageData)
	requireT.NoError(s.SetKey(v5B, txtypes.Amount(10)))
	requireT.Equal(unsafe.Pointer(v5.keyHashP), unsafe.Pointer(v5B.keyHashP))

	balance, exists = s.Query(key)
	requireT.True(exists)
	requireT.Equal(txtypes.Amount(10), balance)
}
