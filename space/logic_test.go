// Github actions run on machines not supporting AVX-512 instructions.
//go:build nogithub

package space

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/quantum/alloc"
	txtypes "github.com/outofforest/quantum/tx/types"
	"github.com/outofforest/quantum/types"
)

const (
	nodesPerGroup = 100
	stateSize     = 100 * nodesPerGroup * types.NodeLength
)

// TestCRUDOnRootDataNode tests basic CRUD operations using one data item on single data node being the root
// of the space.
func TestCRUDOnRootDataNode(t *testing.T) {
	requireT := require.New(t)

	var (
		snapshotID types.SnapshotID = 1

		account = TestKey[txtypes.Account]{
			Key:     txtypes.Account{0x01},
			KeyHash: 1,
		}
		amount txtypes.Amount = 100
	)

	state, err := alloc.RunInTest(t, stateSize, nodesPerGroup)
	requireT.NoError(err)

	s := NewSpaceTest[txtypes.Account, txtypes.Amount](t, state, hashKey)

	// Read non-existing.

	balance, exists := s.Query(account)
	requireT.False(exists)
	requireT.Equal(txtypes.Amount(0), balance)

	v, err := s.NewEntry(snapshotID, account, StageData)
	requireT.NoError(err)

	exists, err = s.KeyExists(v)
	requireT.NoError(err)
	requireT.False(exists)

	balance, err = s.ReadKey(v)
	requireT.NoError(err)
	requireT.Equal(txtypes.Amount(0), balance)

	// Create.

	requireT.NoError(s.SetKey(v, amount))

	// Read existing.

	balance, exists = s.Query(account)
	requireT.True(exists)
	requireT.Equal(amount, balance)

	exists, err = s.KeyExists(v)
	requireT.NoError(err)
	requireT.True(exists)

	balance, err = s.ReadKey(v)
	requireT.NoError(err)
	requireT.Equal(amount, balance)

	v2, err := s.NewEntry(snapshotID, account, StageData)
	requireT.NoError(err)

	exists, err = s.KeyExists(v2)
	requireT.NoError(err)
	requireT.True(exists)

	balance, err = s.ReadKey(v2)
	requireT.NoError(err)
	requireT.Equal(amount, balance)

	v3, err := s.NewEntry(snapshotID, account, StagePointer0)
	requireT.NoError(err)

	exists, err = s.KeyExists(v3)
	requireT.NoError(err)
	requireT.True(exists)

	balance, err = s.ReadKey(v3)
	requireT.NoError(err)
	requireT.Equal(amount, balance)

	// Update 1.

	requireT.NoError(s.SetKey(v3, amount+1))

	exists, err = s.KeyExists(v3)
	requireT.NoError(err)
	requireT.True(exists)

	balance, err = s.ReadKey(v3)
	requireT.NoError(err)
	requireT.Equal(amount+1, balance)

	balance, exists = s.Query(account)
	requireT.True(exists)
	requireT.Equal(amount+1, balance)

	v4, err := s.NewEntry(snapshotID, account, StagePointer0)
	requireT.NoError(err)

	exists, err = s.KeyExists(v4)
	requireT.NoError(err)
	requireT.True(exists)

	balance, err = s.ReadKey(v4)
	requireT.NoError(err)
	requireT.Equal(amount+1, balance)

	// Update 2.

	requireT.NoError(s.SetKey(v4, amount+2))

	exists, err = s.KeyExists(v4)
	requireT.NoError(err)
	requireT.True(exists)

	balance, err = s.ReadKey(v4)
	requireT.NoError(err)
	requireT.Equal(amount+2, balance)

	balance, exists = s.Query(account)
	requireT.True(exists)
	requireT.Equal(amount+2, balance)

	v5, err := s.NewEntry(snapshotID, account, StagePointer0)
	requireT.NoError(err)

	exists, err = s.KeyExists(v5)
	requireT.NoError(err)
	requireT.True(exists)

	balance, err = s.ReadKey(v5)
	requireT.NoError(err)
	requireT.Equal(amount+2, balance)

	// Delete 1.

	requireT.NoError(s.DeleteKey(v5))

	exists, err = s.KeyExists(v5)
	requireT.NoError(err)
	requireT.False(exists)

	balance, err = s.ReadKey(v5)
	requireT.NoError(err)
	requireT.Equal(txtypes.Amount(0), balance)

	balance, exists = s.Query(account)
	requireT.False(exists)
	requireT.Equal(txtypes.Amount(0), balance)

	// Recreate.

	v, err = s.NewEntry(snapshotID, account, StagePointer0)
	requireT.NoError(err)

	requireT.NoError(s.SetKey(v, amount))

	balance, exists = s.Query(account)
	requireT.True(exists)
	requireT.Equal(amount, balance)

	// Delete 2.

	v2, err = s.NewEntry(snapshotID, account, StagePointer0)
	requireT.NoError(err)

	requireT.NoError(s.DeleteKey(v2))

	exists, err = s.KeyExists(v2)
	requireT.NoError(err)
	requireT.False(exists)

	balance, err = s.ReadKey(v2)
	requireT.NoError(err)
	requireT.Equal(txtypes.Amount(0), balance)

	balance, exists = s.Query(account)
	requireT.False(exists)
	requireT.Equal(txtypes.Amount(0), balance)
}

// TestSetConflictingHashesOnRootDataNode sets many keys using the same hash to verify that they are not overwritten
// by each other.
func TestSetConflictingHashesOnRootDataNode(t *testing.T) {
	const (
		numOfItems                  = 50
		snapshotID types.SnapshotID = 1
		keyHash    types.KeyHash    = 1 // Same key hash is intentionally used for all the items to test conflicts.
	)

	requireT := require.New(t)

	state, err := alloc.RunInTest(t, stateSize, nodesPerGroup)
	requireT.NoError(err)

	s := NewSpaceTest[txtypes.Account, txtypes.Amount](t, state, hashKey)

	// Create.

	for i := range uint8(numOfItems) {
		v, err := s.NewEntry(snapshotID, TestKey[txtypes.Account]{
			Key:     txtypes.Account{i},
			KeyHash: keyHash,
		}, StagePointer0)
		requireT.NoError(err)
		requireT.NoError(s.SetKey(v, txtypes.Amount(i)))
	}

	// Verify items exist.

	for i := range uint8(numOfItems) {
		key := TestKey[txtypes.Account]{
			Key:     txtypes.Account{i},
			KeyHash: keyHash,
		}
		amount := txtypes.Amount(i)

		v, err := s.NewEntry(snapshotID, key, StagePointer0)
		requireT.NoError(err)

		exists, err := s.KeyExists(v)
		requireT.NoError(err)
		requireT.True(exists)

		balance, err := s.ReadKey(v)
		requireT.NoError(err)
		requireT.Equal(amount, balance)

		balance, exists = s.Query(key)
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

		v, err := s.NewEntry(snapshotID, key, StagePointer0)
		requireT.NoError(err)
		requireT.NoError(s.SetKey(v, amount))

		v2, err := s.NewEntry(snapshotID, key, StagePointer0)
		requireT.NoError(err)

		exists, err := s.KeyExists(v2)
		requireT.NoError(err)
		requireT.True(exists)

		balance, err := s.ReadKey(v2)
		requireT.NoError(err)
		requireT.Equal(amount, balance)

		balance, exists = s.Query(key)
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

		v, err := s.NewEntry(snapshotID, key, StagePointer0)
		requireT.NoError(err)

		exists, err := s.KeyExists(v)
		requireT.NoError(err)
		requireT.True(exists)

		balance, err := s.ReadKey(v)
		requireT.NoError(err)
		requireT.Equal(amount, balance)

		balance, exists = s.Query(key)
		requireT.True(exists)
		requireT.Equal(amount, balance)
	}

	// Delete every second item.

	for i := uint8(0); i < numOfItems; i += 2 {
		key := TestKey[txtypes.Account]{
			Key:     txtypes.Account{i},
			KeyHash: keyHash,
		}

		v, err := s.NewEntry(snapshotID, key, StagePointer0)
		requireT.NoError(err)
		requireT.NoError(s.DeleteKey(v))

		v2, err := s.NewEntry(snapshotID, key, StagePointer0)
		requireT.NoError(err)

		exists, err := s.KeyExists(v2)
		requireT.NoError(err)
		requireT.False(exists)

		balance, err := s.ReadKey(v2)
		requireT.NoError(err)
		requireT.Equal(txtypes.Amount(0), balance)

		balance, exists = s.Query(key)
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

		v, err := s.NewEntry(snapshotID, key, StagePointer0)
		requireT.NoError(err)

		exists, err := s.KeyExists(v)
		requireT.NoError(err)
		requireT.True(exists)

		balance, err := s.ReadKey(v)
		requireT.NoError(err)
		requireT.Equal(amount, balance)

		balance, exists = s.Query(key)
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
		numOfItems                  = NumOfPointers
		snapshotID types.SnapshotID = 1
		keyHash    types.KeyHash    = 1 // Same key hash is intentionally used for all the items.
	)

	requireT := require.New(t)

	state, err := alloc.RunInTest(t, stateSize, nodesPerGroup)
	requireT.NoError(err)

	s := NewSpaceTest[txtypes.Account, txtypes.Amount](t, state, hashKey)

	// Create.

	for i := range uint8(numOfItems - 1) {
		v, err := s.NewEntry(snapshotID, TestKey[txtypes.Account]{
			Key:     txtypes.Account{i},
			KeyHash: keyHash,
		}, StagePointer0)
		requireT.NoError(err)
		requireT.NoError(s.SetKey(v, txtypes.Amount(i)))
		requireT.Equal(uint8(0), v.level)
	}

	// Add pointer node and set the last item.

	v, err := s.NewEntry(snapshotID, TestKey[txtypes.Account]{
		Key:     txtypes.Account{numOfItems - 1},
		KeyHash: keyHash,
	}, StageData)
	requireT.NoError(err)
	requireT.NoError(s.Find(v))

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

		v, err := s.NewEntry(snapshotID, key, StagePointer0)
		requireT.NoError(err)

		exists, err := s.KeyExists(v)
		requireT.NoError(err)
		requireT.True(exists)

		balance, err := s.ReadKey(v)
		requireT.NoError(err)
		requireT.Equal(amount, balance)

		balance, exists = s.Query(key)
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
		numOfItems                  = NumOfPointers
		snapshotID types.SnapshotID = 1
		keyHash    types.KeyHash    = 1 // Same key hash is intentionally used for all the items.
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
		v, err := s.NewEntry(snapshotID, TestKey[txtypes.Account]{
			Key:     txtypes.Account{i},
			KeyHash: keyHash,
		}, StagePointer0)
		requireT.NoError(err)
		requireT.NoError(s.SetKey(v, txtypes.Amount(i)))
		requireT.Equal(uint8(0), v.level)
	}

	// Add pointer node and set the last item.

	v, err := s.NewEntry(snapshotID, TestKey[txtypes.Account]{
		Key:     txtypes.Account{numOfItems - 1},
		KeyHash: keyHash,
	}, StageData)
	requireT.NoError(err)
	requireT.NoError(s.Find(v))

	// Store the address of the data node to be sure that half of the items is moved to another data node.
	dataNodeAddress := v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.VolatileAddress

	requireT.NoError(s.AddPointerNode(v, true))
	requireT.NoError(s.SetKey(v, txtypes.Amount(numOfItems-1)))
	requireT.Equal(uint8(1), v.level)

	// Verify that all the items has been moved correctly without recomputing hashes.

	dataNodes := map[types.NodeAddress]uint64{}

	for i := range uint8(numOfItems) {
		key := TestKey[txtypes.Account]{
			Key:     txtypes.Account{i},
			KeyHash: keyHash,
		}
		amount := txtypes.Amount(i)

		v, err := s.NewEntry(snapshotID, key, StagePointer0)
		requireT.NoError(err)

		exists, err := s.KeyExists(v)
		requireT.NoError(err)
		requireT.True(exists)

		balance, err := s.ReadKey(v)
		requireT.NoError(err)
		requireT.Equal(amount, balance)

		balance, exists = s.Query(key)
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
	const (
		// It is selected this way so half of the items is moved to another data node.
		numOfItems                  = NumOfPointers
		snapshotID types.SnapshotID = 1
	)

	requireT := require.New(t)

	state, err := alloc.RunInTest(t, stateSize, nodesPerGroup)
	requireT.NoError(err)

	s := NewSpaceTest[txtypes.Account, txtypes.Amount](t, state, hashKey)

	// Create.

	for i := range uint8(numOfItems - 1) {
		v, err := s.NewEntry(snapshotID, TestKey[txtypes.Account]{
			Key:     txtypes.Account{i},
			KeyHash: types.KeyHash(i + 1), // +1 to avoid 0
		}, StagePointer0)
		requireT.NoError(err)
		requireT.NoError(s.SetKey(v, txtypes.Amount(i)))
		requireT.Equal(uint8(0), v.level)
	}

	// Add pointer node and set the last item.

	v, err := s.NewEntry(snapshotID, TestKey[txtypes.Account]{
		Key:     txtypes.Account{numOfItems - 1},
		KeyHash: types.KeyHash(numOfItems),
	}, StageData)
	requireT.NoError(err)
	requireT.NoError(s.Find(v))

	// Store the address of the data node to be sure that half of the items is moved to another data node.
	dataNodeAddress := v.storeRequest.Store[v.storeRequest.PointersToStore-1].Pointer.VolatileAddress

	requireT.NoError(s.AddPointerNode(v, false))
	requireT.NoError(s.SetKey(v, txtypes.Amount(numOfItems-1)))
	requireT.Equal(uint8(1), v.level)

	// Verify that all the items has been moved correctly without recomputing hashes.

	dataNodes := map[types.NodeAddress]uint64{}

	for i := range uint8(numOfItems) {
		key := TestKey[txtypes.Account]{
			Key:     txtypes.Account{i},
			KeyHash: types.KeyHash(i + 1), // +1 to avoid 0
		}
		amount := txtypes.Amount(i)

		v, err := s.NewEntry(snapshotID, key, StagePointer0)
		requireT.NoError(err)

		exists, err := s.KeyExists(v)
		requireT.NoError(err)
		requireT.True(exists)

		balance, err := s.ReadKey(v)
		requireT.NoError(err)
		requireT.Equal(amount, balance)

		balance, exists = s.Query(key)
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
	const (
		// It is selected this way so at the end each pointer references a data node containing one data item.
		numOfItems                  = NumOfPointers
		snapshotID types.SnapshotID = 1
	)

	requireT := require.New(t)

	state, err := alloc.RunInTest(t, stateSize, nodesPerGroup)
	requireT.NoError(err)

	s := NewSpaceTest[txtypes.Account, txtypes.Amount](t, state, hashKey)

	// Store items in the root data node.

	for i := uint8(1); i <= numOfItems; i++ {
		v, err := s.NewEntry(snapshotID, TestKey[txtypes.Account]{
			Key:     txtypes.Account{i},
			KeyHash: types.KeyHash(i),
		}, StagePointer0)
		requireT.NoError(err)
		requireT.NoError(s.SetKey(v, txtypes.Amount(i)))
	}

	// Convert root node into pointer node.

	v, err := s.NewEntry(snapshotID, TestKey[txtypes.Account]{
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
			v, err := s.NewEntry(snapshotID, TestKey[txtypes.Account]{
				Key:     txtypes.Account{i},
				KeyHash: types.KeyHash(i),
			}, StageData)
			requireT.NoError(err)
			requireT.NoError(s.Find(v))

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
		numOfItems                  = NumOfPointers
		snapshotID types.SnapshotID = 1
		keyHash    types.KeyHash    = 1 // Same for all data items to create conflicts.
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
		v, err := s.NewEntry(snapshotID, TestKey[txtypes.Account]{
			Key:     txtypes.Account{i},
			KeyHash: keyHash,
		}, StagePointer0)
		requireT.NoError(err)
		requireT.NoError(s.SetKey(v, txtypes.Amount(i)))
	}

	// Convert root node into pointer node.

	v, err := s.NewEntry(snapshotID, TestKey[txtypes.Account]{
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
			v, err := s.NewEntry(snapshotID, TestKey[txtypes.Account]{
				Key:     txtypes.Account{i},
				KeyHash: types.KeyHash(i),
			}, StageData)
			requireT.NoError(err)
			requireT.NoError(s.Find(v))

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
