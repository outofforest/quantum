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

// TestFindingAvailableFreeSlot verifies that free slot is found for non-existing item.
func TestFindingAvailableFreeSlot(t *testing.T) {
	requireT := require.New(t)

	const snapshotID types.SnapshotID = 1

	state, err := alloc.RunInTest(t, stateSize, nodesPerGroup)
	requireT.NoError(err)

	s := NewSpaceTest[txtypes.Account, txtypes.Amount](t, state, hashKey)

	v1, err := s.NewEntry(snapshotID, TestKey[txtypes.Account]{
		Key:     txtypes.Account{1},
		KeyHash: 1,
	}, StageData)
	requireT.NoError(err)
	requireT.NoError(s.SetKey(v1, 1))

	v2, err := s.NewEntry(snapshotID, TestKey[txtypes.Account]{
		Key:     txtypes.Account{2},
		KeyHash: 2,
	}, StageData)
	requireT.NoError(err)
	requireT.NoError(s.Find(v2))
	requireT.NotNil(v2.keyHashP)
	requireT.Equal(types.KeyHash(0), *v2.keyHashP)
}

// TestFindStages verifies that locating data item is divided into three stages.
func TestFindStages(t *testing.T) {
	requireT := require.New(t)

	const (
		snapshotID types.SnapshotID = 1
		// This key hash means that item will always go to the pointer at index 0.
		keyHash = 1 << 63
	)

	state, err := alloc.RunInTest(t, stateSize, nodesPerGroup)
	requireT.NoError(err)

	s := NewSpaceTest[txtypes.Account, txtypes.Amount](t, state, hashKey)

	// Create levels in the tree.

	key := TestKey[txtypes.Account]{
		Key:     txtypes.Account{0x01},
		KeyHash: keyHash,
	}

	v, err := s.NewEntry(snapshotID, key, StageData)
	requireT.NoError(err)
	requireT.NoError(s.SetKey(v, txtypes.Amount(10)))

	for range 8 {
		requireT.NoError(s.AddPointerNode(v, false))
		requireT.NoError(s.Find(v))
	}

	requireT.Equal(uint8(8), v.level)

	// Test StagePointer0.

	v, err = s.NewEntry(snapshotID, key, StagePointer0)
	requireT.NoError(err)

	requireT.Equal(StagePointer0, v.stage)
	requireT.Equal(uint8(0), v.level)
	requireT.Nil(v.keyHashP)
	requireT.Nil(v.itemP)
	requireT.False(v.exists)

	requireT.NoError(s.Find(v))
	requireT.Equal(StagePointer1, v.stage)
	requireT.Equal(uint8(3), v.level)
	requireT.Nil(v.keyHashP)
	requireT.Nil(v.itemP)
	requireT.False(v.exists)

	requireT.NoError(s.Find(v))
	requireT.Equal(StageData, v.stage)
	requireT.Equal(uint8(8), v.level)
	requireT.Nil(v.keyHashP)
	requireT.Nil(v.itemP)
	requireT.False(v.exists)

	requireT.NoError(s.Find(v))
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

	const (
		snapshotID types.SnapshotID = 1
		// After first split this key hash stays in data node 0, but after second split it will go to the data node 16.
		keyHash types.KeyHash = 16
	)

	state, err := alloc.RunInTest(t, stateSize, nodesPerGroup)
	requireT.NoError(err)

	s := NewSpaceTest[txtypes.Account, txtypes.Amount](t, state, hashKey)

	// Create levels in the tree.

	key := TestKey[txtypes.Account]{
		Key:     txtypes.Account{0x01},
		KeyHash: keyHash,
	}

	v64, err := s.NewEntry(snapshotID, TestKey[txtypes.Account]{
		Key:     txtypes.Account{0x01},
		KeyHash: types.KeyHash(64),
	}, StageData)
	requireT.NoError(err)
	requireT.NoError(s.SetKey(v64, txtypes.Amount(64)))

	v16, err := s.NewEntry(snapshotID, key, StageData)
	requireT.NoError(err)
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

	v16Read, err := s.NewEntry(snapshotID, key, StageData)
	requireT.NoError(err)
	requireT.NoError(s.Find(v16Read))
	// Verify the next expected data node address.
	requireT.Equal(&pointerNode.Pointers[16].VolatileAddress, v16Read.nextDataNode)

	v16Exists, err := s.NewEntry(snapshotID, key, StageData)
	requireT.NoError(err)
	requireT.NoError(s.Find(v16Exists))

	v16Delete, err := s.NewEntry(snapshotID, key, StageData)
	requireT.NoError(err)
	requireT.NoError(s.Find(v16Delete))

	// Now let's split data node 0 using key hash 64.

	v64, err = s.NewEntry(snapshotID, TestKey[txtypes.Account]{
		Key:     txtypes.Account{0x01},
		KeyHash: types.KeyHash(64),
	}, StageData)
	requireT.NoError(err)
	requireT.NoError(s.Find(v64))
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
	requireT.True(pointerNode.Pointers[0].VolatileAddress.IsSet(types.FlagPointerNode))
	requireT.True(v16Read.storeRequest.Store[v16Read.storeRequest.PointersToStore-1].Pointer.VolatileAddress.
		IsSet(types.FlagPointerNode))

	// We are now in situation where key hash 16 is no longer in the place pointed to by v16.
	// When walking the tree now, it should not follow the current pointer node, but go back and switch
	// to the immutable path.

	balance, err := s.ReadKey(v16Read)
	requireT.NoError(err)
	requireT.Equal(txtypes.Amount(16), balance)
	requireT.Nil(v16Read.nextDataNode)

	exists, err := s.KeyExists(v16Exists)
	requireT.NoError(err)
	requireT.True(exists)
	requireT.Nil(v16Exists.nextDataNode)

	requireT.NoError(s.DeleteKey(v16Delete))
	requireT.Nil(v16Delete.nextDataNode)

	balance, exists = s.Query(key)
	requireT.False(exists)
	requireT.Equal(txtypes.Amount(0), balance)
}

// TestDeletingOnEmptySpace verifies that deleting on empty space works and does nothing.
func TestDeletingOnEmptySpace(t *testing.T) {
	requireT := require.New(t)

	const snapshotID types.SnapshotID = 1

	state, err := alloc.RunInTest(t, stateSize, nodesPerGroup)
	requireT.NoError(err)

	s := NewSpaceTest[txtypes.Account, txtypes.Amount](t, state, hashKey)

	v5, err := s.NewEntry(snapshotID, TestKey[txtypes.Account]{
		Key:     txtypes.Account{5},
		KeyHash: 5,
	}, StageData)
	requireT.NoError(err)
	requireT.NoError(s.Find(v5))
	requireT.NoError(s.DeleteKey(v5))
	requireT.Equal(types.FreeAddress, s.Root().VolatileAddress)
}

// TestDeletingReplacedDataItem verifies item deletion in scenarios where the originally tracked slot has been deleted,
// replaced or moved.
func TestDeletingReplacedDataItem(t *testing.T) {
	requireT := require.New(t)

	const snapshotID types.SnapshotID = 1

	// This is the key placed in the middle.
	var key = TestKey[txtypes.Account]{
		Key:     txtypes.Account{5},
		KeyHash: 5,
	}

	state, err := alloc.RunInTest(t, stateSize, nodesPerGroup)
	requireT.NoError(err)

	s := NewSpaceTest[txtypes.Account, txtypes.Amount](t, state, hashKey)

	for i := types.KeyHash(1); i <= 2*key.KeyHash; i++ {
		v, err := s.NewEntry(snapshotID, TestKey[txtypes.Account]{
			Key:     txtypes.Account{uint8(i)},
			KeyHash: i,
		}, StageData)
		requireT.NoError(err)
		requireT.NoError(s.SetKey(v, txtypes.Amount(i)))
	}

	v5, err := s.NewEntry(snapshotID, key, StageData)
	requireT.NoError(err)
	requireT.NoError(s.Find(v5))

	v5A, err := s.NewEntry(snapshotID, key, StageData)
	requireT.NoError(err)
	requireT.NoError(s.Find(v5A))

	v5B, err := s.NewEntry(snapshotID, key, StageData)
	requireT.NoError(err)
	requireT.NoError(s.Find(v5B))

	v5C, err := s.NewEntry(snapshotID, key, StageData)
	requireT.NoError(err)
	requireT.NoError(s.Find(v5C))

	v5D, err := s.NewEntry(snapshotID, key, StageData)
	requireT.NoError(err)
	requireT.NoError(s.Find(v5D))

	v5E, err := s.NewEntry(snapshotID, key, StageData)
	requireT.NoError(err)
	requireT.NoError(s.Find(v5E))

	// Test that nothing happens if hash is different.

	*v5.keyHashP = 1
	requireT.NoError(s.DeleteKey(v5A))
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

	// Test that nothing happens if key is different.

	v5.itemP.Key = txtypes.Account{0x01}
	requireT.NoError(s.DeleteKey(v5B))
	requireT.Equal(key.KeyHash, *v5.keyHashP)
	requireT.Equal(txtypes.Account{0x01}, v5.itemP.Key)
	v5.itemP.Key = key.Key
	// v5B now points to first free slot.
	requireT.Equal(types.KeyHash(0), *v5B.keyHashP)

	for i := types.KeyHash(1); i <= 2*key.KeyHash; i++ {
		balance, exists := s.Query(TestKey[txtypes.Account]{
			Key:     txtypes.Account{uint8(i)},
			KeyHash: i,
		})
		requireT.True(exists)
		requireT.Equal(txtypes.Amount(i), balance)
	}

	// Test that nothing happens if slot is free.

	*v5.keyHashP = 0
	requireT.NoError(s.DeleteKey(v5C))
	requireT.Equal(types.KeyHash(0), *v5.keyHashP)
	*v5.keyHashP = key.KeyHash
	// v5C still points to the same slot.
	requireT.Equal(key.KeyHash, *v5C.keyHashP)

	for i := types.KeyHash(1); i <= 2*key.KeyHash; i++ {
		balance, exists := s.Query(TestKey[txtypes.Account]{
			Key:     txtypes.Account{uint8(i)},
			KeyHash: i,
		})
		requireT.True(exists)
		requireT.Equal(txtypes.Amount(i), balance)
	}

	// Test deleting moved item.

	// Delete v5D.
	requireT.NoError(s.DeleteKey(v5D))

	// This item is inserted on first free slot, which is the one previously occupied by v5D.
	key100 := TestKey[txtypes.Account]{
		Key:     txtypes.Account{100},
		KeyHash: 100,
	}
	v100, err := s.NewEntry(snapshotID, key100, StageData)
	requireT.NoError(err)
	requireT.NoError(s.SetKey(v100, txtypes.Amount(100)))

	// Now v5E points to invalid item.
	requireT.Equal(types.KeyHash(100), *v5E.keyHashP)

	// But when deleting, it will free the right slot.
	requireT.NoError(s.DeleteKey(v5E))
	requireT.Equal(types.KeyHash(0), *v5E.keyHashP)

	// v100 still exists.
	balance, exists := s.Query(key100)
	requireT.True(exists)
	requireT.Equal(txtypes.Amount(100), balance)

	// v5 doesn't exist.
	balance, exists = s.Query(key)
	requireT.False(exists)
	requireT.Equal(txtypes.Amount(0), balance)

	// Try to delete if we were not able to find slot for an item.

	// Set all slots as busy.
	dataNodeAssistant := s.DataNodeAssistant()
	keyHashes := dataNodeAssistant.KeyHashes(state.Node(s.Root().VolatileAddress))
	for i := range keyHashes {
		keyHashes[i] = 1
	}

	// Delete on nil slot.
	v100, err = s.NewEntry(snapshotID, key100, StageData)
	requireT.NoError(err)
	requireT.NoError(s.Find(v100))
	requireT.Nil(v100.keyHashP)
	requireT.NoError(s.DeleteKey(v100))
	requireT.Nil(v100.keyHashP)

	// Verify that nothing has been deleted.
	for _, kh := range keyHashes {
		requireT.Equal(types.KeyHash(1), kh)
	}
}
