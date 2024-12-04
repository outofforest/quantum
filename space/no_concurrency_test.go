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

	// Read non-existing

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

	// Create

	requireT.NoError(s.SetKey(v, amount))

	// Read existing

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

	// Update 1

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

	// Update 2

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

	// Delete 1

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

	// Recreate

	v, err = s.NewEntry(snapshotID, account, StagePointer0)
	requireT.NoError(err)

	requireT.NoError(s.SetKey(v, amount))

	balance, exists = s.Query(account)
	requireT.True(exists)
	requireT.Equal(amount, balance)

	// Delete 2

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

	// Create

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
