package space

import (
	"github.com/stretchr/testify/require"

	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/pipeline"
	"github.com/outofforest/quantum/types"
	"github.com/outofforest/quantum/wal"
)

// TestKey represents key with explicit hash used in tests.
type TestKey[K comparable] struct {
	Key     K
	KeyHash types.KeyHash
}

// NewSpaceTest creates new wrapper for space testing.
func NewSpaceTest[K, V comparable](
	t require.TestingT,
	state *alloc.State,
	hashKeyFunc func(key *K, buff []byte, level uint8) types.KeyHash,
) *SpaceTest[K, V] {
	dataNodeAssistant, err := NewDataNodeAssistant[K, V]()
	require.NoError(t, err)

	s := New[K, V](Config[K, V]{
		SpaceRoot: types.NodeRoot{
			Pointer: &types.Pointer{},
			Hash:    &types.Hash{},
		},
		State:             state,
		DataNodeAssistant: dataNodeAssistant,
		NoSnapshots:       false,
	})

	txFactory := pipeline.NewTransactionRequestFactory()

	return &SpaceTest[K, V]{
		s:           s,
		tx:          txFactory.New(),
		walRecorder: wal.NewRecorder(state, state.NewAllocator()),
		allocator:   state.NewAllocator(),
		hashKeyFunc: hashKeyFunc,
		hashBuff:    s.NewHashBuff(),
		hashMatches: s.NewHashMatches(),
	}
}

// SpaceTest exposes some private functionality of space to make testing concurrent scenarios possible.
//
//nolint:revive
type SpaceTest[K, V comparable] struct {
	s           *Space[K, V]
	tx          *pipeline.TransactionRequest
	walRecorder *wal.Recorder
	allocator   *alloc.Allocator
	hashBuff    []byte
	hashMatches []uint64
	hashKeyFunc func(key *K, buff []byte, level uint8) types.KeyHash
}

// NewEntry initializes new entry.
func (s *SpaceTest[K, V]) NewEntry(
	snapshotID types.SnapshotID,
	key TestKey[K],
	stage uint8,
) (*Entry[K, V], error) {
	v := &Entry[K, V]{}
	if err := s.s.initEntry(v, snapshotID, s.tx, s.walRecorder, s.allocator, key.Key, key.KeyHash, stage); err != nil {
		return nil, err
	}
	return v, nil
}

// KeyExists checks if key is set in the space.
func (s *SpaceTest[K, V]) KeyExists(v *Entry[K, V]) (bool, error) {
	return s.s.keyExists(v, s.tx, s.walRecorder, s.allocator, s.hashBuff, s.hashMatches, s.hashKeyFunc)
}

// ReadKey reads value for the key.
func (s *SpaceTest[K, V]) ReadKey(v *Entry[K, V]) (V, error) {
	return s.s.readKey(v, s.tx, s.walRecorder, s.allocator, s.hashBuff, s.hashMatches, s.hashKeyFunc)
}

// DeleteKey deletes key from space.
func (s *SpaceTest[K, V]) DeleteKey(v *Entry[K, V]) error {
	return s.s.deleteKey(v, s.tx, s.walRecorder, s.allocator, s.hashBuff, s.hashMatches, s.hashKeyFunc)
}

// SetKey sets value for the key.
func (s *SpaceTest[K, V]) SetKey(v *Entry[K, V], value V) error {
	return s.s.setKey(v, s.tx, s.walRecorder, s.allocator, value, s.hashBuff, s.hashMatches, s.hashKeyFunc)
}

// SplitDataNode splits data node.
func (s *SpaceTest[K, V]) SplitDataNode(v *Entry[K, V], snapshotID types.SnapshotID) error {
	_, err := s.s.splitDataNode(snapshotID, s.tx, s.walRecorder, s.allocator, v.parentIndex,
		v.storeRequest.Store[v.storeRequest.PointersToStore-2].Pointer, v.level)
	return err
}

// AddPointerNode adds pointer node.
func (s *SpaceTest[K, V]) AddPointerNode(v *Entry[K, V], conflict bool) error {
	return s.s.addPointerNode(v, s.tx, s.walRecorder, s.allocator, conflict)
}

// WalkPointers walk all the pointers to find the key.
func (s *SpaceTest[K, V]) WalkPointers(v *Entry[K, V]) error {
	return s.s.walkPointers(v, s.tx, s.walRecorder, s.allocator, s.hashBuff, s.hashKeyFunc)
}

// WalkOnePointer walks one pointer only.
func (s *SpaceTest[K, V]) WalkOnePointer(v *Entry[K, V]) (bool, error) {
	return s.s.walkOnePointer(v, s.tx, s.walRecorder, s.allocator, s.hashBuff, s.hashKeyFunc)
}

// WalkDataItems walks items in data node to find position for the key.
func (s *SpaceTest[K, V]) WalkDataItems(v *Entry[K, V]) bool {
	return s.s.walkDataItems(v, s.hashMatches)
}

// Query queries the space for a key.
func (s *SpaceTest[K, V]) Query(key TestKey[K]) (V, bool) {
	return s.s.query(key.Key, key.KeyHash, s.hashBuff, s.hashMatches, s.hashKeyFunc)
}

// Find finds the location in the tree for key.
func (s *SpaceTest[K, V]) Find(v *Entry[K, V]) error {
	return s.s.find(v, s.tx, s.walRecorder, s.allocator, s.hashBuff, s.hashMatches, s.hashKeyFunc)
}
