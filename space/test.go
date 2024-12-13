package space

import (
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/pipeline"
	"github.com/outofforest/quantum/types"
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
		DeletionCounter:   lo.ToPtr[uint64](0),
		NoSnapshots:       false,
	})
	if hashKeyFunc != nil {
		s.hashKeyFunc = hashKeyFunc
	}

	txFactory := pipeline.NewTransactionRequestFactory()

	return &SpaceTest[K, V]{
		s:         s,
		tx:        txFactory.New(),
		allocator: state.NewVolatileAllocator(),
	}
}

// SpaceTest exposes some private functionality of space to make testing concurrent scenarios possible.
//
//nolint:revive
type SpaceTest[K, V comparable] struct {
	s         *Space[K, V]
	tx        *pipeline.TransactionRequest
	allocator *alloc.Allocator[types.VolatileAddress]
}

// NewEntry initializes new entry.
func (s *SpaceTest[K, V]) NewEntry(key TestKey[K], stage uint8) *Entry[K, V] {
	v := &Entry[K, V]{}
	s.s.initEntry(v, key.Key, key.KeyHash, stage)
	return v
}

// Root returns pointer to the space root node.
func (s *SpaceTest[K, V]) Root() *types.Pointer {
	return s.s.config.SpaceRoot.Pointer
}

// DataNodeAssistant returns space's data node assistant.
func (s *SpaceTest[K, V]) DataNodeAssistant() *DataNodeAssistant[K, V] {
	return s.s.config.DataNodeAssistant
}

// KeyExists checks if key is set in the space.
func (s *SpaceTest[K, V]) KeyExists(v *Entry[K, V]) bool {
	return s.s.KeyExists(v)
}

// ReadKey reads value for the key.
func (s *SpaceTest[K, V]) ReadKey(v *Entry[K, V]) V {
	return s.s.ReadKey(v)
}

// DeleteKey deletes key from space.
func (s *SpaceTest[K, V]) DeleteKey(v *Entry[K, V]) {
	s.s.DeleteKey(v, s.tx)
}

// SetKey sets value for the key.
func (s *SpaceTest[K, V]) SetKey(v *Entry[K, V], value V) error {
	return s.s.SetKey(v, s.tx, s.allocator, value)
}

// SplitDataNode splits data node.
func (s *SpaceTest[K, V]) SplitDataNode(v *Entry[K, V], conflict bool) error {
	var err error
	if conflict {
		_, err = s.s.splitDataNodeWithConflict(s.tx, s.allocator, v.parentIndex,
			v.storeRequest.Store[v.storeRequest.PointersToStore-2].Pointer, v.level)
	} else {
		_, err = s.s.splitDataNodeWithoutConflict(s.tx, s.allocator, v.parentIndex,
			v.storeRequest.Store[v.storeRequest.PointersToStore-2].Pointer, v.level)
	}
	return err
}

// AddPointerNode adds pointer node.
func (s *SpaceTest[K, V]) AddPointerNode(v *Entry[K, V], conflict bool) error {
	return s.s.addPointerNode(v, s.tx, s.allocator, conflict)
}

// Query queries the space for a key.
func (s *SpaceTest[K, V]) Query(key TestKey[K]) (V, bool) {
	return s.s.query(key.Key, key.KeyHash)
}

// Find finds the location in the tree for key.
func (s *SpaceTest[K, V]) Find(v *Entry[K, V]) {
	s.s.find(v)
}
