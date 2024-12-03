package space

import (
	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/pipeline"
	"github.com/outofforest/quantum/types"
	"github.com/outofforest/quantum/wal"
)

func NewSpaceTest[K, V comparable](
	space *Space[K, V],
	tx *pipeline.TransactionRequest,
	walRecorder *wal.Recorder,
	allocator *alloc.Allocator,
) *SpaceTest[K, V] {
	return &SpaceTest[K, V]{
		s:           space,
		tx:          tx,
		walRecorder: walRecorder,
		allocator:   allocator,
		hashBuff:    space.NewHashBuff(),
		hashMatches: space.NewHashMatches(),
	}
}

// SpaceTest exposes some private functionality of space to make testing concurrent scenarios possible.
type SpaceTest[K, V comparable] struct {
	s           *Space[K, V]
	tx          *pipeline.TransactionRequest
	walRecorder *wal.Recorder
	allocator   *alloc.Allocator
	hashBuff    []byte
	hashMatches []uint64
}

func (s *SpaceTest[K, V]) NewEntry(
	snapshotID types.SnapshotID,
	key K,
	keyHash types.KeyHash,
	stage uint8,
) (*Entry[K, V], error) {
	v := &Entry[K, V]{}
	if err := s.s.initEntry(v, snapshotID, s.tx, s.walRecorder, s.allocator, key, keyHash, stage); err != nil {
		return nil, err
	}
	return v, nil
}

func (s *SpaceTest[K, V]) SplitDataNode(v *Entry[K, V], snapshotID types.SnapshotID) error {
	_, err := s.s.splitDataNode(snapshotID, s.tx, s.walRecorder, s.allocator, v.parentIndex,
		v.storeRequest.Store[v.storeRequest.PointersToStore-2].Pointer, v.level)
	return err
}

func (s *SpaceTest[K, V]) AddPointerNode(v *Entry[K, V], snapshotID types.SnapshotID, conflict bool) error {
	return s.s.addPointerNode(v, snapshotID, s.tx, s.walRecorder, s.allocator, conflict)
}

func (s *SpaceTest[K, V]) WalkPointers(v *Entry[K, V], snapshotID types.SnapshotID) error {
	return s.s.walkPointers(v, snapshotID, s.tx, s.walRecorder, s.allocator, s.hashBuff)
}

func (s *SpaceTest[K, V]) WalkOnePointer(v *Entry[K, V], snapshotID types.SnapshotID) (bool, error) {
	return s.s.walkOnePointer(v, snapshotID, s.tx, s.walRecorder, s.allocator, s.hashBuff)
}

func (s *SpaceTest[K, V]) WalkDataItems(v *Entry[K, V]) bool {
	return s.s.walkDataItems(v, s.hashMatches)
}
