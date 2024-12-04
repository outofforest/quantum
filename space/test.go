package space

import (
	"sort"

	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/pipeline"
	"github.com/outofforest/quantum/types"
	"github.com/outofforest/quantum/wal"
)

// NewSpaceTest creates new wrapper for space testing.
func NewSpaceTest[K, V comparable](
	s *Space[K, V],
	tx *pipeline.TransactionRequest,
	walRecorder *wal.Recorder,
	allocator *alloc.Allocator,
	hashKeyFunc func(key *K, buff []byte, level uint8) types.KeyHash,
) *SpaceTest[K, V] {
	return &SpaceTest[K, V]{
		s:           s,
		tx:          tx,
		walRecorder: walRecorder,
		allocator:   allocator,
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

// KeyExists checks if key is set in the space.
func (s *SpaceTest[K, V]) KeyExists(v *Entry[K, V], snapshotID types.SnapshotID) (bool, error) {
	return s.s.keyExists(v, snapshotID, s.tx, s.walRecorder, s.allocator, s.hashBuff, s.hashMatches, s.hashKeyFunc)
}

// ReadKey reads value for the key.
func (s *SpaceTest[K, V]) ReadKey(v *Entry[K, V], snapshotID types.SnapshotID) (V, error) {
	return s.s.readKey(v, snapshotID, s.tx, s.walRecorder, s.allocator, s.hashBuff, s.hashMatches, s.hashKeyFunc)
}

// DeleteKey deletes key from space.
func (s *SpaceTest[K, V]) DeleteKey(v *Entry[K, V], snapshotID types.SnapshotID) error {
	return s.s.deleteKey(v, snapshotID, s.tx, s.walRecorder, s.allocator, s.hashBuff, s.hashMatches, s.hashKeyFunc)
}

// SetKey sets value for the key.
func (s *SpaceTest[K, V]) SetKey(v *Entry[K, V], snapshotID types.SnapshotID, value V) error {
	return s.s.setKey(v, snapshotID, s.tx, s.walRecorder, s.allocator, value, s.hashBuff, s.hashMatches, s.hashKeyFunc)
}

// SplitDataNode splits data node.
func (s *SpaceTest[K, V]) SplitDataNode(v *Entry[K, V], snapshotID types.SnapshotID) error {
	_, err := s.s.splitDataNode(snapshotID, s.tx, s.walRecorder, s.allocator, v.parentIndex,
		v.storeRequest.Store[v.storeRequest.PointersToStore-2].Pointer, v.level)
	return err
}

// AddPointerNode adds pointer node.
func (s *SpaceTest[K, V]) AddPointerNode(v *Entry[K, V], snapshotID types.SnapshotID, conflict bool) error {
	return s.s.addPointerNode(v, snapshotID, s.tx, s.walRecorder, s.allocator, conflict)
}

// WalkPointers walk all the pointers to find the key.
func (s *SpaceTest[K, V]) WalkPointers(v *Entry[K, V], snapshotID types.SnapshotID) error {
	return s.s.walkPointers(v, snapshotID, s.tx, s.walRecorder, s.allocator, s.hashBuff, s.hashKeyFunc)
}

// WalkOnePointer walks one pointer only.
func (s *SpaceTest[K, V]) WalkOnePointer(v *Entry[K, V], snapshotID types.SnapshotID) (bool, error) {
	return s.s.walkOnePointer(v, snapshotID, s.tx, s.walRecorder, s.allocator, s.hashBuff, s.hashKeyFunc)
}

// WalkDataItems walks items in data node to find position for the key.
func (s *SpaceTest[K, V]) WalkDataItems(v *Entry[K, V]) bool {
	return s.s.walkDataItems(v, s.hashMatches)
}

// Query queries the space for a key.
func (s *SpaceTest[K, V]) Query(key K, keyHash types.KeyHash) (V, bool) {
	return s.s.query(key, keyHash, s.hashBuff, s.hashMatches, s.hashKeyFunc)
}

// Find finds the location in the tree for key.
func (s *SpaceTest[K, V]) Find(v *Entry[K, V], snapshotID types.SnapshotID) error {
	return s.s.find(v, snapshotID, s.tx, s.walRecorder, s.allocator, s.hashBuff, s.hashMatches, s.hashKeyFunc)
}

// Nodes returns the list of nodes allocated by the tree.
func (s *SpaceTest[K, V]) Nodes() []types.NodeAddress {
	switch s.s.config.SpaceRoot.Pointer.VolatileAddress.State() {
	case types.StateFree:
		return nil
	case types.StateData:
		return []types.NodeAddress{s.s.config.SpaceRoot.Pointer.VolatileAddress}
	}

	nodes := []types.NodeAddress{}
	stack := []types.NodeAddress{s.s.config.SpaceRoot.Pointer.VolatileAddress.Naked()}

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

		pointerNode := ProjectPointerNode(s.s.config.State.Node(pointerNodeAddress.Naked()))
		for pi := range pointerNode.Pointers {
			switch pointerNode.Pointers[pi].VolatileAddress.State() {
			case types.StateFree:
			case types.StateData:
				nodes = append(nodes, pointerNode.Pointers[pi].VolatileAddress)
			case types.StatePointer:
				stack = append(stack, pointerNode.Pointers[pi].VolatileAddress.Naked())
			}
		}
	}
}

// Stats returns space-related statistics.
func (s *SpaceTest[K, V]) Stats() (uint64, uint64, uint64, float64) {
	switch s.s.config.SpaceRoot.Pointer.VolatileAddress.State() {
	case types.StateFree:
		return 0, 0, 0, 0
	case types.StateData:
		return 1, 0, 1, 0
	}

	stack := []types.NodeAddress{s.s.config.SpaceRoot.Pointer.VolatileAddress.Naked()}

	levels := map[types.NodeAddress]uint64{
		s.s.config.SpaceRoot.Pointer.VolatileAddress.Naked(): 1,
	}
	var maxLevel, pointerNodes, dataNodes, dataItems uint64

	for {
		if len(stack) == 0 {
			return maxLevel, pointerNodes, dataNodes, float64(dataItems) / float64(dataNodes*s.s.numOfDataItems)
		}

		n := stack[len(stack)-1]
		level := levels[n] + 1
		pointerNodes++
		stack = stack[:len(stack)-1]

		pointerNode := ProjectPointerNode(s.s.config.State.Node(n.Naked()))
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
				for _, _ = range s.s.config.DataNodeAssistant.Iterator(s.s.config.State.Node(
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
