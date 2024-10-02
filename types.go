package quantum

const uint64Length = 8

// State enumerates possible slot states.
type State byte

const (
	stateFree State = iota
	stateData
	statePointer
)

// NodeHeader is the header common to all node types.
type NodeHeader struct {
	SnapshotID uint64
	HashMod    uint64
}

// Node represents data stored inside node.
type Node[T comparable] struct {
	Header *NodeHeader
	States []State
	Items  []T
}

// DataItem stores single key-value pair.
type DataItem[K, V comparable] struct {
	Hash  uint64
	Key   K
	Value V
}

// ParentInfo stores state and item of the slot used to retrieve node from parent pointer.
type ParentInfo struct {
	State *State
	Item  *uint64
}

// SpaceInfo stores information required to retrieve space.
type SpaceInfo struct {
	State   State
	Node    uint64
	HashMod uint64
}

// SnapshotInfo stores information required to retrieve snapshot.
type SnapshotInfo struct {
	SnapshotID   uint64
	SnapshotRoot SpaceInfo
	SpaceRoot    SpaceInfo
}
