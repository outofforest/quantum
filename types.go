package quantum

const uint64Length = 8

// State enumerates possible slot states.
type State byte

const (
	stateFree State = iota
	stateData
	statePointer
)

type (
	// SnapshotID is the type for snapshot ID.
	SnapshotID uint64

	// NodeAddress is the type for node address.
	NodeAddress uint64

	// Hash is the type for key hash.
	Hash uint64

	// SpaceID is the type for space ID.
	SpaceID uint64
)

// SpaceNodeHeader is the header common to all space node types.
type SpaceNodeHeader struct {
	SnapshotID SnapshotID
	HashMod    uint64
}

// SpaceNode represents data stored inside space node.
type SpaceNode[T comparable] struct {
	Header *SpaceNodeHeader
	States []State
	Items  []T
}

// ListNodeHeader is the header of the list node.
type ListNodeHeader struct {
	SnapshotID     SnapshotID
	NumOfItems     uint64
	NumOfSideLists uint64
}

// ListNode represents data stored inside list node.
type ListNode struct {
	Header *ListNodeHeader
	Items  []NodeAddress
}

// DataItem stores single key-value pair.
type DataItem[K, V comparable] struct {
	Hash  Hash
	Key   K
	Value V
}

// ParentInfo stores state and item of the slot used to retrieve node from parent pointer.
type ParentInfo struct {
	State *State
	Item  *NodeAddress
}

// SpaceInfo stores information required to retrieve space.
type SpaceInfo struct {
	State   State
	Node    NodeAddress
	HashMod uint64
}

// SnapshotInfo stores information required to retrieve snapshot.
type SnapshotInfo struct {
	SnapshotID       SnapshotID
	DeallocationRoot SpaceInfo
	SpaceRoot        SpaceInfo
}

// SingularityNode is the root of the store.
type SingularityNode struct {
	SnapshotID   SnapshotID
	SnapshotRoot SpaceInfo
}
