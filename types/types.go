package types

// UInt64Length is the number of bytes taken by uint64.
const UInt64Length = 8

// State enumerates possible slot states.
type State byte

const (
	// StateFree means slot is free.
	StateFree State = iota

	// StateDeleted means slot is free but was occupied before.
	StateDeleted

	// StateData means slot contains data.
	StateData

	// StatePointer means slot contains pointer.
	StatePointer
)

type (
	// SnapshotID is the type for snapshot ID.
	SnapshotID uint64

	// LogicalAddress represents the address of a node in RAM.
	LogicalAddress uint64

	// PhysicalAddress represents the address of a node on disk.
	PhysicalAddress uint64

	// Hash is the type for key hash.
	Hash uint64

	// SpaceID is the type for space ID.
	SpaceID uint64
)

// RevisionHeader stores information about node revision. It must be stored as first bytes in the node.
type RevisionHeader struct {
	Revision uint64
}

// Pointer is the pointer to another block.
type Pointer struct {
	Version         uint64
	LogicalAddress  LogicalAddress
	PhysicalAddress PhysicalAddress
}

// DataItem stores single key-value pair.
type DataItem[K, V comparable] struct {
	Hash  Hash
	Key   K
	Value V
}

// ParentEntry stores state and item of the slot used to retrieve node from parent pointer.
type ParentEntry struct {
	State   *State
	Pointer *Pointer
}

// SpaceInfo stores information required to retrieve space.
type SpaceInfo struct {
	State   State
	Pointer Pointer
	HashMod uint64
}

// SnapshotInfo stores information required to retrieve snapshot.
type SnapshotInfo struct {
	PreviousSnapshotID SnapshotID
	NextSnapshotID     SnapshotID
	DeallocationRoot   SpaceInfo

	// FIXME (wojciech): Generalize this to any number of spaces.
	Spaces [2]SpaceInfo
}

// SingularityNode is the root of the store.
type SingularityNode struct {
	RevisionHeader
	FirstSnapshotID SnapshotID
	LastSnapshotID  SnapshotID
	SnapshotRoot    SpaceInfo
}

// SpacePointerNodeAllocatedEvent is emitted when new pointer node in space is allocated.
type SpacePointerNodeAllocatedEvent struct {
	NodeAddress           LogicalAddress
	RootPointer           *Pointer
	ImmediateDeallocation bool
}

// SpaceDataNodeAllocatedEvent is emitted when new data node in space is allocated.
type SpaceDataNodeAllocatedEvent struct {
	Pointer               *Pointer
	PNodeAddress          LogicalAddress
	RootPointer           *Pointer
	ImmediateDeallocation bool
}

// SpaceDataNodeUpdatedEvent is emitted when data node is updated in space.
type SpaceDataNodeUpdatedEvent struct {
	Pointer               *Pointer
	PNodeAddress          LogicalAddress
	RootPointer           *Pointer
	ImmediateDeallocation bool
}

// SpaceDataNodeDeallocationEvent is emitted to request space data node deallocation.
type SpaceDataNodeDeallocationEvent struct {
	Pointer               Pointer
	ImmediateDeallocation bool
}

// SpaceDeallocationEvent is emitted to request space deallocation.
type SpaceDeallocationEvent struct {
	SpaceRoot ParentEntry
}

// ListDeallocationEvent is emitted to request list deallocation.
type ListDeallocationEvent struct {
	ListRoot Pointer
}

// SyncEvent is emitted to wait until all the events are processed.
type SyncEvent struct {
	SyncCh chan<- struct{}
}

// DBCommitEvent is emitted to wait until all the events are processed, blocks are stored in the persistent store
// and singularity node might be stored.
type DBCommitEvent struct {
	SingularityNodePointer *Pointer
	SyncCh                 chan<- struct{}
}

// StoreRequest is used to request writing a node to the store.
type StoreRequest struct {
	Revision uint64
	Pointer  *Pointer
	SyncCh   chan<- struct{}
}
