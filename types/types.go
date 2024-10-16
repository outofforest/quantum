package types

import "unsafe"

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

// Allocator manages memory.
type Allocator interface {
	Node(nodeAddress LogicalAddress) unsafe.Pointer
	Allocate() (LogicalAddress, unsafe.Pointer, error)
	Deallocate(nodeAddress LogicalAddress)
	NodeSize() uint64
}

// SnapshotAllocator manages memory on snapshot level.
type SnapshotAllocator interface {
	SetSnapshotID(snapshotID SnapshotID)
	Allocate() (LogicalAddress, unsafe.Pointer, error)
	Deallocate(nodeAddress LogicalAddress, srcSnapshotID SnapshotID) error
}

// RevisionHeader stores information about node revision. It must be stored as first bytes in the node.
type RevisionHeader struct {
	Revision   uint64
	SnapshotID SnapshotID
}

// Pointer is the pointer to another block.
type Pointer struct {
	LogicalAddress  LogicalAddress
	PhysicalAddress PhysicalAddress
}

// SpacePointer is the pointer to another block in the space.
type SpacePointer struct {
	Version uint64
	Pointer Pointer
}

// DataItem stores single key-value pair.
type DataItem[K, V comparable] struct {
	Hash  Hash
	Key   K
	Value V
}

// ParentEntry stores state and item of the slot used to retrieve node from parent pointer.
type ParentEntry struct {
	State        *State
	SpacePointer *SpacePointer
}

// SpaceInfo stores information required to retrieve space.
type SpaceInfo struct {
	State   State
	Pointer SpacePointer
	HashMod uint64
}

// SnapshotInfo stores information required to retrieve snapshot.
type SnapshotInfo struct {
	PreviousSnapshotID SnapshotID
	NextSnapshotID     SnapshotID
	DeallocationRoot   SpaceInfo
	SpaceRoot          SpaceInfo
}

// SingularityNode is the root of the store.
type SingularityNode struct {
	Version         uint64
	FirstSnapshotID SnapshotID
	LastSnapshotID  SnapshotID
	SnapshotRoot    SpaceInfo
}

// DirtySpaceNode stores information about modified data node.
type DirtySpaceNode struct {
	DataNodeAddress    LogicalAddress
	PointerNodeAddress LogicalAddress
}

// DirtyListNode stores information about modified list node.
type DirtyListNode struct {
}
