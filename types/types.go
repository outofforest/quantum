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

	// NodeAddress is the type for node address.
	NodeAddress uint64

	// Hash is the type for key hash.
	Hash uint64

	// SpaceID is the type for space ID.
	SpaceID uint64
)

// Allocator manages memory.
type Allocator interface {
	Node(nodeAddress NodeAddress) unsafe.Pointer
	Allocate() (NodeAddress, unsafe.Pointer, error)
	Deallocate(nodeAddress NodeAddress)
	NodeSize() uint64
}

// SnapshotAllocator manages memory on snapshot level.
type SnapshotAllocator interface {
	SnapshotID() SnapshotID
	SetSnapshotID(snapshotID SnapshotID)
	Allocate() (NodeAddress, unsafe.Pointer, error)
	Deallocate(nodeAddress NodeAddress, srcSnapshotID SnapshotID) error
}

// ListNodeHeader is the header of the list node.
type ListNodeHeader struct {
	Version        uint64
	SnapshotID     SnapshotID
	NumOfItems     uint64
	NumOfSideLists uint64
}

// ListNode represents data stored inside list node.
type ListNode struct {
	Header *ListNodeHeader
	Items  []NodeAddress
}

// Pointer is the pointer to another block.
type Pointer struct {
	Version    uint64
	SnapshotID SnapshotID
	Address    NodeAddress
}

// DataItem stores single key-value pair.
type DataItem[K, V comparable] struct {
	Hash  Hash
	Key   K
	Value V
}

// ParentInfo stores state and item of the slot used to retrieve node from parent pointer.
type ParentInfo struct {
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
	SpaceRoot          SpaceInfo
}

// SingularityNode is the root of the store.
type SingularityNode struct {
	Version         uint64
	FirstSnapshotID SnapshotID
	LastSnapshotID  SnapshotID
	SnapshotRoot    SpaceInfo
}

// DirtyDataNode stores information about modified data node.
type DirtyDataNode struct {
	DataNodeAddress    NodeAddress
	PointerNodeAddress NodeAddress
}

// DirtyListNode stores information about modified list node.
type DirtyListNode struct {
}
