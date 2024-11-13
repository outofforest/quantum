package types

const (
	// UInt64Length is the number of bytes taken by uint64.
	UInt64Length = 8

	// HashLength is the number of bytes taken by hash.
	HashLength = 32

	// BlockLength is the number of bytes in one block used for hashing.
	BlockLength = 64

	// NodeLength is the number of bytes in the node.
	NodeLength = 4096

	// NumOfBlocks defines how many blocks to hash are present in the node.
	NumOfBlocks = NodeLength / BlockLength
)

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

	// StatePointerWithHashMod means slot contains pointer and key hash must be recalculated.
	StatePointerWithHashMod
)

type (
	// SnapshotID is the type for snapshot ID.
	SnapshotID uint64

	// VolatileAddress represents the address of a node in RAM.
	VolatileAddress uint64

	// PersistentAddress represents the address of a node in persistent store.
	PersistentAddress uint64

	// KeyHash is the type for key hash.
	KeyHash uint64

	// Hash represents hash of a node.
	Hash [HashLength]byte

	// SpaceID is the type for space ID.
	SpaceID uint64
)

// Pointer is the pointer to another block.
type Pointer struct {
	SnapshotID        SnapshotID
	Revision          uint64
	VolatileAddress   VolatileAddress
	PersistentAddress PersistentAddress
}

// NodeRoot represents the root of node.
type NodeRoot struct {
	State   *State
	Pointer *Pointer
	Hash    *Hash
}

// Root represents root of the structure.
type Root struct {
	State   State
	Pointer Pointer
	Hash    Hash
}

// DataItem stores single key-value pair.
type DataItem[K, V comparable] struct {
	Hash  KeyHash
	Key   K
	State State
	Value V
}

// SnapshotInfo stores information required to retrieve snapshot.
type SnapshotInfo struct {
	PreviousSnapshotID SnapshotID
	NextSnapshotID     SnapshotID
	DeallocationRoot   Root

	// FIXME (wojciech): Generalize this to any number of spaces.
	Spaces [2]Root
}

// SingularityNode is the root of the store.
type SingularityNode struct {
	Hash            Hash
	FirstSnapshotID SnapshotID
	LastSnapshotID  SnapshotID
	SnapshotRoot    Root
}
