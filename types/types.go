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

// State values are intentionally chosen in a way where following states differ by one bit only.
// It is designed this way to guarantee that goroutines read (maybe outdated but) valid state values.
// This hack is probably not needed because x86 CPUs guarantee write consistency on DWORD level,
// but this business is crazy and nothing is going to surprise me.
const (
	// StateFree means slot is free.
	StateFree State = 0

	// StateData means slot contains data.
	StateData State = 1

	// StatePointer means slot contains pointer.
	StatePointer State = 3
)

// Flags defines pointer flags.
type Flags byte

// IsSet checks if flag is set.
func (f Flags) IsSet(flag Flags) bool {
	return f&flag != FlagNone
}

// Set sets flag.
func (f Flags) Set(flag Flags) Flags {
	return f | flag
}

const (
	// FlagNone is used to verify flags using bitwise and operator.
	FlagNone Flags = 0

	// FlagHashMod says that key hash must be recalculated.
	FlagHashMod Flags = 1 << (iota - 1)
)

type (
	// SnapshotID is the type for snapshot ID.
	SnapshotID uint64

	// NodeAddress represents the address of a node.
	NodeAddress uint64

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
	VolatileAddress   NodeAddress
	PersistentAddress NodeAddress
	Revision          uint32
	State             State
	Flags             Flags
}

// NodeRoot represents the root of node.
type NodeRoot struct {
	Pointer *Pointer
	Hash    *Hash
}

// Root represents root of the structure.
type Root struct {
	Pointer Pointer
	Hash    Hash
}

// DataItem stores single key-value pair.
type DataItem[K, V comparable] struct {
	Key   K
	Value V
}

// SnapshotInfo stores information required to retrieve snapshot.
type SnapshotInfo struct {
	PreviousSnapshotID SnapshotID
	NextSnapshotID     SnapshotID
	DeallocationRoot   Pointer

	// FIXME (wojciech): Generalize this to any number of spaces.
	Spaces [2]Root
}

// SingularityNode is the root of the store.
type SingularityNode struct {
	Hash           Hash
	LastSnapshotID SnapshotID
	SnapshotRoot   Pointer
	WALListTail    NodeAddress
}
