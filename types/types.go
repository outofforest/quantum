package types

import "sync/atomic"

const (
	// UInt64Length is the number of bytes taken by uint64.
	UInt64Length = 8

	// HashLength is the number of bytes taken by hash.
	HashLength = 32

	// BlockLength is the number of bytes in one block used for hashing.
	BlockLength = 64

	// NodeLength is the number of bytes in the node.
	NodeLength = 4096
)

type (
	// SnapshotID is the type for snapshot ID.
	SnapshotID uint64

	// KeyHash is the type for key hash.
	KeyHash uint64

	// Hash represents hash of a node.
	Hash [HashLength]byte

	// SpaceID is the type for space ID.
	SpaceID uint64
)

// NodeAddress represents the address of a node.
type NodeAddress uint64

// State returns node state.
func (na NodeAddress) State() State {
	switch {
	case na.IsSet(FlagPointerNode):
		return StatePointer
	case na == FreeAddress:
		return StateFree
	default:
		return StateData
	}
}

// IsSet checks if flag is set.
func (na NodeAddress) IsSet(flag NodeAddress) bool {
	return na&flag != FreeAddress
}

// Set sets flag.
func (na NodeAddress) Set(flag NodeAddress) NodeAddress {
	return na | flag
}

// Naked returns address without flags.
func (na NodeAddress) Naked() NodeAddress {
	return na & flagNaked
}

// Load loads node address atomically.
func Load(address *NodeAddress) NodeAddress {
	return (NodeAddress)(atomic.LoadUint64((*uint64)(address)))
}

// Store stores node address atomically.
func Store(address *NodeAddress, value NodeAddress) {
	atomic.StoreUint64((*uint64)(address), (uint64)(value))
}

const (
	// FreeAddress means address is not assigned.
	FreeAddress NodeAddress = 0

	// flagNaked is used to retrieve address without flags.
	flagNaked = FlagPointerNode - 1

	// FlagPointerNode says that this is pointer node.
	FlagPointerNode NodeAddress = 1 << 62

	// FlagHashMod says that key hash must be recalculated.
	FlagHashMod NodeAddress = 1 << 63
)

// State enumerates possible slot states.
type State byte

const (
	// StateFree means slot is free.
	StateFree State = iota

	// StateData means slot contains data.
	StateData

	// StatePointer means slot contains pointer.
	StatePointer

	// NumOfSpaces defines available number of spaces.
	// FIXME (wojciech): Generalize this to any number of spaces.
	NumOfSpaces = 2
)

// Pointer is the pointer to another block.
type Pointer struct {
	SnapshotID        SnapshotID
	VolatileAddress   NodeAddress
	PersistentAddress NodeAddress
	Revision          uint32
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

	Spaces [NumOfSpaces]Root
}

// SingularityNode is the root of the store.
type SingularityNode struct {
	Hash           Hash
	LastSnapshotID SnapshotID
	SnapshotRoot   Pointer
	WALListTail    NodeAddress
}
