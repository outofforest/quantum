package types

import (
	"math"
)

const (
	// UInt64Length is the number of bytes taken by uint64.
	UInt64Length = 8

	// HashLength is the number of bytes taken by hash.
	HashLength = 32

	// BlockLength is the number of bytes in one block used for hashing.
	BlockLength = 64

	// NodeLength is the number of bytes in the node.
	NodeLength = 4096

	// NumOfSpaces defines available number of spaces.
	// FIXME (wojciech): Generalize this to any number of spaces.
	NumOfSpaces = 2
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

	// PersistentAddress represents the address of a node in persistent memory.
	PersistentAddress uint64

	// VolatileAddress represents the address of a node in volatile memory.
	VolatileAddress uint64
)

// IsSet checks if flag is set.
func (na VolatileAddress) IsSet(flag VolatileAddress) bool {
	return na&flag != FreeAddress
}

// Set sets flag.
func (na VolatileAddress) Set(flag VolatileAddress) VolatileAddress {
	return na | flag
}

const (
	numOfFlags = 2

	// FreeAddress means address is not assigned.
	FreeAddress VolatileAddress = 0

	// FlagNaked is used to retrieve address without flags.
	// Funny fact is that in most of the cases flags are erased automatically by multiplying address by the NodeLength,
	// which causes bit shifts and as a result flags go out of the scope of uint64.
	FlagNaked = math.MaxUint64 >> numOfFlags
)

// Pointer is the pointer to another block.
type Pointer struct {
	SnapshotID        SnapshotID
	VolatileAddress   VolatileAddress
	PersistentAddress PersistentAddress
	// Revision must be placed in aligned location to guarantee atomic reads on x86 CPUs.
	Revision uintptr
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

// ToStore represents node to be stored.
type ToStore struct {
	VolatileAddress VolatileAddress
	Pointer         *Pointer
	Hash            *Hash
}

// ListRoot represents pointer to the list root.
type ListRoot struct {
	VolatileAddress   VolatileAddress
	PersistentAddress PersistentAddress
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
	LastSnapshotID SnapshotID
	SnapshotRoot   Pointer
}
