package types

import (
	"math"
	"sync/atomic"
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

// Naked returns address without flags.
func (na VolatileAddress) Naked() VolatileAddress {
	return na & FlagNaked
}

// Load loads node address atomically.
func Load(address *VolatileAddress) VolatileAddress {
	return (VolatileAddress)(atomic.LoadUint64((*uint64)(address)))
}

// Store stores node address atomically.
func Store(address *VolatileAddress, value VolatileAddress) {
	atomic.StoreUint64((*uint64)(address), (uint64)(value))
}

const (
	numOfFlags = 2

	// FreeAddress means address is not assigned.
	FreeAddress VolatileAddress = 0

	// FlagNaked is used to retrieve address without flags.
	FlagNaked VolatileAddress = math.MaxUint64 >> numOfFlags
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
