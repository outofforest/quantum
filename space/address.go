package space

import (
	"sync/atomic"

	"github.com/outofforest/quantum/types"
)

const (
	// flagPointerNode says that this is pointer node.
	flagPointerNode = types.FlagNaked + 1

	// flagHashMod says that key hash must be recalculated.
	flagHashMod = flagPointerNode << 1
)

func isFree(address types.VolatileAddress) bool {
	return address == types.FreeAddress
}

func isPointer(address types.VolatileAddress) bool {
	return address.IsSet(flagPointerNode)
}

func isData(address types.VolatileAddress) bool {
	return !isFree(address) && !isPointer(address)
}

func load(address *types.VolatileAddress) types.VolatileAddress {
	return (types.VolatileAddress)(atomic.LoadUint64((*uint64)(address)))
}

func store(address *types.VolatileAddress, value types.VolatileAddress) {
	atomic.StoreUint64((*uint64)(address), (uint64)(value))
}
