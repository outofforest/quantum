package space

import (
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
