package persistent

import "github.com/outofforest/quantum/types"

// Store defines the interface of the store.
type Store interface {
	Write(address types.PhysicalAddress, data []byte) error
}
