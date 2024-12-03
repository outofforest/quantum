package persistent

import "github.com/outofforest/quantum/types"

// Store defines the interface of the store.
type Store interface {
	Write(address types.NodeAddress, data []byte) error
	Sync() error
	Close()
}
