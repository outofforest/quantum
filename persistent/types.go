package persistent

import "github.com/outofforest/quantum/types"

// Store defines the interface of the store.
type Store interface {
	Size() uint64
	Read(address types.PersistentAddress, data []byte) error
	Write(address types.PersistentAddress, data []byte) error
	Sync() error
	Close()
}
