package persistent

// Store defines the interface of the store.
type Store interface {
	Size() uint64
	Write(offset uint64, data []byte) error
}
