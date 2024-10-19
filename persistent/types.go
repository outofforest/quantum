package persistent

type Store interface {
	Size() uint64
	Write(offset uint64, data []byte) error
}
