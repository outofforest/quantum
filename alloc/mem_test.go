package alloc

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestAllocate(t *testing.T) {
	const (
		size      = 1000
		alignment = 11
	)

	requireT := require.New(t)

	p, deallocF, err := Allocate(size, alignment, false)
	requireT.NoError(err)
	t.Cleanup(deallocF)

	requireT.Zero(uintptr(p) % alignment)

	b := unsafe.Slice((*byte)(p), size)
	for i := range size {
		b[i] = byte(i)
	}
}
