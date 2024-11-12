package space

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/quantum/types"
)

func TestNumOfPointersIsEven(t *testing.T) {
	require.Equal(t, 0, NumOfPointers%2)
}

func TestPointerNodeSize(t *testing.T) {
	require.LessOrEqual(t, unsafe.Sizeof(PointerNode{}), uintptr(types.NodeLength))
}
