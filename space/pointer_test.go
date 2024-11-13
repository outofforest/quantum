package space

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/quantum/types"
)

func TestPointerNode(t *testing.T) {
	var p PointerNode
	require.Equal(t, 0, len(p.Pointers)%2)
	require.Equal(t, len(p.Hashes), len(p.Pointers))
	require.LessOrEqual(t, unsafe.Sizeof(p), uintptr(types.NodeLength))
	require.Equal(t, uintptr(NumOfBlocksForPointerNode*types.BlockLength), unsafe.Sizeof(p.Hashes))
	require.Equal(t, uintptr(unsafe.Pointer(&p)), uintptr(unsafe.Pointer(&p.Hashes)))
}
