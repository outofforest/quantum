package types

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/quantum/types"
)

func TestPointerNode(t *testing.T) {
	require.LessOrEqual(t, unsafe.Sizeof(Node{}), uintptr(types.NodeLength))
}
