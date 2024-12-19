package space

import (
	"runtime"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/quantum/types"
)

func TestDataNode(t *testing.T) {
	requireT := require.New(t)
	node := make([]byte, types.NodeLength)
	defer runtime.KeepAlive(node)

	nodeP := unsafe.Pointer(&node[0])

	dataNode, err := NewDataNodeAssistant[uint32, uint16]()
	requireT.NoError(err)

	keyHashes := dataNode.KeyHashes(nodeP)
	requireT.Equal(dataNode.NumOfItems(), uint64(len(keyHashes)))
	requireT.Equal(nodeP, unsafe.Pointer(&keyHashes[0]))

	requireT.GreaterOrEqual(uintptr(nodeP)+types.NodeLength,
		uintptr(unsafe.Pointer(dataNode.Item(nodeP, dataNode.ItemOffset(dataNode.NumOfItems())))))

	for i := range dataNode.NumOfItems() {
		item := dataNode.Item(nodeP, dataNode.ItemOffset(i))
		item.Key = uint32(i)
		item.Value = uint16(i)
	}

	itemP := unsafe.Add(nodeP, dataNode.NumOfItems()*types.UInt64Length)
	for i := range dataNode.NumOfItems() {
		requireT.Equal(uint32(i), *(*uint32)(itemP))
		requireT.Equal(uint16(i), *(*uint16)(unsafe.Add(itemP, 4)))

		itemP = unsafe.Add(itemP, 8)
	}
}

func TestMaxDataNode(t *testing.T) {
	requireT := require.New(t)

	dataNode, err := NewDataNodeAssistant[uint64, [4080]byte]()
	requireT.NoError(err)
	requireT.Equal(uint64(1), dataNode.NumOfItems())
}

func TestTooBigNode(t *testing.T) {
	requireT := require.New(t)

	_, err := NewDataNodeAssistant[uint64, [4081]byte]()
	requireT.Error(err)
}
