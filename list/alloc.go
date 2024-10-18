package list

import (
	"unsafe"

	"github.com/pkg/errors"

	"github.com/outofforest/photon"
	"github.com/outofforest/quantum/types"
)

// NewNodeAllocator creates new list node allocator.
func NewNodeAllocator(allocator types.Allocator) (*NodeAllocator, error) {
	nodeSize := uintptr(allocator.NodeSize())

	headerSize := unsafe.Sizeof(types.ListNodeHeader{})
	headerSize = (headerSize + types.UInt64Length - 1) / types.UInt64Length * types.UInt64Length // memory alignment
	if headerSize >= nodeSize {
		return nil, errors.New("node size is too small")
	}

	numOfItems := (nodeSize - headerSize) / types.UInt64Length
	if numOfItems < 2 {
		return nil, errors.New("node size is too small")
	}

	return &NodeAllocator{
		allocator:  allocator,
		listNode:   &types.ListNode{},
		numOfItems: uint64(numOfItems),
		itemOffset: headerSize,
	}, nil
}

// NodeAllocator converts nodes from bytes to list objects.
type NodeAllocator struct {
	allocator types.Allocator

	listNode   *types.ListNode
	numOfItems uint64
	itemOffset uintptr
}

// Get returns object for node.
func (na *NodeAllocator) Get(nodeAddress types.NodeAddress) *types.ListNode {
	return na.project(na.allocator.Node(nodeAddress))
}

// Allocate allocates new object.
func (na *NodeAllocator) Allocate(allocator types.SnapshotAllocator) (types.NodeAddress, *types.ListNode, error) {
	n, node, err := allocator.Allocate()
	if err != nil {
		return 0, nil, err
	}
	return n, na.project(node), nil
}

func (na *NodeAllocator) project(node unsafe.Pointer) *types.ListNode {
	na.listNode.Header = photon.FromPointer[types.ListNodeHeader](node)
	na.listNode.Items = photon.SliceFromPointer[types.NodeAddress](unsafe.Add(node, na.itemOffset), int(na.numOfItems))

	return na.listNode
}
