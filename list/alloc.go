package list

import (
	"unsafe"

	"github.com/pkg/errors"

	"github.com/outofforest/photon"
	"github.com/outofforest/quantum/types"
)

// NewNodeAllocator creates new list node allocator.
func NewNodeAllocator(allocator types.Allocator) (NodeAllocator, error) {
	nodeSize := allocator.NodeSize()

	headerSize := uint64(unsafe.Sizeof(types.ListNodeHeader{}))
	if headerSize >= nodeSize {
		return NodeAllocator{}, errors.New("node size is too small")
	}

	stateOffset := headerSize + headerSize%types.UInt64Length
	spaceLeft := nodeSize - stateOffset

	numOfItems := spaceLeft / types.UInt64Length
	if numOfItems < 2 {
		return NodeAllocator{}, errors.New("node size is too small")
	}

	return NodeAllocator{
		allocator:  allocator,
		numOfItems: int(numOfItems),
		itemOffset: nodeSize - spaceLeft,
	}, nil
}

// NodeAllocator converts nodes from bytes to list objects.
type NodeAllocator struct {
	allocator types.Allocator

	numOfItems int
	itemOffset uint64
}

// Get returns object for node.
func (na NodeAllocator) Get(nodeAddress types.NodeAddress) types.ListNode {
	return na.project(na.allocator.Node(nodeAddress))
}

// Allocate allocates new object.
func (na NodeAllocator) Allocate(allocator types.SnapshotAllocator) (types.NodeAddress, types.ListNode, error) {
	n, node, err := allocator.Allocate()
	if err != nil {
		return 0, types.ListNode{}, err
	}
	return n, na.project(node), nil
}

// Copy allocates copy of existing object.
func (na NodeAllocator) Copy(
	allocator types.SnapshotAllocator,
	nodeAddress types.NodeAddress,
) (types.NodeAddress, types.ListNode, error) {
	n, node, err := allocator.Copy(nodeAddress)
	if err != nil {
		return 0, types.ListNode{}, err
	}
	return n, na.project(node), nil
}

func (na NodeAllocator) project(node []byte) types.ListNode {
	return types.ListNode{
		Header: photon.FromBytes[types.ListNodeHeader](node),
		Items:  photon.SliceFromBytes[types.NodeAddress](node[na.itemOffset:], na.numOfItems),
	}
}
