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

	headerSize := unsafe.Sizeof(NodeHeader{})
	headerSize = (headerSize + types.UInt64Length - 1) / types.UInt64Length * types.UInt64Length // memory alignment
	if headerSize >= nodeSize {
		return nil, errors.New("node size is too small")
	}

	pointerSize := unsafe.Sizeof(types.Pointer{})
	numOfPointers := (nodeSize - headerSize) / pointerSize
	if numOfPointers < 2 {
		return nil, errors.New("node size is too small")
	}

	return &NodeAllocator{
		allocator:     allocator,
		listNode:      &Node{},
		numOfPointers: uint64(numOfPointers),
		pointerOffset: headerSize,
	}, nil
}

// NodeAllocator converts nodes from bytes to list objects.
type NodeAllocator struct {
	allocator types.Allocator

	listNode      *Node
	numOfPointers uint64
	pointerOffset uintptr
}

// Get returns object for node.
func (na *NodeAllocator) Get(nodeAddress types.LogicalAddress) *Node {
	return na.project(na.allocator.Node(nodeAddress))
}

// Allocate allocates new object.
func (na *NodeAllocator) Allocate(allocator types.SnapshotAllocator) (types.LogicalAddress, *Node, error) {
	n, node, err := allocator.Allocate()
	if err != nil {
		return 0, nil, err
	}
	return n, na.project(node), nil
}

func (na *NodeAllocator) project(node unsafe.Pointer) *Node {
	na.listNode.Header = photon.FromPointer[NodeHeader](node)
	na.listNode.Pointers = photon.SliceFromPointer[types.Pointer](unsafe.Add(node, na.pointerOffset),
		int(na.numOfPointers))

	return na.listNode
}

// NodeHeader is the header of the list node.
type NodeHeader struct {
	RevisionHeader types.RevisionHeader
	Version        uint64
	NumOfItems     uint64
	NumOfSideLists uint64
}

// Node represents data stored inside list node.
type Node struct {
	Header   *NodeHeader
	Pointers []types.Pointer
}
