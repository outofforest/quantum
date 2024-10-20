package list

import (
	"unsafe"

	"github.com/pkg/errors"

	"github.com/outofforest/photon"
	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/types"
)

// NewNodeAllocator creates new list node allocator.
func NewNodeAllocator(state *alloc.State) (*NodeAllocator, error) {
	nodeSize := uintptr(state.NodeSize())

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
		state:         state,
		numOfPointers: uint64(numOfPointers),
		pointerOffset: headerSize,
	}, nil
}

// NodeAllocator converts nodes from bytes to list objects.
type NodeAllocator struct {
	state *alloc.State

	numOfPointers uint64
	pointerOffset uintptr
}

// NewNode initializes new node.
func (na *NodeAllocator) NewNode() *Node {
	return &Node{}
}

// Get returns object for node.
func (na *NodeAllocator) Get(nodeAddress types.LogicalAddress, node *Node) {
	na.project(na.state.Node(nodeAddress), node)
}

// Allocate allocates new object.
func (na *NodeAllocator) Allocate(
	pool *alloc.Pool[types.LogicalAddress],
	node *Node,
) (types.LogicalAddress, error) {
	nodeAddress, err := pool.Allocate()
	if err != nil {
		return 0, err
	}

	na.project(na.state.Node(nodeAddress), node)
	return nodeAddress, nil
}

func (na *NodeAllocator) project(nodeP unsafe.Pointer, node *Node) {
	node.Header = photon.FromPointer[NodeHeader](nodeP)
	node.Pointers = photon.SliceFromPointer[types.Pointer](unsafe.Add(nodeP, na.pointerOffset),
		int(na.numOfPointers))
}

// NodeHeader is the header of the list node.
type NodeHeader struct {
	RevisionHeader types.RevisionHeader
	Version        uint64
	NumOfPointers  uint64
	NumOfSideLists uint64
}

// Node represents data stored inside list node.
type Node struct {
	Header   *NodeHeader
	Pointers []types.Pointer
}
