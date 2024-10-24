package list

import (
	"unsafe"

	"github.com/pkg/errors"

	"github.com/outofforest/photon"
	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/types"
)

// NewNodeAssistant creates new list node assistant.
func NewNodeAssistant(state *alloc.State) (*NodeAssistant, error) {
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

	return &NodeAssistant{
		state:         state,
		numOfPointers: uint64(numOfPointers),
		pointerOffset: headerSize,
	}, nil
}

// NodeAssistant converts nodes from bytes to list objects.
type NodeAssistant struct {
	state *alloc.State

	numOfPointers uint64
	pointerOffset uintptr
}

// NewNode initializes new node.
func (ns *NodeAssistant) NewNode() *Node {
	return &Node{}
}

// Project projects node bytes to its structure.
func (ns *NodeAssistant) Project(nodeAddress types.LogicalAddress, node *Node) {
	nodeP := ns.state.Node(nodeAddress)
	node.Header = photon.FromPointer[NodeHeader](nodeP)
	node.Pointers = photon.SliceFromPointer[types.Pointer](unsafe.Add(nodeP, ns.pointerOffset),
		int(ns.numOfPointers))
}

// NodeHeader is the header of the list node.
type NodeHeader struct {
	RevisionHeader types.RevisionHeader
	SnapshotID     types.SnapshotID
	NumOfPointers  uint64
	NumOfSideLists uint64
}

// Node represents data stored inside list node.
type Node struct {
	Header   *NodeHeader
	Pointers []types.Pointer
}
