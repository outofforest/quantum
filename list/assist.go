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
	// memory alignment
	headerSize := uint64(unsafe.Sizeof(NodeHeader{})+types.UInt64Length-1) / types.UInt64Length * types.UInt64Length
	if headerSize >= types.NodeLength {
		return nil, errors.New("node size is too small")
	}

	pointerSize := uint64(unsafe.Sizeof(types.Pointer{}))
	numOfPointers := (types.NodeLength - headerSize) / pointerSize
	if numOfPointers < 2 {
		return nil, errors.New("node size is too small")
	}

	return &NodeAssistant{
		state:         state,
		numOfPointers: numOfPointers,
		pointerOffset: headerSize,
	}, nil
}

// NodeAssistant converts nodes from bytes to list objects.
type NodeAssistant struct {
	state *alloc.State

	numOfPointers uint64
	pointerOffset uint64
}

// NewNode initializes new node.
func (ns *NodeAssistant) NewNode() *Node {
	return &Node{}
}

// Project projects node bytes to its structure.
func (ns *NodeAssistant) Project(nodeAddress types.VolatileAddress, node *Node) {
	nodeP := ns.state.Node(nodeAddress)
	node.Header = photon.FromPointer[NodeHeader](nodeP)
	node.Pointers = photon.SliceFromPointer[types.Pointer](unsafe.Add(nodeP, ns.pointerOffset),
		int(ns.numOfPointers))
}

// NodeHeader is the header of the list node.
type NodeHeader struct {
	NumOfPointers  uint64
	NumOfSideLists uint64
}

// Node represents data stored inside list node.
type Node struct {
	Header   *NodeHeader
	Pointers []types.Pointer
}
