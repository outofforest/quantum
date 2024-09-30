package quantum

import (
	"github.com/cespare/xxhash"

	"github.com/outofforest/photon"
)

const (
	bitsPerHop   = 4
	arraySize    = 1 << bitsPerHop
	mask         = arraySize - 1
	pageSize     = 512
	uint64Length = 8
)

// State enumerates possible slot states.
type State byte

const (
	stateFree State = iota
	stateData
	statePointer
)

// Config stores configuration.
type Config struct {
	TotalSize uint64
}

// NodeHeader is the header common to all node types.
type NodeHeader struct {
	Version uint64
	HashMod uint64
}

// PointerNode is the node containing pointers to other nodes.
type PointerNode struct {
	Header   NodeHeader
	States   [arraySize]State
	Pointers [arraySize]uint64
}

// DataItem stores single key-value pair.
type DataItem[K, V comparable] struct {
	Hash  uint64
	Key   K
	Value V
}

// DataNode stores the data items.
type DataNode[K, V comparable] struct {
	Header NodeHeader
	States [arraySize]State
	Items  [arraySize]DataItem[K, V]
}

// New creates new quantum store.
func New[K, V comparable](config Config) Snapshot[K, V] {
	s := Snapshot[K, V]{
		version:      0,
		data:         make([]byte, config.TotalSize),
		rootNodeType: stateData,
		defaultValue: *new(V),
	}
	return s
}

// Snapshot represents the state at particular point in time.
type Snapshot[K, V comparable] struct {
	version      uint64
	rootNode     uint64
	rootNodeType State
	hashMod      uint64
	defaultValue V

	data               []byte
	allocatedNodeIndex uint64
}

// Next transitions to the next snapshot of the state.
func (s Snapshot[K, V]) Next() Snapshot[K, V] {
	s.version++
	return s
}

// Get gets the value of the key.
func (s *Snapshot[K, V]) Get(key K) (value V, exists bool) {
	h := hashKey(key, 0)
	nType := s.rootNodeType
	n := s.node(s.rootNode)
	for {
		header := photon.NewFromBytes[NodeHeader](n)

		if header.V.HashMod > 0 {
			h = hashKey(key, header.V.HashMod)
		}

		index := h & mask
		h >>= bitsPerHop

		switch nType {
		case statePointer:
			node := photon.NewFromBytes[PointerNode](n)
			if node.V.States[index] == stateFree {
				return s.defaultValue, false
			}
			nType = node.V.States[index]
			n = s.node(node.V.Pointers[index])
		default:
			node := photon.NewFromBytes[DataNode[K, V]](n)
			if node.V.States[index] == stateFree {
				return s.defaultValue, false
			}
			item := node.V.Items[index]
			if item.Hash == h && item.Key == key {
				return item.Value, true
			}
			return s.defaultValue, false
		}
	}
}

// Set sets the value for the key.
func (s *Snapshot[K, V]) Set(key K, value V) {
	h := hashKey(key, 0)
	nType := s.rootNodeType
	n := s.node(s.rootNode)

	var parentNode photon.Union[*PointerNode]
	var parentIndex uint64

	for {
		header := photon.NewFromBytes[NodeHeader](n)
		if header.V.Version < s.version {
			newNodeIndex, newNodeData := s.allocateNode()
			copy(newNodeData, n)
			header = photon.NewFromBytes[NodeHeader](newNodeData)
			header.V.Version = s.version
			n = newNodeData

			switch {
			case parentNode.V == nil:
				s.rootNode = newNodeIndex
			default:
				parentNode.V.Pointers[parentIndex] = newNodeIndex
			}
		}

		if header.V.HashMod > 0 {
			h = hashKey(key, header.V.HashMod)
		}

		index := h & mask
		h >>= bitsPerHop

		switch nType {
		case statePointer:
			node := photon.NewFromBytes[PointerNode](n)
			if node.V.States[index] == stateFree {
				node.V.States[index] = stateData
				nodeIndex, nodeData := s.allocateNode()
				node.V.Pointers[index] = nodeIndex

				dataNode := photon.NewFromBytes[DataNode[K, V]](nodeData)
				dataNode.V.Header.Version = s.version
			}
			parentIndex = index
			parentNode = node
			nType = node.V.States[index]
			n = s.node(node.V.Pointers[index])
		default:
			node := photon.NewFromBytes[DataNode[K, V]](n)
			if node.V.States[index] == stateFree {
				node.V.States[index] = stateData
				node.V.Items[index] = DataItem[K, V]{
					Hash:  h,
					Key:   key,
					Value: value,
				}
				return
			}

			item := node.V.Items[index]
			var conflict bool
			if item.Hash == h {
				if item.Key == key {
					node.V.Items[index].Value = value
					return
				}

				// hash conflict
				conflict = true
			}

			// conflict or split needed

			pointerNodeIndex, pointerNodeData := s.allocateNode()
			pointerNode := photon.NewFromBytes[PointerNode](pointerNodeData)
			pointerNode.V.Header = NodeHeader{
				Version: s.version,
				HashMod: node.V.Header.HashMod,
			}

			for i := range uint64(arraySize) {
				if node.V.States[i] == stateFree {
					continue
				}

				pointerNode.V.States[i] = stateData
				dataNodeIndex, dataNodeData := s.allocateNode()
				pointerNode.V.Pointers[i] = dataNodeIndex
				dataNode := photon.NewFromBytes[DataNode[K, V]](dataNodeData)
				dataNode.V.Header.Version = s.version

				item := node.V.Items[i]
				var hash uint64
				if conflict && i == index {
					s.hashMod++
					dataNode.V.Header.HashMod = s.hashMod
					hash = hashKey(item.Key, s.hashMod)
				} else {
					hash = item.Hash
				}

				index := hash & mask
				dataNode.V.States[index] = stateData
				dataNode.V.Items[index] = DataItem[K, V]{
					Hash:  hash >> bitsPerHop,
					Key:   item.Key,
					Value: item.Value,
				}
			}

			if parentNode.V == nil {
				s.rootNodeType = statePointer
				s.rootNode = pointerNodeIndex
			} else {
				parentNode.V.States[parentIndex] = statePointer
				parentNode.V.Pointers[parentIndex] = pointerNodeIndex
			}

			parentNode = pointerNode
			parentIndex = index
			n = s.node(pointerNode.V.Pointers[index])
		}
	}
}

func (s *Snapshot[K, V]) node(n uint64) []byte {
	return s.data[n*pageSize : (n+1)*pageSize]
}

func (s *Snapshot[K, V]) allocateNode() (uint64, []byte) {
	// FIXME (wojciech): Copy 0x00 bytes to allocated node.

	s.allocatedNodeIndex++
	return s.allocatedNodeIndex, s.node(s.allocatedNodeIndex)
}

func hashKey[K comparable](key K, hashMod uint64) uint64 {
	var hash uint64
	p := photon.NewFromValue[K](&key)
	if hashMod == 0 {
		hash = xxhash.Sum64(p.B)
	} else {
		// FIXME (wojciech): Remove heap allocation
		b := make([]byte, uint64Length+len(p.B))
		copy(b, photon.NewFromValue(&hashMod).B)
		copy(b[uint64Length:], photon.NewFromValue[K](&key).B)
		hash = xxhash.Sum64(b)
	}

	if isTesting {
		hash = testHash(hash)
	}

	return hash
}

func testHash(hash uint64) uint64 {
	return hash & 0x7fffffff
}
