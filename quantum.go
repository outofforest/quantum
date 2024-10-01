package quantum

import (
	"unsafe"

	"github.com/cespare/xxhash"
	"github.com/pkg/errors"
	"github.com/samber/lo"

	"github.com/outofforest/photon"
)

const uint64Length = 8

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
	NodeSize  uint64
}

// NodeHeader is the header common to all node types.
type NodeHeader struct {
	Version uint64
	HashMod uint64
}

// Node represents data stored inside node.
type Node[T comparable] struct {
	Header *NodeHeader
	States []State
	Items  []T
}

// DataItem stores single key-value pair.
type DataItem[K, V comparable] struct {
	Hash  uint64
	Key   K
	Value V
}

// New creates new quantum store.
func New[K, V comparable](config Config) (Snapshot[K, V], error) {
	pointerNodeDescriptor, err := NewNodeDescriptor[uint64](config.NodeSize)
	if err != nil {
		return Snapshot[K, V]{}, err
	}

	dataNodeDescriptor, err := NewNodeDescriptor[DataItem[K, V]](config.NodeSize)
	if err != nil {
		return Snapshot[K, V]{}, err
	}

	s := Snapshot[K, V]{
		config:                config,
		pointerNodeDescriptor: pointerNodeDescriptor,
		dataNodeDescriptor:    dataNodeDescriptor,
		data:                  make([]byte, config.TotalSize),
		rootInfo: parentInfo{
			State: lo.ToPtr(stateFree),
			Item:  lo.ToPtr[uint64](0),
		},
		defaultValue: *new(V),
	}
	return s, nil
}

// Snapshot represents the state at particular point in time.
type Snapshot[K, V comparable] struct {
	config                Config
	pointerNodeDescriptor NodeDescriptor[uint64]
	dataNodeDescriptor    NodeDescriptor[DataItem[K, V]]

	version      uint64
	rootInfo     parentInfo
	hashMod      uint64
	defaultValue V

	data               []byte
	allocatedNodeIndex uint64
}

// Next transitions to the next snapshot of the state.
func (s Snapshot[K, V]) Next() Snapshot[K, V] {
	s.version++
	s.rootInfo = parentInfo{
		State: lo.ToPtr(*s.rootInfo.State),
		Item:  lo.ToPtr(*s.rootInfo.Item),
	}
	return s
}

// Get gets the value of the key.
func (s *Snapshot[K, V]) Get(key K) (value V, exists bool) {
	h := hashKey(key, 0)
	pInfo := s.rootInfo

	for {
		switch *pInfo.State {
		case stateFree:
			return s.defaultValue, false
		case stateData:
			dataNode := s.dataNodeDescriptor.ToNode(s.node(*pInfo.Item))
			if dataNode.Header.HashMod > 0 {
				h = hashKey(key, dataNode.Header.HashMod)
			}

			index := h & s.dataNodeDescriptor.addressMask

			if dataNode.States[index] == stateFree {
				return s.defaultValue, false
			}
			item := dataNode.Items[index]
			if item.Hash == h && item.Key == key {
				return item.Value, true
			}
			return s.defaultValue, false
		default:
			pointerNode := s.pointerNodeDescriptor.ToNode(s.node(*pInfo.Item))
			if pointerNode.Header.HashMod > 0 {
				h = hashKey(key, pointerNode.Header.HashMod)
			}

			index := h & s.pointerNodeDescriptor.addressMask
			h >>= s.pointerNodeDescriptor.bitsPerHop

			pInfo = parentInfo{
				State: &pointerNode.States[index],
				Item:  &pointerNode.Items[index],
			}
		}
	}
}

// Set sets the value for the key.
func (s *Snapshot[K, V]) Set(key K, value V) {
	s.set(s.rootInfo, DataItem[K, V]{
		Hash:  hashKey(key, 0),
		Key:   key,
		Value: value,
	})
}

type parentInfo struct {
	State *State
	Item  *uint64
}

func (s *Snapshot[K, V]) set(pInfo parentInfo, item DataItem[K, V]) {
	for {
		switch *pInfo.State {
		case stateFree:
			dataNodeIndex, dataNodeData := s.allocateNode()
			dataNode := s.dataNodeDescriptor.ToNode(dataNodeData)
			dataNode.Header.Version = s.version

			*pInfo.State = stateData
			*pInfo.Item = dataNodeIndex

			index := item.Hash & s.dataNodeDescriptor.addressMask
			dataNode.States[index] = stateData
			dataNode.Items[index] = item

			return
		case stateData:
			node := s.node(*pInfo.Item)
			dataNode := s.dataNodeDescriptor.ToNode(node)
			if dataNode.Header.Version < s.version {
				newNodeIndex, newNodeData := s.allocateNode()
				copy(newNodeData, node)
				dataNode = s.dataNodeDescriptor.ToNode(newNodeData)
				dataNode.Header.Version = s.version
				*pInfo.Item = newNodeIndex
			}
			if dataNode.Header.HashMod > 0 {
				item.Hash = hashKey(item.Key, dataNode.Header.HashMod)
			}

			index := item.Hash & s.dataNodeDescriptor.addressMask
			if dataNode.States[index] == stateFree {
				dataNode.States[index] = stateData
				dataNode.Items[index] = item

				return
			}

			if item.Hash == dataNode.Items[index].Hash {
				if item.Key == dataNode.Items[index].Key {
					dataNode.Items[index] = item

					return
				}

				// hash conflict
				s.hashMod++
				dataNode.Header.HashMod = s.hashMod
			}

			s.redistributeNode(pInfo)
			s.set(pInfo, item)

			return
		default:
			node := s.node(*pInfo.Item)
			pointerNode := s.pointerNodeDescriptor.ToNode(node)
			if pointerNode.Header.Version < s.version {
				newNodeIndex, newNodeData := s.allocateNode()
				copy(newNodeData, node)
				pointerNode = s.pointerNodeDescriptor.ToNode(newNodeData)
				pointerNode.Header.Version = s.version
				*pInfo.Item = newNodeIndex
			}
			if pointerNode.Header.HashMod > 0 {
				item.Hash = hashKey(item.Key, pointerNode.Header.HashMod)
			}

			index := item.Hash & s.pointerNodeDescriptor.addressMask
			item.Hash >>= s.pointerNodeDescriptor.bitsPerHop

			pInfo = parentInfo{
				State: &pointerNode.States[index],
				Item:  &pointerNode.Items[index],
			}
		}
	}
}

func (s *Snapshot[K, V]) redistributeNode(pInfo parentInfo) {
	dataNode := s.dataNodeDescriptor.ToNode(s.node(*pInfo.Item))

	pointerNodeIndex, pointerNodeData := s.allocateNode()
	pointerNode := s.pointerNodeDescriptor.ToNode(pointerNodeData)
	*pointerNode.Header = NodeHeader{
		Version: s.version,
		HashMod: dataNode.Header.HashMod,
	}

	*pInfo.State = statePointer
	*pInfo.Item = pointerNodeIndex

	for i := range s.dataNodeDescriptor.numOfItems {
		if dataNode.States[i] == stateFree {
			continue
		}

		s.set(pInfo, dataNode.Items[i])
	}
}

func (s *Snapshot[K, V]) node(n uint64) []byte {
	return s.data[n*s.config.NodeSize : (n+1)*s.config.NodeSize]
}

func (s *Snapshot[K, V]) allocateNode() (uint64, []byte) {
	// FIXME (wojciech): Copy 0x00 bytes to allocated node.

	s.allocatedNodeIndex++
	return s.allocatedNodeIndex, s.node(s.allocatedNodeIndex)
}

// NewNodeDescriptor creates new descriptor converting byte slices of `nodeSize` size to node objects.
func NewNodeDescriptor[T comparable](nodeSize uint64) (NodeDescriptor[T], error) {
	headerSize := uint64(unsafe.Sizeof(NodeHeader{}))
	if headerSize >= nodeSize {
		return NodeDescriptor[T]{}, errors.New("node size is too small")
	}

	stateOffset := headerSize + headerSize%uint64Length
	spaceLeft := nodeSize - stateOffset

	var t T
	itemSize := uint64(unsafe.Sizeof(t))
	itemSize += itemSize % uint64Length

	numOfItems := spaceLeft / (itemSize + 1) // 1 is for slot state
	if numOfItems == 0 {
		return NodeDescriptor[T]{}, errors.New("node size is too small")
	}
	numOfItems, _ = highestPowerOfTwo(numOfItems)
	spaceLeft -= numOfItems
	spaceLeft -= numOfItems % uint64Length

	numOfItems = spaceLeft / itemSize
	numOfItems, bitsPerHop := highestPowerOfTwo(numOfItems)
	if numOfItems == 0 {
		return NodeDescriptor[T]{}, errors.New("node size is too small")
	}

	return NodeDescriptor[T]{
		numOfItems:  numOfItems,
		itemSize:    itemSize,
		stateOffset: stateOffset,
		itemOffset:  nodeSize - spaceLeft,
		addressMask: numOfItems - 1,
		bitsPerHop:  bitsPerHop,
	}, nil
}

// NodeDescriptor describes the data structure of node.
type NodeDescriptor[T comparable] struct {
	numOfItems  uint64
	itemSize    uint64
	stateOffset uint64
	itemOffset  uint64
	addressMask uint64
	bitsPerHop  uint64
}

// ToNode converts byte representation of the node to object.
func (nd NodeDescriptor[T]) ToNode(data []byte) Node[T] {
	return Node[T]{
		Header: (*NodeHeader)(unsafe.Pointer(&data[0])),
		States: unsafe.Slice((*State)(unsafe.Pointer(&data[nd.stateOffset])), nd.numOfItems),
		Items:  unsafe.Slice((*T)(unsafe.Pointer(&data[nd.itemOffset])), nd.numOfItems),
	}
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

func highestPowerOfTwo(n uint64) (uint64, uint64) {
	var m uint64 = 1
	var p uint64
	for m <= n {
		m <<= 1 // Multiply m by 2 (left shift)
		p++
	}
	return m >> 1, p - 1 // Divide by 2 (right shift) to get the highest power of 2 <= n
}
