package hash

import (
	"math"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/outofforest/quantum/state"
	"github.com/outofforest/quantum/types"
)

var (
	zeroNode = func() []byte {
		b, _, _ := state.Allocate(types.NodeLength, 64, false)
		return unsafe.Slice((*byte)(b), types.NodeLength)
	}()
	zn      = &zeroNode[0]
	oneNode = func() []byte {
		b, _, _ := state.Allocate(types.NodeLength, 64, false)
		bSlice := unsafe.Slice((*byte)(b), types.NodeLength)
		for i := range bSlice {
			bSlice[i] = 0xff
		}
		return bSlice
	}()
	on            = &oneNode[0]
	zeroMatrix    = [16]*byte{zn, zn, zn, zn, zn, zn, zn, zn, zn, zn, zn, zn, zn, zn, zn, zn}
	zeroValueHash = types.Hash{
		0x4f, 0x7e, 0xdc, 0x36, 0xd0, 0xd2, 0xfa, 0x0f, 0x14, 0xc9, 0x33, 0xba, 0x7f, 0x41, 0xe6, 0x5c,
		0xa5, 0x83, 0xf7, 0x79, 0xc9, 0x38, 0xda, 0x75, 0x3c, 0xd4, 0xab, 0x51, 0x3e, 0x82, 0x0e, 0x7d,
	}
	oneValueHash = types.Hash{
		0x77, 0xff, 0x86, 0xf6, 0xeb, 0x79, 0xfc, 0x9a, 0x25, 0xdc, 0xb5, 0x26, 0x54, 0xb9, 0x71, 0xab,
		0x89, 0x12, 0xb9, 0x4c, 0xcb, 0x72, 0xff, 0xaf, 0x5a, 0xcf, 0x52, 0x75, 0x82, 0xa2, 0x3a, 0xee,
	}
)

func TestBlake3OneMessage(t *testing.T) {
	for i := range zeroMatrix {
		matrix := zeroMatrix
		matrix[i] = on

		hashesP, hashesDealloc, err := state.Allocate(16*types.HashLength, 32, false)
		require.NoError(t, err)
		t.Cleanup(hashesDealloc)

		hashes := unsafe.Slice((*types.Hash)(hashesP), 16)

		var hashPointers [16]*byte
		for j := range hashes {
			hashPointers[j] = &hashes[j][0]
		}

		Blake34096(&matrix[0], &hashPointers[0], math.MaxUint16)

		for j, h := range hashes {
			if j == i {
				assert.Equal(t, oneValueHash, h, "false zero i: %d, j: %d", i, j)
			} else {
				assert.Equal(t, zeroValueHash, h, "false one, i: %d, j: %d", i, j)
			}
		}
	}
}

func TestBlake3Zeros(t *testing.T) {
	matrix := zeroMatrix

	hashesP, hashesDealloc, err := state.Allocate(16*types.HashLength, 32, false)
	require.NoError(t, err)
	t.Cleanup(hashesDealloc)

	hashes := unsafe.Slice((*types.Hash)(hashesP), 16)

	var hashPointers [16]*byte
	for j := range hashes {
		hashPointers[j] = &hashes[j][0]
	}

	Blake34096(&matrix[0], &hashPointers[0], math.MaxUint16)

	for _, h := range hashes {
		assert.Equal(t, zeroValueHash, h)
	}
}

func TestLastHashIsStored(t *testing.T) {
	for i := range zeroMatrix {
		matrix := zeroMatrix
		matrix[i] = on

		hashesP, hashesDealloc, err := state.Allocate(2*types.HashLength, 32, false)
		require.NoError(t, err)
		t.Cleanup(hashesDealloc)

		hashes := unsafe.Slice((*types.Hash)(hashesP), 16)

		var hashPointers [16]*byte
		for j := range hashes {
			if j <= i {
				hashPointers[j] = &hashes[0][0]
			} else {
				hashPointers[j] = &hashes[1][0]
			}
		}

		Blake34096(&matrix[0], &hashPointers[0], math.MaxUint16)

		assert.Equal(t, oneValueHash, hashes[0])
		if i < len(zeroMatrix)-1 {
			assert.Equal(t, zeroValueHash, hashes[1])
		}
	}
}
