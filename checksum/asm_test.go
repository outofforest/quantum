// Github actions run on machines not supporting AVX-512 instructions.
//go:build nogithub

//nolint:lll

package checksum

import (
	"bytes"
	"math/bits"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/outofforest/quantum/types"
)

var (
	zeroBlock  [types.BlockLength]byte
	zb         = &zeroBlock[0]
	oneBlock   = bytes.Repeat([]byte{0xff}, types.BlockLength)
	ob         = &oneBlock[0]
	oneMessage = [64]*byte{
		ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, ob, //nolint:lll
	}
	zeroMatrix = [16][64]*byte{
		{zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb}, //nolint:lll
		{zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb}, //nolint:lll
		{zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb}, //nolint:lll
		{zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb}, //nolint:lll
		{zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb}, //nolint:lll
		{zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb}, //nolint:lll
		{zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb}, //nolint:lll
		{zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb}, //nolint:lll
		{zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb}, //nolint:lll
		{zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb}, //nolint:lll
		{zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb}, //nolint:lll
		{zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb}, //nolint:lll
		{zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb}, //nolint:lll
		{zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb}, //nolint:lll
		{zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb}, //nolint:lll
		{zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb, zb}, //nolint:lll
	}
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
	for i := range len(zeroMatrix) {
		matrix := zeroMatrix
		matrix[i] = oneMessage

		var hashes [16]types.Hash
		var hashPointers [16]*byte
		for j := range hashes {
			hashPointers[j] = &hashes[j][0]
		}

		matrixP := &matrix[0][0]
		hashesP := &hashPointers[0]

		Blake3(matrixP, hashesP)

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
	var hashes [16]types.Hash
	var hashPointers [16]*byte
	for i := range hashes {
		hashPointers[i] = &hashes[i][0]
	}

	matrixP := &matrix[0][0]
	hashesP := &hashPointers[0]

	Blake3(matrixP, hashesP)

	for _, h := range hashes {
		assert.Equal(t, zeroValueHash, h)
	}
}

func TestTranspose8x16(t *testing.T) {
	matrix := matrix8x16
	expectedZ := [16][8]uint32{
		{0x00, 0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70},
		{0x01, 0x11, 0x21, 0x31, 0x41, 0x51, 0x61, 0x71},
		{0x02, 0x12, 0x22, 0x32, 0x42, 0x52, 0x62, 0x72},
		{0x03, 0x13, 0x23, 0x33, 0x43, 0x53, 0x63, 0x73},
		{0x04, 0x14, 0x24, 0x34, 0x44, 0x54, 0x64, 0x74},
		{0x05, 0x15, 0x25, 0x35, 0x45, 0x55, 0x65, 0x75},
		{0x06, 0x16, 0x26, 0x36, 0x46, 0x56, 0x66, 0x76},
		{0x07, 0x17, 0x27, 0x37, 0x47, 0x57, 0x67, 0x77},
		{0x08, 0x18, 0x28, 0x38, 0x48, 0x58, 0x68, 0x78},
		{0x09, 0x19, 0x29, 0x39, 0x49, 0x59, 0x69, 0x79},
		{0x0a, 0x1a, 0x2a, 0x3a, 0x4a, 0x5a, 0x6a, 0x7a},
		{0x0b, 0x1b, 0x2b, 0x3b, 0x4b, 0x5b, 0x6b, 0x7b},
		{0x0c, 0x1c, 0x2c, 0x3c, 0x4c, 0x5c, 0x6c, 0x7c},
		{0x0d, 0x1d, 0x2d, 0x3d, 0x4d, 0x5d, 0x6d, 0x7d},
		{0x0e, 0x1e, 0x2e, 0x3e, 0x4e, 0x5e, 0x6e, 0x7e},
		{0x0f, 0x1f, 0x2f, 0x3f, 0x4f, 0x5f, 0x6f, 0x7f},
	}

	var z [16][8]uint32
	Transpose8x16(&matrix[0][0], &z[0][0])

	require.Equal(t, expectedZ, z)
}

func TestTranspose16x16(t *testing.T) {
	matrix := matrix16x16

	expectedZ := [16][16]uint32{
		{0x00, 0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80, 0x90, 0xa0, 0xb0, 0xc0, 0xd0, 0xe0, 0xf0},
		{0x01, 0x11, 0x21, 0x31, 0x41, 0x51, 0x61, 0x71, 0x81, 0x91, 0xa1, 0xb1, 0xc1, 0xd1, 0xe1, 0xf1},
		{0x02, 0x12, 0x22, 0x32, 0x42, 0x52, 0x62, 0x72, 0x82, 0x92, 0xa2, 0xb2, 0xc2, 0xd2, 0xe2, 0xf2},
		{0x03, 0x13, 0x23, 0x33, 0x43, 0x53, 0x63, 0x73, 0x83, 0x93, 0xa3, 0xb3, 0xc3, 0xd3, 0xe3, 0xf3},
		{0x04, 0x14, 0x24, 0x34, 0x44, 0x54, 0x64, 0x74, 0x84, 0x94, 0xa4, 0xb4, 0xc4, 0xd4, 0xe4, 0xf4},
		{0x05, 0x15, 0x25, 0x35, 0x45, 0x55, 0x65, 0x75, 0x85, 0x95, 0xa5, 0xb5, 0xc5, 0xd5, 0xe5, 0xf5},
		{0x06, 0x16, 0x26, 0x36, 0x46, 0x56, 0x66, 0x76, 0x86, 0x96, 0xa6, 0xb6, 0xc6, 0xd6, 0xe6, 0xf6},
		{0x07, 0x17, 0x27, 0x37, 0x47, 0x57, 0x67, 0x77, 0x87, 0x97, 0xa7, 0xb7, 0xc7, 0xd7, 0xe7, 0xf7},
		{0x08, 0x18, 0x28, 0x38, 0x48, 0x58, 0x68, 0x78, 0x88, 0x98, 0xa8, 0xb8, 0xc8, 0xd8, 0xe8, 0xf8},
		{0x09, 0x19, 0x29, 0x39, 0x49, 0x59, 0x69, 0x79, 0x89, 0x99, 0xa9, 0xb9, 0xc9, 0xd9, 0xe9, 0xf9},
		{0x0a, 0x1a, 0x2a, 0x3a, 0x4a, 0x5a, 0x6a, 0x7a, 0x8a, 0x9a, 0xaa, 0xba, 0xca, 0xda, 0xea, 0xfa},
		{0x0b, 0x1b, 0x2b, 0x3b, 0x4b, 0x5b, 0x6b, 0x7b, 0x8b, 0x9b, 0xab, 0xbb, 0xcb, 0xdb, 0xeb, 0xfb},
		{0x0c, 0x1c, 0x2c, 0x3c, 0x4c, 0x5c, 0x6c, 0x7c, 0x8c, 0x9c, 0xac, 0xbc, 0xcc, 0xdc, 0xec, 0xfc},
		{0x0d, 0x1d, 0x2d, 0x3d, 0x4d, 0x5d, 0x6d, 0x7d, 0x8d, 0x9d, 0xad, 0xbd, 0xcd, 0xdd, 0xed, 0xfd},
		{0x0e, 0x1e, 0x2e, 0x3e, 0x4e, 0x5e, 0x6e, 0x7e, 0x8e, 0x9e, 0xae, 0xbe, 0xce, 0xde, 0xee, 0xfe},
		{0x0f, 0x1f, 0x2f, 0x3f, 0x4f, 0x5f, 0x6f, 0x7f, 0x8f, 0x9f, 0xaf, 0xbf, 0xcf, 0xdf, 0xef, 0xff},
	}

	var z [16][16]uint32

	Transpose16x16(&matrix[0][0], &z[0][0])

	require.Equal(t, expectedZ, z)
}

func TestG(t *testing.T) {
	for range 100 {
		var a, b, c, d, mx, my [16]uint32

		randUint32Array(&a)
		randUint32Array(&b)
		randUint32Array(&c)
		randUint32Array(&d)
		randUint32Array(&mx)
		randUint32Array(&my)

		aGo, bGo, cGo, dGo := a, b, c, d

		for i := range a {
			aGo[i], bGo[i], cGo[i], dGo[i] = g(aGo[i], bGo[i], cGo[i], dGo[i], mx[i], my[i])
		}

		G(&a, &b, &c, &d, &mx, &my)

		assert.Equal(t, aGo, a)
		assert.Equal(t, bGo, b)
		assert.Equal(t, cGo, c)
		assert.Equal(t, dGo, d)
	}
}

func g(a, b, c, d, mx, my uint32) (uint32, uint32, uint32, uint32) {
	a += b + mx
	d = bits.RotateLeft32(d^a, -16)
	c += d
	b = bits.RotateLeft32(b^c, -12)
	a += b + my
	d = bits.RotateLeft32(d^a, -8)
	c += d
	b = bits.RotateLeft32(b^c, -7)
	return a, b, c, d
}
