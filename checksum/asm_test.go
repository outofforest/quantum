// Github actions run on machines not supporting AVX-512 instructions.
//go:build nogithub

//nolint:lll

package checksum

import (
	"math/bits"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	blake3zeebo "github.com/zeebo/blake3"
)

func TestBlake3(t *testing.T) {
	chunks := chunks

	chP := [256]*byte{
		(*byte)(unsafe.Pointer(&chunks[0][0])),
		(*byte)(unsafe.Pointer(&chunks[0][16])),
		(*byte)(unsafe.Pointer(&chunks[0][2*16])),
		(*byte)(unsafe.Pointer(&chunks[0][3*16])),
		(*byte)(unsafe.Pointer(&chunks[0][4*16])),
		(*byte)(unsafe.Pointer(&chunks[0][5*16])),
		(*byte)(unsafe.Pointer(&chunks[0][6*16])),
		(*byte)(unsafe.Pointer(&chunks[0][7*16])),
		(*byte)(unsafe.Pointer(&chunks[0][8*16])),
		(*byte)(unsafe.Pointer(&chunks[0][9*16])),
		(*byte)(unsafe.Pointer(&chunks[0][10*16])),
		(*byte)(unsafe.Pointer(&chunks[0][11*16])),
		(*byte)(unsafe.Pointer(&chunks[0][12*16])),
		(*byte)(unsafe.Pointer(&chunks[0][13*16])),
		(*byte)(unsafe.Pointer(&chunks[0][14*16])),
		(*byte)(unsafe.Pointer(&chunks[0][15*16])),

		(*byte)(unsafe.Pointer(&chunks[1][0])),
		(*byte)(unsafe.Pointer(&chunks[1][16])),
		(*byte)(unsafe.Pointer(&chunks[1][2*16])),
		(*byte)(unsafe.Pointer(&chunks[1][3*16])),
		(*byte)(unsafe.Pointer(&chunks[1][4*16])),
		(*byte)(unsafe.Pointer(&chunks[1][5*16])),
		(*byte)(unsafe.Pointer(&chunks[1][6*16])),
		(*byte)(unsafe.Pointer(&chunks[1][7*16])),
		(*byte)(unsafe.Pointer(&chunks[1][8*16])),
		(*byte)(unsafe.Pointer(&chunks[1][9*16])),
		(*byte)(unsafe.Pointer(&chunks[1][10*16])),
		(*byte)(unsafe.Pointer(&chunks[1][11*16])),
		(*byte)(unsafe.Pointer(&chunks[1][12*16])),
		(*byte)(unsafe.Pointer(&chunks[1][13*16])),
		(*byte)(unsafe.Pointer(&chunks[1][14*16])),
		(*byte)(unsafe.Pointer(&chunks[1][15*16])),

		(*byte)(unsafe.Pointer(&chunks[2][0])),
		(*byte)(unsafe.Pointer(&chunks[2][16])),
		(*byte)(unsafe.Pointer(&chunks[2][2*16])),
		(*byte)(unsafe.Pointer(&chunks[2][3*16])),
		(*byte)(unsafe.Pointer(&chunks[2][4*16])),
		(*byte)(unsafe.Pointer(&chunks[2][5*16])),
		(*byte)(unsafe.Pointer(&chunks[2][6*16])),
		(*byte)(unsafe.Pointer(&chunks[2][7*16])),
		(*byte)(unsafe.Pointer(&chunks[2][8*16])),
		(*byte)(unsafe.Pointer(&chunks[2][9*16])),
		(*byte)(unsafe.Pointer(&chunks[2][10*16])),
		(*byte)(unsafe.Pointer(&chunks[2][11*16])),
		(*byte)(unsafe.Pointer(&chunks[2][12*16])),
		(*byte)(unsafe.Pointer(&chunks[2][13*16])),
		(*byte)(unsafe.Pointer(&chunks[2][14*16])),
		(*byte)(unsafe.Pointer(&chunks[2][15*16])),

		(*byte)(unsafe.Pointer(&chunks[3][0])),
		(*byte)(unsafe.Pointer(&chunks[3][16])),
		(*byte)(unsafe.Pointer(&chunks[3][2*16])),
		(*byte)(unsafe.Pointer(&chunks[3][3*16])),
		(*byte)(unsafe.Pointer(&chunks[3][4*16])),
		(*byte)(unsafe.Pointer(&chunks[3][5*16])),
		(*byte)(unsafe.Pointer(&chunks[3][6*16])),
		(*byte)(unsafe.Pointer(&chunks[3][7*16])),
		(*byte)(unsafe.Pointer(&chunks[3][8*16])),
		(*byte)(unsafe.Pointer(&chunks[3][9*16])),
		(*byte)(unsafe.Pointer(&chunks[3][10*16])),
		(*byte)(unsafe.Pointer(&chunks[3][11*16])),
		(*byte)(unsafe.Pointer(&chunks[3][12*16])),
		(*byte)(unsafe.Pointer(&chunks[3][13*16])),
		(*byte)(unsafe.Pointer(&chunks[3][14*16])),
		(*byte)(unsafe.Pointer(&chunks[3][15*16])),

		(*byte)(unsafe.Pointer(&chunks[4][0])),
		(*byte)(unsafe.Pointer(&chunks[4][16])),
		(*byte)(unsafe.Pointer(&chunks[4][2*16])),
		(*byte)(unsafe.Pointer(&chunks[4][3*16])),
		(*byte)(unsafe.Pointer(&chunks[4][4*16])),
		(*byte)(unsafe.Pointer(&chunks[4][5*16])),
		(*byte)(unsafe.Pointer(&chunks[4][6*16])),
		(*byte)(unsafe.Pointer(&chunks[4][7*16])),
		(*byte)(unsafe.Pointer(&chunks[4][8*16])),
		(*byte)(unsafe.Pointer(&chunks[4][9*16])),
		(*byte)(unsafe.Pointer(&chunks[4][10*16])),
		(*byte)(unsafe.Pointer(&chunks[4][11*16])),
		(*byte)(unsafe.Pointer(&chunks[4][12*16])),
		(*byte)(unsafe.Pointer(&chunks[4][13*16])),
		(*byte)(unsafe.Pointer(&chunks[4][14*16])),
		(*byte)(unsafe.Pointer(&chunks[4][15*16])),

		(*byte)(unsafe.Pointer(&chunks[5][0])),
		(*byte)(unsafe.Pointer(&chunks[5][16])),
		(*byte)(unsafe.Pointer(&chunks[5][2*16])),
		(*byte)(unsafe.Pointer(&chunks[5][3*16])),
		(*byte)(unsafe.Pointer(&chunks[5][4*16])),
		(*byte)(unsafe.Pointer(&chunks[5][5*16])),
		(*byte)(unsafe.Pointer(&chunks[5][6*16])),
		(*byte)(unsafe.Pointer(&chunks[5][7*16])),
		(*byte)(unsafe.Pointer(&chunks[5][8*16])),
		(*byte)(unsafe.Pointer(&chunks[5][9*16])),
		(*byte)(unsafe.Pointer(&chunks[5][10*16])),
		(*byte)(unsafe.Pointer(&chunks[5][11*16])),
		(*byte)(unsafe.Pointer(&chunks[5][12*16])),
		(*byte)(unsafe.Pointer(&chunks[5][13*16])),
		(*byte)(unsafe.Pointer(&chunks[5][14*16])),
		(*byte)(unsafe.Pointer(&chunks[5][15*16])),

		(*byte)(unsafe.Pointer(&chunks[6][0])),
		(*byte)(unsafe.Pointer(&chunks[6][16])),
		(*byte)(unsafe.Pointer(&chunks[6][2*16])),
		(*byte)(unsafe.Pointer(&chunks[6][3*16])),
		(*byte)(unsafe.Pointer(&chunks[6][4*16])),
		(*byte)(unsafe.Pointer(&chunks[6][5*16])),
		(*byte)(unsafe.Pointer(&chunks[6][6*16])),
		(*byte)(unsafe.Pointer(&chunks[6][7*16])),
		(*byte)(unsafe.Pointer(&chunks[6][8*16])),
		(*byte)(unsafe.Pointer(&chunks[6][9*16])),
		(*byte)(unsafe.Pointer(&chunks[6][10*16])),
		(*byte)(unsafe.Pointer(&chunks[6][11*16])),
		(*byte)(unsafe.Pointer(&chunks[6][12*16])),
		(*byte)(unsafe.Pointer(&chunks[6][13*16])),
		(*byte)(unsafe.Pointer(&chunks[6][14*16])),
		(*byte)(unsafe.Pointer(&chunks[6][15*16])),

		(*byte)(unsafe.Pointer(&chunks[7][0])),
		(*byte)(unsafe.Pointer(&chunks[7][16])),
		(*byte)(unsafe.Pointer(&chunks[7][2*16])),
		(*byte)(unsafe.Pointer(&chunks[7][3*16])),
		(*byte)(unsafe.Pointer(&chunks[7][4*16])),
		(*byte)(unsafe.Pointer(&chunks[7][5*16])),
		(*byte)(unsafe.Pointer(&chunks[7][6*16])),
		(*byte)(unsafe.Pointer(&chunks[7][7*16])),
		(*byte)(unsafe.Pointer(&chunks[7][8*16])),
		(*byte)(unsafe.Pointer(&chunks[7][9*16])),
		(*byte)(unsafe.Pointer(&chunks[7][10*16])),
		(*byte)(unsafe.Pointer(&chunks[7][11*16])),
		(*byte)(unsafe.Pointer(&chunks[7][12*16])),
		(*byte)(unsafe.Pointer(&chunks[7][13*16])),
		(*byte)(unsafe.Pointer(&chunks[7][14*16])),
		(*byte)(unsafe.Pointer(&chunks[7][15*16])),

		(*byte)(unsafe.Pointer(&chunks[8][0])),
		(*byte)(unsafe.Pointer(&chunks[8][16])),
		(*byte)(unsafe.Pointer(&chunks[8][2*16])),
		(*byte)(unsafe.Pointer(&chunks[8][3*16])),
		(*byte)(unsafe.Pointer(&chunks[8][4*16])),
		(*byte)(unsafe.Pointer(&chunks[8][5*16])),
		(*byte)(unsafe.Pointer(&chunks[8][6*16])),
		(*byte)(unsafe.Pointer(&chunks[8][7*16])),
		(*byte)(unsafe.Pointer(&chunks[8][8*16])),
		(*byte)(unsafe.Pointer(&chunks[8][9*16])),
		(*byte)(unsafe.Pointer(&chunks[8][10*16])),
		(*byte)(unsafe.Pointer(&chunks[8][11*16])),
		(*byte)(unsafe.Pointer(&chunks[8][12*16])),
		(*byte)(unsafe.Pointer(&chunks[8][13*16])),
		(*byte)(unsafe.Pointer(&chunks[8][14*16])),
		(*byte)(unsafe.Pointer(&chunks[8][15*16])),

		(*byte)(unsafe.Pointer(&chunks[9][0])),
		(*byte)(unsafe.Pointer(&chunks[9][16])),
		(*byte)(unsafe.Pointer(&chunks[9][2*16])),
		(*byte)(unsafe.Pointer(&chunks[9][3*16])),
		(*byte)(unsafe.Pointer(&chunks[9][4*16])),
		(*byte)(unsafe.Pointer(&chunks[9][5*16])),
		(*byte)(unsafe.Pointer(&chunks[9][6*16])),
		(*byte)(unsafe.Pointer(&chunks[9][7*16])),
		(*byte)(unsafe.Pointer(&chunks[9][8*16])),
		(*byte)(unsafe.Pointer(&chunks[9][9*16])),
		(*byte)(unsafe.Pointer(&chunks[9][10*16])),
		(*byte)(unsafe.Pointer(&chunks[9][11*16])),
		(*byte)(unsafe.Pointer(&chunks[9][12*16])),
		(*byte)(unsafe.Pointer(&chunks[9][13*16])),
		(*byte)(unsafe.Pointer(&chunks[9][14*16])),
		(*byte)(unsafe.Pointer(&chunks[9][15*16])),

		(*byte)(unsafe.Pointer(&chunks[10][0])),
		(*byte)(unsafe.Pointer(&chunks[10][16])),
		(*byte)(unsafe.Pointer(&chunks[10][2*16])),
		(*byte)(unsafe.Pointer(&chunks[10][3*16])),
		(*byte)(unsafe.Pointer(&chunks[10][4*16])),
		(*byte)(unsafe.Pointer(&chunks[10][5*16])),
		(*byte)(unsafe.Pointer(&chunks[10][6*16])),
		(*byte)(unsafe.Pointer(&chunks[10][7*16])),
		(*byte)(unsafe.Pointer(&chunks[10][8*16])),
		(*byte)(unsafe.Pointer(&chunks[10][9*16])),
		(*byte)(unsafe.Pointer(&chunks[10][10*16])),
		(*byte)(unsafe.Pointer(&chunks[10][11*16])),
		(*byte)(unsafe.Pointer(&chunks[10][12*16])),
		(*byte)(unsafe.Pointer(&chunks[10][13*16])),
		(*byte)(unsafe.Pointer(&chunks[10][14*16])),
		(*byte)(unsafe.Pointer(&chunks[10][15*16])),

		(*byte)(unsafe.Pointer(&chunks[11][0])),
		(*byte)(unsafe.Pointer(&chunks[11][16])),
		(*byte)(unsafe.Pointer(&chunks[11][2*16])),
		(*byte)(unsafe.Pointer(&chunks[11][3*16])),
		(*byte)(unsafe.Pointer(&chunks[11][4*16])),
		(*byte)(unsafe.Pointer(&chunks[11][5*16])),
		(*byte)(unsafe.Pointer(&chunks[11][6*16])),
		(*byte)(unsafe.Pointer(&chunks[11][7*16])),
		(*byte)(unsafe.Pointer(&chunks[11][8*16])),
		(*byte)(unsafe.Pointer(&chunks[11][9*16])),
		(*byte)(unsafe.Pointer(&chunks[11][10*16])),
		(*byte)(unsafe.Pointer(&chunks[11][11*16])),
		(*byte)(unsafe.Pointer(&chunks[11][12*16])),
		(*byte)(unsafe.Pointer(&chunks[11][13*16])),
		(*byte)(unsafe.Pointer(&chunks[11][14*16])),
		(*byte)(unsafe.Pointer(&chunks[11][15*16])),

		(*byte)(unsafe.Pointer(&chunks[12][0])),
		(*byte)(unsafe.Pointer(&chunks[12][16])),
		(*byte)(unsafe.Pointer(&chunks[12][2*16])),
		(*byte)(unsafe.Pointer(&chunks[12][3*16])),
		(*byte)(unsafe.Pointer(&chunks[12][4*16])),
		(*byte)(unsafe.Pointer(&chunks[12][5*16])),
		(*byte)(unsafe.Pointer(&chunks[12][6*16])),
		(*byte)(unsafe.Pointer(&chunks[12][7*16])),
		(*byte)(unsafe.Pointer(&chunks[12][8*16])),
		(*byte)(unsafe.Pointer(&chunks[12][9*16])),
		(*byte)(unsafe.Pointer(&chunks[12][10*16])),
		(*byte)(unsafe.Pointer(&chunks[12][11*16])),
		(*byte)(unsafe.Pointer(&chunks[12][12*16])),
		(*byte)(unsafe.Pointer(&chunks[12][13*16])),
		(*byte)(unsafe.Pointer(&chunks[12][14*16])),
		(*byte)(unsafe.Pointer(&chunks[12][15*16])),

		(*byte)(unsafe.Pointer(&chunks[13][0])),
		(*byte)(unsafe.Pointer(&chunks[13][16])),
		(*byte)(unsafe.Pointer(&chunks[13][2*16])),
		(*byte)(unsafe.Pointer(&chunks[13][3*16])),
		(*byte)(unsafe.Pointer(&chunks[13][4*16])),
		(*byte)(unsafe.Pointer(&chunks[13][5*16])),
		(*byte)(unsafe.Pointer(&chunks[13][6*16])),
		(*byte)(unsafe.Pointer(&chunks[13][7*16])),
		(*byte)(unsafe.Pointer(&chunks[13][8*16])),
		(*byte)(unsafe.Pointer(&chunks[13][9*16])),
		(*byte)(unsafe.Pointer(&chunks[13][10*16])),
		(*byte)(unsafe.Pointer(&chunks[13][11*16])),
		(*byte)(unsafe.Pointer(&chunks[13][12*16])),
		(*byte)(unsafe.Pointer(&chunks[13][13*16])),
		(*byte)(unsafe.Pointer(&chunks[13][14*16])),
		(*byte)(unsafe.Pointer(&chunks[13][15*16])),

		(*byte)(unsafe.Pointer(&chunks[14][0])),
		(*byte)(unsafe.Pointer(&chunks[14][16])),
		(*byte)(unsafe.Pointer(&chunks[14][2*16])),
		(*byte)(unsafe.Pointer(&chunks[14][3*16])),
		(*byte)(unsafe.Pointer(&chunks[14][4*16])),
		(*byte)(unsafe.Pointer(&chunks[14][5*16])),
		(*byte)(unsafe.Pointer(&chunks[14][6*16])),
		(*byte)(unsafe.Pointer(&chunks[14][7*16])),
		(*byte)(unsafe.Pointer(&chunks[14][8*16])),
		(*byte)(unsafe.Pointer(&chunks[14][9*16])),
		(*byte)(unsafe.Pointer(&chunks[14][10*16])),
		(*byte)(unsafe.Pointer(&chunks[14][11*16])),
		(*byte)(unsafe.Pointer(&chunks[14][12*16])),
		(*byte)(unsafe.Pointer(&chunks[14][13*16])),
		(*byte)(unsafe.Pointer(&chunks[14][14*16])),
		(*byte)(unsafe.Pointer(&chunks[14][15*16])),

		(*byte)(unsafe.Pointer(&chunks[15][0])),
		(*byte)(unsafe.Pointer(&chunks[15][16])),
		(*byte)(unsafe.Pointer(&chunks[15][2*16])),
		(*byte)(unsafe.Pointer(&chunks[15][3*16])),
		(*byte)(unsafe.Pointer(&chunks[15][4*16])),
		(*byte)(unsafe.Pointer(&chunks[15][5*16])),
		(*byte)(unsafe.Pointer(&chunks[15][6*16])),
		(*byte)(unsafe.Pointer(&chunks[15][7*16])),
		(*byte)(unsafe.Pointer(&chunks[15][8*16])),
		(*byte)(unsafe.Pointer(&chunks[15][9*16])),
		(*byte)(unsafe.Pointer(&chunks[15][10*16])),
		(*byte)(unsafe.Pointer(&chunks[15][11*16])),
		(*byte)(unsafe.Pointer(&chunks[15][12*16])),
		(*byte)(unsafe.Pointer(&chunks[15][13*16])),
		(*byte)(unsafe.Pointer(&chunks[15][14*16])),
		(*byte)(unsafe.Pointer(&chunks[15][15*16])),
	}

	var z [16][32]byte
	zP := [16]*byte{
		&z[0][0], &z[1][0], &z[2][0], &z[3][0], &z[4][0], &z[5][0], &z[6][0], &z[7][0],
		&z[8][0], &z[9][0], &z[10][0], &z[11][0], &z[12][0], &z[13][0], &z[14][0], &z[15][0],
	}

	Blake3(&chP[0], &zP[0])

	for i := range len(chunks) {
		ch := &chunks[i][0]
		b3 := blake3zeebo.Sum256(unsafe.Slice((*byte)(unsafe.Pointer(ch)), len(chunks[0])*4))
		assert.Equal(t, b3, z[i])
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
