package checksum

import (
	"crypto/rand"
	"fmt"
	"io"
	"math/bits"
	"testing"
	"unsafe"
)

var (
	x      = [16]uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	y      = [16]uint32{17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
	matrix = [16][16]uint32{
		{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f},
		{0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f},
		{0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f},
		{0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3a, 0x3b, 0x3c, 0x3d, 0x3e, 0x3f},
		{0x40, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x49, 0x4a, 0x4b, 0x4c, 0x4d, 0x4e, 0x4f},
		{0x50, 0x51, 0x52, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58, 0x59, 0x5a, 0x5b, 0x5c, 0x5d, 0x5e, 0x5f},
		{0x60, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6a, 0x6b, 0x6c, 0x6d, 0x6e, 0x6f},
		{0x70, 0x71, 0x72, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78, 0x79, 0x7a, 0x7b, 0x7c, 0x7d, 0x7e, 0x7f},
		{0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8a, 0x8b, 0x8c, 0x8d, 0x8e, 0x8f},
		{0x90, 0x91, 0x92, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0x99, 0x9a, 0x9b, 0x9c, 0x9d, 0x9e, 0x9f},
		{0xa0, 0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8, 0xa9, 0xaa, 0xab, 0xac, 0xad, 0xae, 0xaf},
		{0xb0, 0xb1, 0xb2, 0xb3, 0xb4, 0xb5, 0xb6, 0xb7, 0xb8, 0xb9, 0xba, 0xbb, 0xbc, 0xbd, 0xbe, 0xbf},
		{0xc0, 0xc1, 0xc2, 0xc3, 0xc4, 0xc5, 0xc6, 0xc7, 0xc8, 0xc9, 0xca, 0xcb, 0xcc, 0xcd, 0xce, 0xcf},
		{0xd0, 0xd1, 0xd2, 0xd3, 0xd4, 0xd5, 0xd6, 0xd7, 0xd8, 0xd9, 0xda, 0xdb, 0xdc, 0xdd, 0xde, 0xdf},
		{0xe0, 0xe1, 0xe2, 0xe3, 0xe4, 0xe5, 0xe6, 0xe7, 0xe8, 0xe9, 0xea, 0xeb, 0xec, 0xed, 0xee, 0xef},
		{0xf0, 0xf1, 0xf2, 0xf3, 0xf4, 0xf5, 0xf6, 0xf7, 0xf8, 0xf9, 0xfa, 0xfb, 0xfc, 0xfd, 0xfe, 0xff},
	}
)

func BenchmarkTransposeGo(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	x := matrix
	var z [16][16]uint32

	b.StartTimer()
	for range b.N {
		z[0][0] = x[0][0]
		z[0][1] = x[1][0]
		z[0][2] = x[2][0]
		z[0][3] = x[3][0]
		z[0][4] = x[4][0]
		z[0][5] = x[5][0]
		z[0][6] = x[6][0]
		z[0][7] = x[7][0]
		z[0][8] = x[8][0]
		z[0][9] = x[9][0]
		z[0][10] = x[10][0]
		z[0][11] = x[11][0]
		z[0][12] = x[12][0]
		z[0][13] = x[13][0]
		z[0][14] = x[14][0]
		z[0][15] = x[15][0]

		z[1][0] = x[0][1]
		z[1][1] = x[1][1]
		z[1][2] = x[2][1]
		z[1][3] = x[3][1]
		z[1][4] = x[4][1]
		z[1][5] = x[5][1]
		z[1][6] = x[6][1]
		z[1][7] = x[7][1]
		z[1][8] = x[8][1]
		z[1][9] = x[9][1]
		z[1][10] = x[10][1]
		z[1][11] = x[11][1]
		z[1][12] = x[12][1]
		z[1][13] = x[13][1]
		z[1][14] = x[14][1]
		z[1][15] = x[15][1]

		z[2][0] = x[0][2]
		z[2][1] = x[1][2]
		z[2][2] = x[2][2]
		z[2][3] = x[3][2]
		z[2][4] = x[4][2]
		z[2][5] = x[5][2]
		z[2][6] = x[6][2]
		z[2][7] = x[7][2]
		z[2][8] = x[8][2]
		z[2][9] = x[9][2]
		z[2][10] = x[10][2]
		z[2][11] = x[11][2]
		z[2][12] = x[12][2]
		z[2][13] = x[13][2]
		z[2][14] = x[14][2]
		z[2][15] = x[15][2]

		z[3][0] = x[0][3]
		z[3][1] = x[1][3]
		z[3][2] = x[2][3]
		z[3][3] = x[3][3]
		z[3][4] = x[4][3]
		z[3][5] = x[5][3]
		z[3][6] = x[6][3]
		z[3][7] = x[7][3]
		z[3][8] = x[8][3]
		z[3][9] = x[9][3]
		z[3][10] = x[10][3]
		z[3][11] = x[11][3]
		z[3][12] = x[12][3]
		z[3][13] = x[13][3]
		z[3][14] = x[14][3]
		z[3][15] = x[15][3]

		z[4][0] = x[0][4]
		z[4][1] = x[1][4]
		z[4][2] = x[2][4]
		z[4][3] = x[3][4]
		z[4][4] = x[4][4]
		z[4][5] = x[5][4]
		z[4][6] = x[6][4]
		z[4][7] = x[7][4]
		z[4][8] = x[8][4]
		z[4][9] = x[9][4]
		z[4][10] = x[10][4]
		z[4][11] = x[11][4]
		z[4][12] = x[12][4]
		z[4][13] = x[13][4]
		z[4][14] = x[14][4]
		z[4][15] = x[15][4]

		z[5][0] = x[0][5]
		z[5][1] = x[1][5]
		z[5][2] = x[2][5]
		z[5][3] = x[3][5]
		z[5][4] = x[4][5]
		z[5][5] = x[5][5]
		z[5][6] = x[6][5]
		z[5][7] = x[7][5]
		z[5][8] = x[8][5]
		z[5][9] = x[9][5]
		z[5][10] = x[10][5]
		z[5][11] = x[11][5]
		z[5][12] = x[12][5]
		z[5][13] = x[13][5]
		z[5][14] = x[14][5]
		z[5][15] = x[15][5]

		z[6][0] = x[0][6]
		z[6][1] = x[1][6]
		z[6][2] = x[2][6]
		z[6][3] = x[3][6]
		z[6][4] = x[4][6]
		z[6][5] = x[5][6]
		z[6][6] = x[6][6]
		z[6][7] = x[7][6]
		z[6][8] = x[8][6]
		z[6][9] = x[9][6]
		z[6][10] = x[10][6]
		z[6][11] = x[11][6]
		z[6][12] = x[12][6]
		z[6][13] = x[13][6]
		z[6][14] = x[14][6]
		z[6][15] = x[15][6]

		z[7][0] = x[0][7]
		z[7][1] = x[1][7]
		z[7][2] = x[2][7]
		z[7][3] = x[3][7]
		z[7][4] = x[4][7]
		z[7][5] = x[5][7]
		z[7][6] = x[6][7]
		z[7][7] = x[7][7]
		z[7][8] = x[8][7]
		z[7][9] = x[9][7]
		z[7][10] = x[10][7]
		z[7][11] = x[11][7]
		z[7][12] = x[12][7]
		z[7][13] = x[13][7]
		z[7][14] = x[14][7]
		z[7][15] = x[15][7]

		z[8][0] = x[0][8]
		z[8][1] = x[1][8]
		z[8][2] = x[2][8]
		z[8][3] = x[3][8]
		z[8][4] = x[4][8]
		z[8][5] = x[5][8]
		z[8][6] = x[6][8]
		z[8][7] = x[7][8]
		z[8][8] = x[8][8]
		z[8][9] = x[9][8]
		z[8][10] = x[10][8]
		z[8][11] = x[11][8]
		z[8][12] = x[12][8]
		z[8][13] = x[13][8]
		z[8][14] = x[14][8]
		z[8][15] = x[15][8]

		z[9][0] = x[0][9]
		z[9][1] = x[1][9]
		z[9][2] = x[2][9]
		z[9][3] = x[3][9]
		z[9][4] = x[4][9]
		z[9][5] = x[5][9]
		z[9][6] = x[6][9]
		z[9][7] = x[7][9]
		z[9][8] = x[8][9]
		z[9][9] = x[9][9]
		z[9][10] = x[10][9]
		z[9][11] = x[11][9]
		z[9][12] = x[12][9]
		z[9][13] = x[13][9]
		z[9][14] = x[14][9]
		z[9][15] = x[15][9]

		z[10][0] = x[0][10]
		z[10][1] = x[1][10]
		z[10][2] = x[2][10]
		z[10][3] = x[3][10]
		z[10][4] = x[4][10]
		z[10][5] = x[5][10]
		z[10][6] = x[6][10]
		z[10][7] = x[7][10]
		z[10][8] = x[8][10]
		z[10][9] = x[9][10]
		z[10][10] = x[10][10]
		z[10][11] = x[11][10]
		z[10][12] = x[12][10]
		z[10][13] = x[13][10]
		z[10][14] = x[14][10]
		z[10][15] = x[15][10]

		z[11][0] = x[0][11]
		z[11][1] = x[1][11]
		z[11][2] = x[2][11]
		z[11][3] = x[3][11]
		z[11][4] = x[4][11]
		z[11][5] = x[5][11]
		z[11][6] = x[6][11]
		z[11][7] = x[7][11]
		z[11][8] = x[8][11]
		z[11][9] = x[9][11]
		z[11][10] = x[10][11]
		z[11][11] = x[11][11]
		z[11][12] = x[12][11]
		z[11][13] = x[13][11]
		z[11][14] = x[14][11]
		z[11][15] = x[15][11]

		z[12][0] = x[0][12]
		z[12][1] = x[1][12]
		z[12][2] = x[2][12]
		z[12][3] = x[3][12]
		z[12][4] = x[4][12]
		z[12][5] = x[5][12]
		z[12][6] = x[6][12]
		z[12][7] = x[7][12]
		z[12][8] = x[8][12]
		z[12][9] = x[9][12]
		z[12][10] = x[10][12]
		z[12][11] = x[11][12]
		z[12][12] = x[12][12]
		z[12][13] = x[13][12]
		z[12][14] = x[14][12]
		z[12][15] = x[15][12]

		z[13][0] = x[0][13]
		z[13][1] = x[1][13]
		z[13][2] = x[2][13]
		z[13][3] = x[3][13]
		z[13][4] = x[4][13]
		z[13][5] = x[5][13]
		z[13][6] = x[6][13]
		z[13][7] = x[7][13]
		z[13][8] = x[8][13]
		z[13][9] = x[9][13]
		z[13][10] = x[10][13]
		z[13][11] = x[11][13]
		z[13][12] = x[12][13]
		z[13][13] = x[13][13]
		z[13][14] = x[14][13]
		z[13][15] = x[15][13]

		z[14][0] = x[0][14]
		z[14][1] = x[1][14]
		z[14][2] = x[2][14]
		z[14][3] = x[3][14]
		z[14][4] = x[4][14]
		z[14][5] = x[5][14]
		z[14][6] = x[6][14]
		z[14][7] = x[7][14]
		z[14][8] = x[8][14]
		z[14][9] = x[9][14]
		z[14][10] = x[10][14]
		z[14][11] = x[11][14]
		z[14][12] = x[12][14]
		z[14][13] = x[13][14]
		z[14][14] = x[14][14]
		z[14][15] = x[15][14]

		z[15][0] = x[0][15]
		z[15][1] = x[1][15]
		z[15][2] = x[2][15]
		z[15][3] = x[3][15]
		z[15][4] = x[4][15]
		z[15][5] = x[5][15]
		z[15][6] = x[6][15]
		z[15][7] = x[7][15]
		z[15][8] = x[8][15]
		z[15][9] = x[9][15]
		z[15][10] = x[10][15]
		z[15][11] = x[11][15]
		z[15][12] = x[12][15]
		z[15][13] = x[13][15]
		z[15][14] = x[14][15]
		z[15][15] = x[15][15]
	}
	b.StopTimer()

	_, _ = fmt.Fprint(io.Discard, z)
}

func BenchmarkTransposeAVX(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	x := matrix
	var z [16][16]uint32

	xP := &[16]*[16]uint32{
		&x[0], &x[1], &x[2], &x[3], &x[4], &x[5], &x[6], &x[7],
		&x[8], &x[9], &x[10], &x[11], &x[12], &x[13], &x[14], &x[15],
	}

	b.StartTimer()
	for range b.N {
		Transpose(xP, &z)
	}
	b.StopTimer()

	_, _ = fmt.Fprint(io.Discard, z)
}

func BenchmarkGGo(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	var va, vb, vc, vd, vmx, vmy [16]uint32

	randUint32Array(&va)
	randUint32Array(&vb)
	randUint32Array(&vc)
	randUint32Array(&vd)
	randUint32Array(&vmx)
	randUint32Array(&vmy)

	b.StartTimer()
	for range b.N {
		va[0], va[0], va[0], va[0] = g(va[0], va[0], va[0], va[0], va[0], va[0])
		va[1], va[1], va[1], va[1] = g(va[1], va[1], va[1], va[1], va[1], va[1])
		va[2], va[2], va[2], va[2] = g(va[2], va[2], va[2], va[2], va[2], va[2])
		va[3], va[3], va[3], va[3] = g(va[3], va[3], va[3], va[3], va[3], va[3])
		va[4], va[4], va[4], va[4] = g(va[4], va[4], va[4], va[4], va[4], va[4])
		va[5], va[5], va[5], va[5] = g(va[5], va[5], va[5], va[5], va[5], va[5])
		va[6], va[6], va[6], va[6] = g(va[6], va[6], va[6], va[6], va[6], va[6])
		va[7], va[7], va[7], va[7] = g(va[7], va[7], va[7], va[7], va[7], va[7])
		va[8], va[8], va[8], va[8] = g(va[8], va[8], va[8], va[8], va[8], va[8])
		va[9], va[9], va[9], va[9] = g(va[9], va[9], va[9], va[9], va[9], va[9])
		va[10], va[10], va[10], va[10] = g(va[10], va[10], va[10], va[10], va[10], va[10])
		va[11], va[11], va[11], va[11] = g(va[11], va[11], va[11], va[11], va[11], va[11])
		va[12], va[12], va[12], va[12] = g(va[12], va[12], va[12], va[12], va[12], va[12])
		va[13], va[13], va[13], va[13] = g(va[13], va[13], va[13], va[13], va[13], va[13])
		va[14], va[14], va[14], va[14] = g(va[14], va[14], va[14], va[14], va[14], va[14])
		va[15], va[15], va[15], va[15] = g(va[15], va[15], va[15], va[15], va[15], va[15])
	}
	b.StopTimer()
}

func BenchmarkGAVX(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	var va, vb, vc, vd, vmx, vmy [16]uint32

	randUint32Array(&va)
	randUint32Array(&vb)
	randUint32Array(&vc)
	randUint32Array(&vd)
	randUint32Array(&vmx)
	randUint32Array(&vmy)

	b.StartTimer()
	for range b.N {
		G(&va, &vb, &vc, &vd, &vmx, &vmy)
	}

	b.StopTimer()
}

func BenchmarkAddGo(b *testing.B) {
	x := x
	y := y

	for i := 0; i < b.N; i++ {
		x[0] += y[0]
		x[1] += y[1]
		x[2] += y[2]
		x[3] += y[3]
		x[4] += y[4]
		x[5] += y[5]
		x[6] += y[6]
		x[7] += y[7]
		x[8] += y[8]
		x[9] += y[9]
		x[10] += y[10]
		x[11] += y[11]
		x[12] += y[12]
		x[13] += y[13]
		x[14] += y[14]
		x[15] += y[15]

		x[0] += y[0]
		x[1] += y[1]
		x[2] += y[2]
		x[3] += y[3]
		x[4] += y[4]
		x[5] += y[5]
		x[6] += y[6]
		x[7] += y[7]
		x[8] += y[8]
		x[9] += y[9]
		x[10] += y[10]
		x[11] += y[11]
		x[12] += y[12]
		x[13] += y[13]
		x[14] += y[14]
		x[15] += y[15]

		x[0] += y[0]
		x[1] += y[1]
		x[2] += y[2]
		x[3] += y[3]
		x[4] += y[4]
		x[5] += y[5]
		x[6] += y[6]
		x[7] += y[7]
		x[8] += y[8]
		x[9] += y[9]
		x[10] += y[10]
		x[11] += y[11]
		x[12] += y[12]
		x[13] += y[13]
		x[14] += y[14]
		x[15] += y[15]

		x[0] += y[0]
		x[1] += y[1]
		x[2] += y[2]
		x[3] += y[3]
		x[4] += y[4]
		x[5] += y[5]
		x[6] += y[6]
		x[7] += y[7]
		x[8] += y[8]
		x[9] += y[9]
		x[10] += y[10]
		x[11] += y[11]
		x[12] += y[12]
		x[13] += y[13]
		x[14] += y[14]
		x[15] += y[15]

		x[0] += y[0]
		x[1] += y[1]
		x[2] += y[2]
		x[3] += y[3]
		x[4] += y[4]
		x[5] += y[5]
		x[6] += y[6]
		x[7] += y[7]
		x[8] += y[8]
		x[9] += y[9]
		x[10] += y[10]
		x[11] += y[11]
		x[12] += y[12]
		x[13] += y[13]
		x[14] += y[14]
		x[15] += y[15]

		x[0] += y[0]
		x[1] += y[1]
		x[2] += y[2]
		x[3] += y[3]
		x[4] += y[4]
		x[5] += y[5]
		x[6] += y[6]
		x[7] += y[7]
		x[8] += y[8]
		x[9] += y[9]
		x[10] += y[10]
		x[11] += y[11]
		x[12] += y[12]
		x[13] += y[13]
		x[14] += y[14]
		x[15] += y[15]

		x[0] += y[0]
		x[1] += y[1]
		x[2] += y[2]
		x[3] += y[3]
		x[4] += y[4]
		x[5] += y[5]
		x[6] += y[6]
		x[7] += y[7]
		x[8] += y[8]
		x[9] += y[9]
		x[10] += y[10]
		x[11] += y[11]
		x[12] += y[12]
		x[13] += y[13]
		x[14] += y[14]
		x[15] += y[15]

		x[0] += y[0]
		x[1] += y[1]
		x[2] += y[2]
		x[3] += y[3]
		x[4] += y[4]
		x[5] += y[5]
		x[6] += y[6]
		x[7] += y[7]
		x[8] += y[8]
		x[9] += y[9]
		x[10] += y[10]
		x[11] += y[11]
		x[12] += y[12]
		x[13] += y[13]
		x[14] += y[14]
		x[15] += y[15]

		x[0] += y[0]
		x[1] += y[1]
		x[2] += y[2]
		x[3] += y[3]
		x[4] += y[4]
		x[5] += y[5]
		x[6] += y[6]
		x[7] += y[7]
		x[8] += y[8]
		x[9] += y[9]
		x[10] += y[10]
		x[11] += y[11]
		x[12] += y[12]
		x[13] += y[13]
		x[14] += y[14]
		x[15] += y[15]

		x[0] += y[0]
		x[1] += y[1]
		x[2] += y[2]
		x[3] += y[3]
		x[4] += y[4]
		x[5] += y[5]
		x[6] += y[6]
		x[7] += y[7]
		x[8] += y[8]
		x[9] += y[9]
		x[10] += y[10]
		x[11] += y[11]
		x[12] += y[12]
		x[13] += y[13]
		x[14] += y[14]
		x[15] += y[15]
	}
}

func BenchmarkAddAVX(b *testing.B) {
	x := x
	y := y

	for i := 0; i < b.N; i++ {
		Add(&x, &y, &x)
		Add(&x, &y, &x)
		Add(&x, &y, &x)
		Add(&x, &y, &x)
		Add(&x, &y, &x)
		Add(&x, &y, &x)
		Add(&x, &y, &x)
		Add(&x, &y, &x)
		Add(&x, &y, &x)
		Add(&x, &y, &x)
	}
}

func BenchmarkAddAVX10(b *testing.B) {
	x := x
	y := y

	for i := 0; i < b.N; i++ {
		Add10(&x, &y, &x)
	}
}

func BenchmarkXorGo(b *testing.B) {
	x := x
	y := y

	for i := 0; i < b.N; i++ {
		x[0] ^= y[0]
		x[1] ^= y[1]
		x[2] ^= y[2]
		x[3] ^= y[3]
		x[4] ^= y[4]
		x[5] ^= y[5]
		x[6] ^= y[6]
		x[7] ^= y[7]
		x[8] ^= y[8]
		x[9] ^= y[9]
		x[10] ^= y[10]
		x[11] ^= y[11]
		x[12] ^= y[12]
		x[13] ^= y[13]
		x[14] ^= y[14]
		x[15] ^= y[15]

		x[0] ^= y[0]
		x[1] ^= y[1]
		x[2] ^= y[2]
		x[3] ^= y[3]
		x[4] ^= y[4]
		x[5] ^= y[5]
		x[6] ^= y[6]
		x[7] ^= y[7]
		x[8] ^= y[8]
		x[9] ^= y[9]
		x[10] ^= y[10]
		x[11] ^= y[11]
		x[12] ^= y[12]
		x[13] ^= y[13]
		x[14] ^= y[14]
		x[15] ^= y[15]

		x[0] ^= y[0]
		x[1] ^= y[1]
		x[2] ^= y[2]
		x[3] ^= y[3]
		x[4] ^= y[4]
		x[5] ^= y[5]
		x[6] ^= y[6]
		x[7] ^= y[7]
		x[8] ^= y[8]
		x[9] ^= y[9]
		x[10] ^= y[10]
		x[11] ^= y[11]
		x[12] ^= y[12]
		x[13] ^= y[13]
		x[14] ^= y[14]
		x[15] ^= y[15]

		x[0] ^= y[0]
		x[1] ^= y[1]
		x[2] ^= y[2]
		x[3] ^= y[3]
		x[4] ^= y[4]
		x[5] ^= y[5]
		x[6] ^= y[6]
		x[7] ^= y[7]
		x[8] ^= y[8]
		x[9] ^= y[9]
		x[10] ^= y[10]
		x[11] ^= y[11]
		x[12] ^= y[12]
		x[13] ^= y[13]
		x[14] ^= y[14]
		x[15] ^= y[15]

		x[0] ^= y[0]
		x[1] ^= y[1]
		x[2] ^= y[2]
		x[3] ^= y[3]
		x[4] ^= y[4]
		x[5] ^= y[5]
		x[6] ^= y[6]
		x[7] ^= y[7]
		x[8] ^= y[8]
		x[9] ^= y[9]
		x[10] ^= y[10]
		x[11] ^= y[11]
		x[12] ^= y[12]
		x[13] ^= y[13]
		x[14] ^= y[14]
		x[15] ^= y[15]

		x[0] ^= y[0]
		x[1] ^= y[1]
		x[2] ^= y[2]
		x[3] ^= y[3]
		x[4] ^= y[4]
		x[5] ^= y[5]
		x[6] ^= y[6]
		x[7] ^= y[7]
		x[8] ^= y[8]
		x[9] ^= y[9]
		x[10] ^= y[10]
		x[11] ^= y[11]
		x[12] ^= y[12]
		x[13] ^= y[13]
		x[14] ^= y[14]
		x[15] ^= y[15]

		x[0] ^= y[0]
		x[1] ^= y[1]
		x[2] ^= y[2]
		x[3] ^= y[3]
		x[4] ^= y[4]
		x[5] ^= y[5]
		x[6] ^= y[6]
		x[7] ^= y[7]
		x[8] ^= y[8]
		x[9] ^= y[9]
		x[10] ^= y[10]
		x[11] ^= y[11]
		x[12] ^= y[12]
		x[13] ^= y[13]
		x[14] ^= y[14]
		x[15] ^= y[15]

		x[0] ^= y[0]
		x[1] ^= y[1]
		x[2] ^= y[2]
		x[3] ^= y[3]
		x[4] ^= y[4]
		x[5] ^= y[5]
		x[6] ^= y[6]
		x[7] ^= y[7]
		x[8] ^= y[8]
		x[9] ^= y[9]
		x[10] ^= y[10]
		x[11] ^= y[11]
		x[12] ^= y[12]
		x[13] ^= y[13]
		x[14] ^= y[14]
		x[15] ^= y[15]

		x[0] ^= y[0]
		x[1] ^= y[1]
		x[2] ^= y[2]
		x[3] ^= y[3]
		x[4] ^= y[4]
		x[5] ^= y[5]
		x[6] ^= y[6]
		x[7] ^= y[7]
		x[8] ^= y[8]
		x[9] ^= y[9]
		x[10] ^= y[10]
		x[11] ^= y[11]
		x[12] ^= y[12]
		x[13] ^= y[13]
		x[14] ^= y[14]
		x[15] ^= y[15]

		x[0] ^= y[0]
		x[1] ^= y[1]
		x[2] ^= y[2]
		x[3] ^= y[3]
		x[4] ^= y[4]
		x[5] ^= y[5]
		x[6] ^= y[6]
		x[7] ^= y[7]
		x[8] ^= y[8]
		x[9] ^= y[9]
		x[10] ^= y[10]
		x[11] ^= y[11]
		x[12] ^= y[12]
		x[13] ^= y[13]
		x[14] ^= y[14]
		x[15] ^= y[15]
	}
}

func BenchmarkXorAVX(b *testing.B) {
	x := x
	y := y

	for i := 0; i < b.N; i++ {
		Xor(&x, &y, &x)
		Xor(&x, &y, &x)
		Xor(&x, &y, &x)
		Xor(&x, &y, &x)
		Xor(&x, &y, &x)
		Xor(&x, &y, &x)
		Xor(&x, &y, &x)
		Xor(&x, &y, &x)
		Xor(&x, &y, &x)
		Xor(&x, &y, &x)
	}
}

func BenchmarkXorAVX10(b *testing.B) {
	x := x
	y := y

	for i := 0; i < b.N; i++ {
		Xor10(&x, &y, &x)
	}
}

func BenchmarkRotateRight7Go(b *testing.B) {
	const numOfBits = 7

	x := x

	for i := 0; i < b.N; i++ {
		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)
	}
}

func BenchmarkRotateRight7AVX(b *testing.B) {
	x := x

	for i := 0; i < b.N; i++ {
		RotateRight7(&x, &x)
		RotateRight7(&x, &x)
		RotateRight7(&x, &x)
		RotateRight7(&x, &x)
		RotateRight7(&x, &x)
		RotateRight7(&x, &x)
		RotateRight7(&x, &x)
		RotateRight7(&x, &x)
		RotateRight7(&x, &x)
		RotateRight7(&x, &x)
	}
}

func BenchmarkRotateRight7AVX10(b *testing.B) {
	x := x

	for i := 0; i < b.N; i++ {
		RotateRight107(&x, &x)
	}
}

func BenchmarkRotateRight8Go(b *testing.B) {
	const numOfBits = 8

	x := x

	for i := 0; i < b.N; i++ {
		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)
	}
}

func BenchmarkRotateRight8AVX(b *testing.B) {
	x := x

	for i := 0; i < b.N; i++ {
		RotateRight8(&x, &x)
		RotateRight8(&x, &x)
		RotateRight8(&x, &x)
		RotateRight8(&x, &x)
		RotateRight8(&x, &x)
		RotateRight8(&x, &x)
		RotateRight8(&x, &x)
		RotateRight8(&x, &x)
		RotateRight8(&x, &x)
		RotateRight8(&x, &x)
	}
}

func BenchmarkRotateRight8AVX10(b *testing.B) {
	x := x

	for i := 0; i < b.N; i++ {
		RotateRight108(&x, &x)
	}
}

func BenchmarkRotateRight12Go(b *testing.B) {
	const numOfBits = 12

	x := x

	for i := 0; i < b.N; i++ {
		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)
	}
}

func BenchmarkRotateRight12AVX(b *testing.B) {
	x := x

	for i := 0; i < b.N; i++ {
		RotateRight12(&x, &x)
		RotateRight12(&x, &x)
		RotateRight12(&x, &x)
		RotateRight12(&x, &x)
		RotateRight12(&x, &x)
		RotateRight12(&x, &x)
		RotateRight12(&x, &x)
		RotateRight12(&x, &x)
		RotateRight12(&x, &x)
		RotateRight12(&x, &x)
	}
}

func BenchmarkRotateRight12AVX10(b *testing.B) {
	x := x

	for i := 0; i < b.N; i++ {
		RotateRight1012(&x, &x)
	}
}

func BenchmarkRotateRight16Go(b *testing.B) {
	const numOfBits = 16

	x := x

	for i := 0; i < b.N; i++ {
		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)

		x[0] = bits.RotateLeft32(x[0], numOfBits)
		x[1] = bits.RotateLeft32(x[1], numOfBits)
		x[2] = bits.RotateLeft32(x[2], numOfBits)
		x[3] = bits.RotateLeft32(x[3], numOfBits)
		x[4] = bits.RotateLeft32(x[4], numOfBits)
		x[5] = bits.RotateLeft32(x[5], numOfBits)
		x[6] = bits.RotateLeft32(x[6], numOfBits)
		x[7] = bits.RotateLeft32(x[7], numOfBits)
		x[8] = bits.RotateLeft32(x[8], numOfBits)
		x[9] = bits.RotateLeft32(x[9], numOfBits)
		x[10] = bits.RotateLeft32(x[10], numOfBits)
		x[11] = bits.RotateLeft32(x[11], numOfBits)
		x[12] = bits.RotateLeft32(x[12], numOfBits)
		x[13] = bits.RotateLeft32(x[13], numOfBits)
		x[14] = bits.RotateLeft32(x[14], numOfBits)
		x[15] = bits.RotateLeft32(x[15], numOfBits)
	}
}

func BenchmarkRotateRight16AVX(b *testing.B) {
	x := x

	for i := 0; i < b.N; i++ {
		RotateRight16(&x, &x)
		RotateRight16(&x, &x)
		RotateRight16(&x, &x)
		RotateRight16(&x, &x)
		RotateRight16(&x, &x)
		RotateRight16(&x, &x)
		RotateRight16(&x, &x)
		RotateRight16(&x, &x)
		RotateRight16(&x, &x)
		RotateRight16(&x, &x)
	}
}

func BenchmarkRotateRight16AVX10(b *testing.B) {
	x := x

	for i := 0; i < b.N; i++ {
		RotateRight1016(&x, &x)
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

func randUint32Array(arr *[16]uint32) {
	_, err := rand.Read(unsafe.Slice((*byte)(unsafe.Pointer(&arr[0])), int(unsafe.Sizeof(arr[0]))*len(arr)))
	if err != nil {
		panic(err)
	}
}
