package checksum

import (
	"crypto/rand"
	"math/bits"
	"testing"
	"unsafe"
)

var x = [16]uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
var y = [16]uint32{17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}

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
