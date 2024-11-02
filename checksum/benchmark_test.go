package checksum

import (
	"math/bits"
	"testing"
)

var x = [16]uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
var y = [16]uint32{17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}

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
