// Github actions run on machines not supporting AVX-512 instructions.
//go:build nogithub

package checksum

import (
	"math/bits"
	"testing"

	"github.com/stretchr/testify/assert"
)

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

func TestAdd(t *testing.T) {
	x := x
	y := y
	var z [16]uint32

	expectedX := x
	expectedY := y
	var expectedZ [16]uint32

	for i := range z {
		expectedZ[i] = expectedX[i] + expectedY[i]
	}

	Add(&x, &y, &z)

	assert.Equal(t, expectedX, x)
	assert.Equal(t, expectedY, y)
	assert.Equal(t, expectedZ, z)
}

func TestXor(t *testing.T) {
	x := x
	y := y
	var z [16]uint32

	expectedX := x
	expectedY := y
	var expectedZ [16]uint32

	for i := range z {
		expectedZ[i] = expectedX[i] ^ expectedY[i]
	}

	Xor(&x, &y, &z)

	assert.Equal(t, expectedX, x)
	assert.Equal(t, expectedY, y)
	assert.Equal(t, expectedZ, z)
}

func TestRotateRight7(t *testing.T) {
	x := x
	var z [16]uint32

	expectedX := x
	var expectedZ [16]uint32

	for i := range z {
		expectedZ[i] = bits.RotateLeft32(expectedX[i], -7)
	}

	RotateRight7(&x, &z)

	assert.Equal(t, expectedX, x)
	assert.Equal(t, expectedZ, z)
}

func TestRotateRight8(t *testing.T) {
	x := x
	var z [16]uint32

	expectedX := x
	var expectedZ [16]uint32

	for i := range z {
		expectedZ[i] = bits.RotateLeft32(expectedX[i], -8)
	}

	RotateRight8(&x, &z)

	assert.Equal(t, expectedX, x)
	assert.Equal(t, expectedZ, z)
}

func TestRotateRight12(t *testing.T) {
	x := x
	var z [16]uint32

	expectedX := x
	var expectedZ [16]uint32

	for i := range z {
		expectedZ[i] = bits.RotateLeft32(expectedX[i], -12)
	}

	RotateRight12(&x, &z)

	assert.Equal(t, expectedX, x)
	assert.Equal(t, expectedZ, z)
}

func TestRotateRight16(t *testing.T) {
	x := x
	var z [16]uint32

	expectedX := x
	var expectedZ [16]uint32

	for i := range z {
		expectedZ[i] = bits.RotateLeft32(expectedX[i], -16)
	}

	RotateRight16(&x, &z)

	assert.Equal(t, expectedX, x)
	assert.Equal(t, expectedZ, z)
}
