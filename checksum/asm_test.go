package checksum

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestASM(t *testing.T) {
	x := [16]uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	y := [16]uint32{17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
	var z [16]uint32

	Add(&x, &y, &z)

	expectedX := [16]uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	expectedY := [16]uint32{17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
	expectedZ := [16]uint32{18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 42, 44, 46, 48}

	assert.Equal(t, expectedX, x)
	assert.Equal(t, expectedY, y)
	assert.Equal(t, expectedZ, z)
}
