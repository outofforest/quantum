package checksum

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestASM(t *testing.T) {
	x := [8]uint32{1, 2, 3, 4, 5, 6, 7, 8}
	y := [8]uint32{17, 18, 19, 20, 21, 22, 23, 24}
	var z [8]uint32

	Add(&x, &y, &z)

	expectedX := [8]uint32{1, 2, 3, 4, 5, 6, 7, 8}
	expectedY := [8]uint32{17, 18, 19, 20, 21, 22, 23, 24}
	expectedZ := [8]uint32{18, 20, 22, 24, 26, 28, 30, 32}

	assert.Equal(t, expectedX, x)
	assert.Equal(t, expectedY, y)
	assert.Equal(t, expectedZ, z)
}
