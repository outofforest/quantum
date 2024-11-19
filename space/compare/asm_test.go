// Github actions run on machines not supporting AVX-512 instructions.
//go:build nogithub

package compare

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompare(t *testing.T) {
	values := values

	res := resDef
	zeroIndex := Compare(0, &values[0], &res[0])
	assert.Equal(t, [8]uint8{5, 6, m, m, m, m, m, m}, res)
	assert.Equal(t, uint8(5), zeroIndex)

	res = resDef
	Compare(1, &values[0], &res[0])
	assert.Equal(t, [8]uint8{1, 4, m, m, m, m, m, m}, res)

	res = resDef
	Compare(2, &values[0], &res[0])
	assert.Equal(t, [8]uint8{0, 2, m, m, m, m, m, m}, res)

	res = resDef
	Compare(3, &values[0], &res[0])
	assert.Equal(t, [8]uint8{3, m, m, m, m, m, m, m}, res)

	res = resDef
	Compare(4, &values[0], &res[0])
	assert.Equal(t, [8]uint8{7, m, m, m, m, m, m, m}, res)
}
