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
	zeroIndex, count := Compare(0, &values[0], &res[0], 22)
	assert.Equal(t, [32]uint64{
		5, 6, 13, 14, 21, m, m, m, m, m, m, m, m, m, m, m,
		m, m, m, m, m, m, m, m, m, m, m, m, m, m, m, m,
	}, res)
	assert.Equal(t, uint64(5), zeroIndex)
	assert.Equal(t, uint64(5), count)

	res = resDef
	_, count = Compare(1, &values[0], &res[0], 22)
	assert.Equal(t, [32]uint64{
		1, 4, 9, 12, 17, 20, m, m, m, m, m, m, m, m, m, m,
		m, m, m, m, m, m, m, m, m, m, m, m, m, m, m, m,
	}, res)
	assert.Equal(t, uint64(6), count)

	res = resDef
	_, count = Compare(2, &values[0], &res[0], 22)
	assert.Equal(t, [32]uint64{
		0, 2, 8, 10, 16, 18, m, m, m, m, m, m, m, m, m, m,
		m, m, m, m, m, m, m, m, m, m, m, m, m, m, m, m,
	}, res)
	assert.Equal(t, uint64(6), count)

	res = resDef
	_, count = Compare(3, &values[0], &res[0], 22)
	assert.Equal(t, [32]uint64{
		3, 11, 19, m, m, m, m, m, m, m, m, m, m, m, m, m,
		m, m, m, m, m, m, m, m, m, m, m, m, m, m, m, m,
	}, res)
	assert.Equal(t, uint64(3), count)

	res = resDef
	_, count = Compare(4, &values[0], &res[0], 22)
	assert.Equal(t, [32]uint64{
		7, 15, m, m, m, m, m, m, m, m, m, m, m, m, m, m,
		m, m, m, m, m, m, m, m, m, m, m, m, m, m, m, m,
	}, res)
	assert.Equal(t, uint64(2), count)
}
