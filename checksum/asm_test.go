package checksum

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestASM(t *testing.T) {
	require.Equal(t, uint64(30), Add(10, 20))
}
