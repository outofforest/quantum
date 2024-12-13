package alloc

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// NewForTest creates state for unit tests.
func NewForTest(t *testing.T, size uint64) *State {
	state, stateDeallocFunc, err := NewState(size, size, false)
	require.NoError(t, err)
	t.Cleanup(stateDeallocFunc)

	return state
}
