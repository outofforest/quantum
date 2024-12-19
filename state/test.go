package state

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// NewForTest creates state for unit tests.
func NewForTest(t *testing.T, size uint64) *State {
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "quantum.db")
	f, err := os.OpenFile(dbFile, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	defer f.Close()

	_, err = f.Seek(int64(2*size)-1, io.SeekStart)
	require.NoError(t, err)

	_, err = f.Write([]byte{0x00})
	require.NoError(t, err)

	state, stateDeallocFunc, err := New(size, false, dbFile)
	require.NoError(t, err)
	t.Cleanup(stateDeallocFunc)

	return state
}
