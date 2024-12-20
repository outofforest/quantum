package state

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/outofforest/quantum/types"
)

func TestWriter(t *testing.T) {
	const stateSize = 1000 * types.NodeLength

	requireT := require.New(t)

	s := NewForTest(t, stateSize)
	volatileAllocator := s.NewVolatileAllocator()
	persistentAllocator := s.NewPersistentAllocator()
	writer, err := s.NewPersistentWriter()
	requireT.NoError(err)
	t.Cleanup(writer.Close)

	persistentAddresses := make([]types.PersistentAddress, 0, stateSize/types.NodeLength-2)
	for i := range cap(persistentAddresses) {
		volatileAddress, err := volatileAllocator.Allocate()
		requireT.NoError(err)
		persistentAddress, err := persistentAllocator.Allocate()
		requireT.NoError(err)

		persistentAddresses = append(persistentAddresses, persistentAddress)
		b := s.Bytes(volatileAddress)
		for j := range b {
			b[j] = byte(i)
		}

		requireT.NoError(writer.Write(persistentAddress, volatileAddress))
	}
	for i := range types.SnapshotID(numOfPersistentSingularityNodes) {
		singularityNode := s.SingularityNodeRoot(i)
		b := s.Bytes(singularityNode.VolatileAddress)
		for j := range b {
			b[j] = byte(100 + i)
		}

		requireT.NoError(writer.Write(singularityNode.Pointer.PersistentAddress, singularityNode.VolatileAddress))
	}

	f := os.NewFile(uintptr(s.persistentFile), "")
	buf := make([]byte, types.NodeLength)
	for i, pa := range persistentAddresses {
		_, err = f.Seek(int64(pa)*types.NodeLength, io.SeekStart)
		requireT.NoError(err)

		_, err = f.Read(buf)
		requireT.NoError(err)

		requireT.Equal(bytes.Repeat([]byte{byte(i)}, types.NodeLength), buf)
	}
	for i := range types.SnapshotID(numOfPersistentSingularityNodes) {
		singularityNode := s.SingularityNodeRoot(i)
		_, err = f.Seek(int64(singularityNode.Pointer.PersistentAddress)*types.NodeLength, io.SeekStart)
		requireT.NoError(err)

		_, err = f.Read(buf)
		requireT.NoError(err)

		requireT.Equal(bytes.Repeat([]byte{byte(100 + i)}, types.NodeLength), buf)
	}

	requireT.NoError(writer.Write(persistentAddresses[0], 2))
	writer.Close()

	_, err = f.Seek(int64(persistentAddresses[0])*types.NodeLength, io.SeekStart)
	requireT.NoError(err)

	_, err = f.Read(buf)
	requireT.NoError(err)

	requireT.Equal(bytes.Repeat([]byte{byte(1)}, types.NodeLength), buf)
}
