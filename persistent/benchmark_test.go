package persistent_test

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zeebo/blake3"
	blake3l "lukechampine.com/blake3"
)

// go test -benchtime=1x -bench=. -run=^$ -cpuprofile profile.out
// go tool pprof -http="localhost:8000" pprofbin ./profile.out

func BenchmarkChecksumFull(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	node := make([]byte, 4096)
	_, err := rand.Read(node)
	require.NoError(b, err)

	b.StartTimer()
	for range b.N {
		// 3 pointer nodes + data node
		for range 4 {
			s := blake3.Sum256(node)
			copy(node, s[:])
		}
	}
	b.StopTimer()
}

func BenchmarkChecksumPartial(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	node := make([]byte, 4096)
	_, err := rand.Read(node)
	require.NoError(b, err)

	checksums := make([]byte, 128)
	_, err = rand.Read(checksums)
	require.NoError(b, err)

	b.StartTimer()
	for range b.N {
		// 3 pointer nodes + data node
		for range 4 {
			//s := sha3.Sum256(node[:1024])
			blake3.Sum256(node[:1024])
			//copy(checksums, s[:])
			//s = blake3.Sum256(checksums)
			//copy(node, s[:])
		}
	}
	b.StopTimer()
}

func BenchmarkChecksumPartialL(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	node := make([]byte, 4096)
	_, err := rand.Read(node)
	require.NoError(b, err)

	checksums := make([]byte, 128)
	_, err = rand.Read(checksums)
	require.NoError(b, err)

	b.StartTimer()
	for range b.N {
		// 3 pointer nodes + data node
		for range 4 {
			//s := sha3.Sum256(node[:1024])
			blake3l.Sum256(node[:1024])
			//copy(checksums, s[:])
			//s = blake3.Sum256(checksums)
			//copy(node, s[:])
		}
	}
	b.StopTimer()
}
