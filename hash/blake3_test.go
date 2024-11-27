package hash

import (
	"crypto/rand"
	"fmt"
	"io"
	"math"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
	blake3zeebo "github.com/zeebo/blake3"
	blake3luke "lukechampine.com/blake3"

	"github.com/outofforest/quantum/alloc"
	"github.com/outofforest/quantum/types"
)

//nolint:unparam
func randData(size uint64) []byte {
	dataP, _, _ := alloc.Allocate(size, 64, false)
	data := unsafe.Slice((*byte)(dataP), size)
	if _, err := rand.Read(data); err != nil {
		panic(err)
	}
	return data
}

var data4K = func() [16][]byte {
	return [16][]byte{
		randData(4096), randData(4096), randData(4096), randData(4096),
		randData(4096), randData(4096), randData(4096), randData(4096),
		randData(4096), randData(4096), randData(4096), randData(4096),
		randData(4096), randData(4096), randData(4096), randData(4096),
	}
}()

func BenchmarkChecksum4KZeebo(b *testing.B) {
	for range b.N {
		blake3zeebo.Sum256(data4K[0])
		blake3zeebo.Sum256(data4K[1])
		blake3zeebo.Sum256(data4K[2])
		blake3zeebo.Sum256(data4K[3])
		blake3zeebo.Sum256(data4K[4])
		blake3zeebo.Sum256(data4K[5])
		blake3zeebo.Sum256(data4K[6])
		blake3zeebo.Sum256(data4K[7])
		blake3zeebo.Sum256(data4K[8])
		blake3zeebo.Sum256(data4K[9])
		blake3zeebo.Sum256(data4K[10])
		blake3zeebo.Sum256(data4K[11])
		blake3zeebo.Sum256(data4K[12])
		blake3zeebo.Sum256(data4K[13])
		blake3zeebo.Sum256(data4K[14])
		blake3zeebo.Sum256(data4K[15])
	}
}

func BenchmarkChecksum4KLuke(b *testing.B) {
	for range b.N {
		blake3luke.Sum256(data4K[0])
		blake3luke.Sum256(data4K[1])
		blake3luke.Sum256(data4K[2])
		blake3luke.Sum256(data4K[3])
		blake3luke.Sum256(data4K[4])
		blake3luke.Sum256(data4K[5])
		blake3luke.Sum256(data4K[6])
		blake3luke.Sum256(data4K[7])
		blake3luke.Sum256(data4K[8])
		blake3luke.Sum256(data4K[9])
		blake3luke.Sum256(data4K[10])
		blake3luke.Sum256(data4K[11])
		blake3luke.Sum256(data4K[12])
		blake3luke.Sum256(data4K[13])
		blake3luke.Sum256(data4K[14])
		blake3luke.Sum256(data4K[15])
	}
}

func BenchmarkChecksum4KAVX(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	var matrixCopy [16]*byte
	for j := range matrixCopy {
		rn, dealloc, err := alloc.Allocate(types.NodeLength, 64, false)
		require.NoError(b, err)
		b.Cleanup(dealloc)
		matrixCopy[j] = (*byte)(rn)
	}

	chP := make([]*byte, 0, 16)
	for i := range data4K {
		chP = append(chP, &data4K[i][0])
	}

	var z1, z2 [16]*byte
	for i := range z1 {
		z, dealloc, err := alloc.Allocate(types.HashLength, 32, false)
		require.NoError(b, err)
		b.Cleanup(dealloc)
		z1[i] = (*byte)(z)
	}
	for i := range z2 {
		z, dealloc, err := alloc.Allocate(types.HashLength, 32, false)
		require.NoError(b, err)
		b.Cleanup(dealloc)
		z2[i] = (*byte)(z)
	}

	b.StartTimer()
	for range b.N {
		Blake3AndCopy4096(&chP[0], (**byte)(unsafe.Pointer(&matrixCopy)), &z1[0], &z2[0], math.MaxUint32)
	}
	b.StopTimer()

	_, _ = fmt.Fprint(io.Discard, z1)
	_, _ = fmt.Fprint(io.Discard, z2)
}
