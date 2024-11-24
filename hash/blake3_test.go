package hash

import (
	"crypto/rand"
	"fmt"
	"io"
	"testing"
	"unsafe"

	blake3zeebo "github.com/zeebo/blake3"
	blake3luke "lukechampine.com/blake3"

	"github.com/outofforest/quantum/types"
)

//nolint:unparam
func randData(size uint) []byte {
	data := make([]byte, size)
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
		var rn [types.NodeLength]byte
		matrixCopy[j] = &rn[0]
	}

	chP := make([]*byte, 0, 16)
	for i := range data4K {
		chP = append(chP, &data4K[i][0])
	}

	var z1, z2 [16][32]byte
	zP1 := [16]*byte{
		&z1[0][0], &z1[1][0], &z1[2][0], &z1[3][0], &z1[4][0], &z1[5][0], &z1[6][0], &z1[7][0],
		&z1[8][0], &z1[9][0], &z1[10][0], &z1[11][0], &z1[12][0], &z1[13][0], &z1[14][0], &z1[15][0],
	}
	zP2 := [16]*byte{
		&z2[0][0], &z2[1][0], &z2[2][0], &z2[3][0], &z2[4][0], &z2[5][0], &z2[6][0], &z2[7][0],
		&z2[8][0], &z2[9][0], &z2[10][0], &z2[11][0], &z2[12][0], &z2[13][0], &z2[14][0], &z2[15][0],
	}

	b.StartTimer()
	for range b.N {
		Blake3AndCopy4096(&chP[0], (**byte)(unsafe.Pointer(&matrixCopy)), &zP1[0], &zP2[0])
	}
	b.StopTimer()

	_, _ = fmt.Fprint(io.Discard, z1)
	_, _ = fmt.Fprint(io.Discard, z2)
}
