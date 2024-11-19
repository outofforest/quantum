package hash

import (
	"crypto/rand"
	"testing"

	blake3zeebo "github.com/zeebo/blake3"
	blake3luke "lukechampine.com/blake3"
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

	chP := make([]*byte, 0, 1024)
	for i := range data4K {
		for j := range 64 {
			chP = append(chP, &data4K[i][j*64])
		}
	}

	var z [16][32]byte
	zP := [16]*byte{
		&z[0][0], &z[1][0], &z[2][0], &z[3][0], &z[4][0], &z[5][0], &z[6][0], &z[7][0],
		&z[8][0], &z[9][0], &z[10][0], &z[11][0], &z[12][0], &z[13][0], &z[14][0], &z[15][0],
	}

	b.StartTimer()
	for range b.N {
		Blake3(&chP[0], &zP[0])
	}
	b.StopTimer()
}
