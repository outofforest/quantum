package checksum

import (
	"crypto/rand"
	"testing"
	"unsafe"

	blake3zeebo "github.com/zeebo/blake3"
	blake3luke "lukechampine.com/blake3"
)

func randData(size uint) []byte {
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		panic(err)
	}
	return data
}

var (
	data1K = func() [16][]byte {
		return [16][]byte{
			randData(1024), randData(1024), randData(1024), randData(1024),
			randData(1024), randData(1024), randData(1024), randData(1024),
			randData(1024), randData(1024), randData(1024), randData(1024),
			randData(1024), randData(1024), randData(1024), randData(1024),
		}
	}()
	data4K = randData(4096)
)

func BenchmarkChecksum4KZeebo(b *testing.B) {
	for range b.N {
		blake3zeebo.Sum256(data4K)
		blake3zeebo.Sum256(data4K)
		blake3zeebo.Sum256(data4K)
		blake3zeebo.Sum256(data4K)
		blake3zeebo.Sum256(data4K)
		blake3zeebo.Sum256(data4K)
		blake3zeebo.Sum256(data4K)
		blake3zeebo.Sum256(data4K)
		blake3zeebo.Sum256(data4K)
		blake3zeebo.Sum256(data4K)
	}
}

func BenchmarkChecksum4KLuke(b *testing.B) {
	for range b.N {
		blake3luke.Sum256(data4K)
		blake3luke.Sum256(data4K)
		blake3luke.Sum256(data4K)
		blake3luke.Sum256(data4K)
		blake3luke.Sum256(data4K)
		blake3luke.Sum256(data4K)
		blake3luke.Sum256(data4K)
		blake3luke.Sum256(data4K)
		blake3luke.Sum256(data4K)
		blake3luke.Sum256(data4K)
	}
}

func BenchmarkChecksum1KZeebo(b *testing.B) {
	for range b.N {
		blake3zeebo.Sum256(data1K[0])
		blake3zeebo.Sum256(data1K[1])
		blake3zeebo.Sum256(data1K[2])
		blake3zeebo.Sum256(data1K[3])
		blake3zeebo.Sum256(data1K[4])
		blake3zeebo.Sum256(data1K[5])
		blake3zeebo.Sum256(data1K[6])
		blake3zeebo.Sum256(data1K[7])
		blake3zeebo.Sum256(data1K[8])
		blake3zeebo.Sum256(data1K[9])
		blake3zeebo.Sum256(data1K[10])
		blake3zeebo.Sum256(data1K[11])
		blake3zeebo.Sum256(data1K[12])
		blake3zeebo.Sum256(data1K[13])
		blake3zeebo.Sum256(data1K[14])
		blake3zeebo.Sum256(data1K[15])
	}
}

func BenchmarkChecksum1KLuke(b *testing.B) {
	for range b.N {
		blake3luke.Sum256(data1K[0])
		blake3luke.Sum256(data1K[1])
		blake3luke.Sum256(data1K[2])
		blake3luke.Sum256(data1K[3])
		blake3luke.Sum256(data1K[4])
		blake3luke.Sum256(data1K[5])
		blake3luke.Sum256(data1K[6])
		blake3luke.Sum256(data1K[7])
		blake3luke.Sum256(data1K[8])
		blake3luke.Sum256(data1K[9])
		blake3luke.Sum256(data1K[10])
		blake3luke.Sum256(data1K[11])
		blake3luke.Sum256(data1K[12])
		blake3luke.Sum256(data1K[13])
		blake3luke.Sum256(data1K[14])
		blake3luke.Sum256(data1K[15])
	}
}

func BenchmarkChecksum1KAVX(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	chunks := chunks

	chP := [16]*byte{
		(*byte)(unsafe.Pointer(&chunks[0][0])),
		(*byte)(unsafe.Pointer(&chunks[1][0])),
		(*byte)(unsafe.Pointer(&chunks[2][0])),
		(*byte)(unsafe.Pointer(&chunks[3][0])),
		(*byte)(unsafe.Pointer(&chunks[4][0])),
		(*byte)(unsafe.Pointer(&chunks[5][0])),
		(*byte)(unsafe.Pointer(&chunks[6][0])),
		(*byte)(unsafe.Pointer(&chunks[7][0])),
		(*byte)(unsafe.Pointer(&chunks[8][0])),
		(*byte)(unsafe.Pointer(&chunks[9][0])),
		(*byte)(unsafe.Pointer(&chunks[10][0])),
		(*byte)(unsafe.Pointer(&chunks[11][0])),
		(*byte)(unsafe.Pointer(&chunks[12][0])),
		(*byte)(unsafe.Pointer(&chunks[13][0])),
		(*byte)(unsafe.Pointer(&chunks[14][0])),
		(*byte)(unsafe.Pointer(&chunks[15][0])),
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
