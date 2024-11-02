package checksum_test

import (
	"crypto/rand"
	"testing"

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
	data1K = randData(1024)
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
		blake3zeebo.Sum256(data1K)
		blake3zeebo.Sum256(data1K)
		blake3zeebo.Sum256(data1K)
		blake3zeebo.Sum256(data1K)
		blake3zeebo.Sum256(data1K)
		blake3zeebo.Sum256(data1K)
		blake3zeebo.Sum256(data1K)
		blake3zeebo.Sum256(data1K)
		blake3zeebo.Sum256(data1K)
		blake3zeebo.Sum256(data1K)
	}
}

func BenchmarkChecksum1KLuke(b *testing.B) {
	for range b.N {
		blake3luke.Sum256(data1K)
		blake3luke.Sum256(data1K)
		blake3luke.Sum256(data1K)
		blake3luke.Sum256(data1K)
		blake3luke.Sum256(data1K)
		blake3luke.Sum256(data1K)
		blake3luke.Sum256(data1K)
		blake3luke.Sum256(data1K)
		blake3luke.Sum256(data1K)
		blake3luke.Sum256(data1K)
	}
}
