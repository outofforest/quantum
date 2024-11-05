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

	chP := [256]*byte{
		(*byte)(unsafe.Pointer(&chunks[0][0])),
		(*byte)(unsafe.Pointer(&chunks[0][16])),
		(*byte)(unsafe.Pointer(&chunks[0][2*16])),
		(*byte)(unsafe.Pointer(&chunks[0][3*16])),
		(*byte)(unsafe.Pointer(&chunks[0][4*16])),
		(*byte)(unsafe.Pointer(&chunks[0][5*16])),
		(*byte)(unsafe.Pointer(&chunks[0][6*16])),
		(*byte)(unsafe.Pointer(&chunks[0][7*16])),
		(*byte)(unsafe.Pointer(&chunks[0][8*16])),
		(*byte)(unsafe.Pointer(&chunks[0][9*16])),
		(*byte)(unsafe.Pointer(&chunks[0][10*16])),
		(*byte)(unsafe.Pointer(&chunks[0][11*16])),
		(*byte)(unsafe.Pointer(&chunks[0][12*16])),
		(*byte)(unsafe.Pointer(&chunks[0][13*16])),
		(*byte)(unsafe.Pointer(&chunks[0][14*16])),
		(*byte)(unsafe.Pointer(&chunks[0][15*16])),

		(*byte)(unsafe.Pointer(&chunks[1][0])),
		(*byte)(unsafe.Pointer(&chunks[1][16])),
		(*byte)(unsafe.Pointer(&chunks[1][2*16])),
		(*byte)(unsafe.Pointer(&chunks[1][3*16])),
		(*byte)(unsafe.Pointer(&chunks[1][4*16])),
		(*byte)(unsafe.Pointer(&chunks[1][5*16])),
		(*byte)(unsafe.Pointer(&chunks[1][6*16])),
		(*byte)(unsafe.Pointer(&chunks[1][7*16])),
		(*byte)(unsafe.Pointer(&chunks[1][8*16])),
		(*byte)(unsafe.Pointer(&chunks[1][9*16])),
		(*byte)(unsafe.Pointer(&chunks[1][10*16])),
		(*byte)(unsafe.Pointer(&chunks[1][11*16])),
		(*byte)(unsafe.Pointer(&chunks[1][12*16])),
		(*byte)(unsafe.Pointer(&chunks[1][13*16])),
		(*byte)(unsafe.Pointer(&chunks[1][14*16])),
		(*byte)(unsafe.Pointer(&chunks[1][15*16])),

		(*byte)(unsafe.Pointer(&chunks[2][0])),
		(*byte)(unsafe.Pointer(&chunks[2][16])),
		(*byte)(unsafe.Pointer(&chunks[2][2*16])),
		(*byte)(unsafe.Pointer(&chunks[2][3*16])),
		(*byte)(unsafe.Pointer(&chunks[2][4*16])),
		(*byte)(unsafe.Pointer(&chunks[2][5*16])),
		(*byte)(unsafe.Pointer(&chunks[2][6*16])),
		(*byte)(unsafe.Pointer(&chunks[2][7*16])),
		(*byte)(unsafe.Pointer(&chunks[2][8*16])),
		(*byte)(unsafe.Pointer(&chunks[2][9*16])),
		(*byte)(unsafe.Pointer(&chunks[2][10*16])),
		(*byte)(unsafe.Pointer(&chunks[2][11*16])),
		(*byte)(unsafe.Pointer(&chunks[2][12*16])),
		(*byte)(unsafe.Pointer(&chunks[2][13*16])),
		(*byte)(unsafe.Pointer(&chunks[2][14*16])),
		(*byte)(unsafe.Pointer(&chunks[2][15*16])),

		(*byte)(unsafe.Pointer(&chunks[3][0])),
		(*byte)(unsafe.Pointer(&chunks[3][16])),
		(*byte)(unsafe.Pointer(&chunks[3][2*16])),
		(*byte)(unsafe.Pointer(&chunks[3][3*16])),
		(*byte)(unsafe.Pointer(&chunks[3][4*16])),
		(*byte)(unsafe.Pointer(&chunks[3][5*16])),
		(*byte)(unsafe.Pointer(&chunks[3][6*16])),
		(*byte)(unsafe.Pointer(&chunks[3][7*16])),
		(*byte)(unsafe.Pointer(&chunks[3][8*16])),
		(*byte)(unsafe.Pointer(&chunks[3][9*16])),
		(*byte)(unsafe.Pointer(&chunks[3][10*16])),
		(*byte)(unsafe.Pointer(&chunks[3][11*16])),
		(*byte)(unsafe.Pointer(&chunks[3][12*16])),
		(*byte)(unsafe.Pointer(&chunks[3][13*16])),
		(*byte)(unsafe.Pointer(&chunks[3][14*16])),
		(*byte)(unsafe.Pointer(&chunks[3][15*16])),

		(*byte)(unsafe.Pointer(&chunks[4][0])),
		(*byte)(unsafe.Pointer(&chunks[4][16])),
		(*byte)(unsafe.Pointer(&chunks[4][2*16])),
		(*byte)(unsafe.Pointer(&chunks[4][3*16])),
		(*byte)(unsafe.Pointer(&chunks[4][4*16])),
		(*byte)(unsafe.Pointer(&chunks[4][5*16])),
		(*byte)(unsafe.Pointer(&chunks[4][6*16])),
		(*byte)(unsafe.Pointer(&chunks[4][7*16])),
		(*byte)(unsafe.Pointer(&chunks[4][8*16])),
		(*byte)(unsafe.Pointer(&chunks[4][9*16])),
		(*byte)(unsafe.Pointer(&chunks[4][10*16])),
		(*byte)(unsafe.Pointer(&chunks[4][11*16])),
		(*byte)(unsafe.Pointer(&chunks[4][12*16])),
		(*byte)(unsafe.Pointer(&chunks[4][13*16])),
		(*byte)(unsafe.Pointer(&chunks[4][14*16])),
		(*byte)(unsafe.Pointer(&chunks[4][15*16])),

		(*byte)(unsafe.Pointer(&chunks[5][0])),
		(*byte)(unsafe.Pointer(&chunks[5][16])),
		(*byte)(unsafe.Pointer(&chunks[5][2*16])),
		(*byte)(unsafe.Pointer(&chunks[5][3*16])),
		(*byte)(unsafe.Pointer(&chunks[5][4*16])),
		(*byte)(unsafe.Pointer(&chunks[5][5*16])),
		(*byte)(unsafe.Pointer(&chunks[5][6*16])),
		(*byte)(unsafe.Pointer(&chunks[5][7*16])),
		(*byte)(unsafe.Pointer(&chunks[5][8*16])),
		(*byte)(unsafe.Pointer(&chunks[5][9*16])),
		(*byte)(unsafe.Pointer(&chunks[5][10*16])),
		(*byte)(unsafe.Pointer(&chunks[5][11*16])),
		(*byte)(unsafe.Pointer(&chunks[5][12*16])),
		(*byte)(unsafe.Pointer(&chunks[5][13*16])),
		(*byte)(unsafe.Pointer(&chunks[5][14*16])),
		(*byte)(unsafe.Pointer(&chunks[5][15*16])),

		(*byte)(unsafe.Pointer(&chunks[6][0])),
		(*byte)(unsafe.Pointer(&chunks[6][16])),
		(*byte)(unsafe.Pointer(&chunks[6][2*16])),
		(*byte)(unsafe.Pointer(&chunks[6][3*16])),
		(*byte)(unsafe.Pointer(&chunks[6][4*16])),
		(*byte)(unsafe.Pointer(&chunks[6][5*16])),
		(*byte)(unsafe.Pointer(&chunks[6][6*16])),
		(*byte)(unsafe.Pointer(&chunks[6][7*16])),
		(*byte)(unsafe.Pointer(&chunks[6][8*16])),
		(*byte)(unsafe.Pointer(&chunks[6][9*16])),
		(*byte)(unsafe.Pointer(&chunks[6][10*16])),
		(*byte)(unsafe.Pointer(&chunks[6][11*16])),
		(*byte)(unsafe.Pointer(&chunks[6][12*16])),
		(*byte)(unsafe.Pointer(&chunks[6][13*16])),
		(*byte)(unsafe.Pointer(&chunks[6][14*16])),
		(*byte)(unsafe.Pointer(&chunks[6][15*16])),

		(*byte)(unsafe.Pointer(&chunks[7][0])),
		(*byte)(unsafe.Pointer(&chunks[7][16])),
		(*byte)(unsafe.Pointer(&chunks[7][2*16])),
		(*byte)(unsafe.Pointer(&chunks[7][3*16])),
		(*byte)(unsafe.Pointer(&chunks[7][4*16])),
		(*byte)(unsafe.Pointer(&chunks[7][5*16])),
		(*byte)(unsafe.Pointer(&chunks[7][6*16])),
		(*byte)(unsafe.Pointer(&chunks[7][7*16])),
		(*byte)(unsafe.Pointer(&chunks[7][8*16])),
		(*byte)(unsafe.Pointer(&chunks[7][9*16])),
		(*byte)(unsafe.Pointer(&chunks[7][10*16])),
		(*byte)(unsafe.Pointer(&chunks[7][11*16])),
		(*byte)(unsafe.Pointer(&chunks[7][12*16])),
		(*byte)(unsafe.Pointer(&chunks[7][13*16])),
		(*byte)(unsafe.Pointer(&chunks[7][14*16])),
		(*byte)(unsafe.Pointer(&chunks[7][15*16])),

		(*byte)(unsafe.Pointer(&chunks[8][0])),
		(*byte)(unsafe.Pointer(&chunks[8][16])),
		(*byte)(unsafe.Pointer(&chunks[8][2*16])),
		(*byte)(unsafe.Pointer(&chunks[8][3*16])),
		(*byte)(unsafe.Pointer(&chunks[8][4*16])),
		(*byte)(unsafe.Pointer(&chunks[8][5*16])),
		(*byte)(unsafe.Pointer(&chunks[8][6*16])),
		(*byte)(unsafe.Pointer(&chunks[8][7*16])),
		(*byte)(unsafe.Pointer(&chunks[8][8*16])),
		(*byte)(unsafe.Pointer(&chunks[8][9*16])),
		(*byte)(unsafe.Pointer(&chunks[8][10*16])),
		(*byte)(unsafe.Pointer(&chunks[8][11*16])),
		(*byte)(unsafe.Pointer(&chunks[8][12*16])),
		(*byte)(unsafe.Pointer(&chunks[8][13*16])),
		(*byte)(unsafe.Pointer(&chunks[8][14*16])),
		(*byte)(unsafe.Pointer(&chunks[8][15*16])),

		(*byte)(unsafe.Pointer(&chunks[9][0])),
		(*byte)(unsafe.Pointer(&chunks[9][16])),
		(*byte)(unsafe.Pointer(&chunks[9][2*16])),
		(*byte)(unsafe.Pointer(&chunks[9][3*16])),
		(*byte)(unsafe.Pointer(&chunks[9][4*16])),
		(*byte)(unsafe.Pointer(&chunks[9][5*16])),
		(*byte)(unsafe.Pointer(&chunks[9][6*16])),
		(*byte)(unsafe.Pointer(&chunks[9][7*16])),
		(*byte)(unsafe.Pointer(&chunks[9][8*16])),
		(*byte)(unsafe.Pointer(&chunks[9][9*16])),
		(*byte)(unsafe.Pointer(&chunks[9][10*16])),
		(*byte)(unsafe.Pointer(&chunks[9][11*16])),
		(*byte)(unsafe.Pointer(&chunks[9][12*16])),
		(*byte)(unsafe.Pointer(&chunks[9][13*16])),
		(*byte)(unsafe.Pointer(&chunks[9][14*16])),
		(*byte)(unsafe.Pointer(&chunks[9][15*16])),

		(*byte)(unsafe.Pointer(&chunks[10][0])),
		(*byte)(unsafe.Pointer(&chunks[10][16])),
		(*byte)(unsafe.Pointer(&chunks[10][2*16])),
		(*byte)(unsafe.Pointer(&chunks[10][3*16])),
		(*byte)(unsafe.Pointer(&chunks[10][4*16])),
		(*byte)(unsafe.Pointer(&chunks[10][5*16])),
		(*byte)(unsafe.Pointer(&chunks[10][6*16])),
		(*byte)(unsafe.Pointer(&chunks[10][7*16])),
		(*byte)(unsafe.Pointer(&chunks[10][8*16])),
		(*byte)(unsafe.Pointer(&chunks[10][9*16])),
		(*byte)(unsafe.Pointer(&chunks[10][10*16])),
		(*byte)(unsafe.Pointer(&chunks[10][11*16])),
		(*byte)(unsafe.Pointer(&chunks[10][12*16])),
		(*byte)(unsafe.Pointer(&chunks[10][13*16])),
		(*byte)(unsafe.Pointer(&chunks[10][14*16])),
		(*byte)(unsafe.Pointer(&chunks[10][15*16])),

		(*byte)(unsafe.Pointer(&chunks[11][0])),
		(*byte)(unsafe.Pointer(&chunks[11][16])),
		(*byte)(unsafe.Pointer(&chunks[11][2*16])),
		(*byte)(unsafe.Pointer(&chunks[11][3*16])),
		(*byte)(unsafe.Pointer(&chunks[11][4*16])),
		(*byte)(unsafe.Pointer(&chunks[11][5*16])),
		(*byte)(unsafe.Pointer(&chunks[11][6*16])),
		(*byte)(unsafe.Pointer(&chunks[11][7*16])),
		(*byte)(unsafe.Pointer(&chunks[11][8*16])),
		(*byte)(unsafe.Pointer(&chunks[11][9*16])),
		(*byte)(unsafe.Pointer(&chunks[11][10*16])),
		(*byte)(unsafe.Pointer(&chunks[11][11*16])),
		(*byte)(unsafe.Pointer(&chunks[11][12*16])),
		(*byte)(unsafe.Pointer(&chunks[11][13*16])),
		(*byte)(unsafe.Pointer(&chunks[11][14*16])),
		(*byte)(unsafe.Pointer(&chunks[11][15*16])),

		(*byte)(unsafe.Pointer(&chunks[12][0])),
		(*byte)(unsafe.Pointer(&chunks[12][16])),
		(*byte)(unsafe.Pointer(&chunks[12][2*16])),
		(*byte)(unsafe.Pointer(&chunks[12][3*16])),
		(*byte)(unsafe.Pointer(&chunks[12][4*16])),
		(*byte)(unsafe.Pointer(&chunks[12][5*16])),
		(*byte)(unsafe.Pointer(&chunks[12][6*16])),
		(*byte)(unsafe.Pointer(&chunks[12][7*16])),
		(*byte)(unsafe.Pointer(&chunks[12][8*16])),
		(*byte)(unsafe.Pointer(&chunks[12][9*16])),
		(*byte)(unsafe.Pointer(&chunks[12][10*16])),
		(*byte)(unsafe.Pointer(&chunks[12][11*16])),
		(*byte)(unsafe.Pointer(&chunks[12][12*16])),
		(*byte)(unsafe.Pointer(&chunks[12][13*16])),
		(*byte)(unsafe.Pointer(&chunks[12][14*16])),
		(*byte)(unsafe.Pointer(&chunks[12][15*16])),

		(*byte)(unsafe.Pointer(&chunks[13][0])),
		(*byte)(unsafe.Pointer(&chunks[13][16])),
		(*byte)(unsafe.Pointer(&chunks[13][2*16])),
		(*byte)(unsafe.Pointer(&chunks[13][3*16])),
		(*byte)(unsafe.Pointer(&chunks[13][4*16])),
		(*byte)(unsafe.Pointer(&chunks[13][5*16])),
		(*byte)(unsafe.Pointer(&chunks[13][6*16])),
		(*byte)(unsafe.Pointer(&chunks[13][7*16])),
		(*byte)(unsafe.Pointer(&chunks[13][8*16])),
		(*byte)(unsafe.Pointer(&chunks[13][9*16])),
		(*byte)(unsafe.Pointer(&chunks[13][10*16])),
		(*byte)(unsafe.Pointer(&chunks[13][11*16])),
		(*byte)(unsafe.Pointer(&chunks[13][12*16])),
		(*byte)(unsafe.Pointer(&chunks[13][13*16])),
		(*byte)(unsafe.Pointer(&chunks[13][14*16])),
		(*byte)(unsafe.Pointer(&chunks[13][15*16])),

		(*byte)(unsafe.Pointer(&chunks[14][0])),
		(*byte)(unsafe.Pointer(&chunks[14][16])),
		(*byte)(unsafe.Pointer(&chunks[14][2*16])),
		(*byte)(unsafe.Pointer(&chunks[14][3*16])),
		(*byte)(unsafe.Pointer(&chunks[14][4*16])),
		(*byte)(unsafe.Pointer(&chunks[14][5*16])),
		(*byte)(unsafe.Pointer(&chunks[14][6*16])),
		(*byte)(unsafe.Pointer(&chunks[14][7*16])),
		(*byte)(unsafe.Pointer(&chunks[14][8*16])),
		(*byte)(unsafe.Pointer(&chunks[14][9*16])),
		(*byte)(unsafe.Pointer(&chunks[14][10*16])),
		(*byte)(unsafe.Pointer(&chunks[14][11*16])),
		(*byte)(unsafe.Pointer(&chunks[14][12*16])),
		(*byte)(unsafe.Pointer(&chunks[14][13*16])),
		(*byte)(unsafe.Pointer(&chunks[14][14*16])),
		(*byte)(unsafe.Pointer(&chunks[14][15*16])),

		(*byte)(unsafe.Pointer(&chunks[15][0])),
		(*byte)(unsafe.Pointer(&chunks[15][16])),
		(*byte)(unsafe.Pointer(&chunks[15][2*16])),
		(*byte)(unsafe.Pointer(&chunks[15][3*16])),
		(*byte)(unsafe.Pointer(&chunks[15][4*16])),
		(*byte)(unsafe.Pointer(&chunks[15][5*16])),
		(*byte)(unsafe.Pointer(&chunks[15][6*16])),
		(*byte)(unsafe.Pointer(&chunks[15][7*16])),
		(*byte)(unsafe.Pointer(&chunks[15][8*16])),
		(*byte)(unsafe.Pointer(&chunks[15][9*16])),
		(*byte)(unsafe.Pointer(&chunks[15][10*16])),
		(*byte)(unsafe.Pointer(&chunks[15][11*16])),
		(*byte)(unsafe.Pointer(&chunks[15][12*16])),
		(*byte)(unsafe.Pointer(&chunks[15][13*16])),
		(*byte)(unsafe.Pointer(&chunks[15][14*16])),
		(*byte)(unsafe.Pointer(&chunks[15][15*16])),
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
