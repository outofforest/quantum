package quantum

import (
	"crypto/rand"
	"testing"
)

// go test -benchtime=100x -bench=BenchmarkQuantum -run=^$ -cpuprofile profile.out
// go tool pprof -http="localhost:8000" pprofbin ./profile.out

type key struct {
	store [32]byte
	key   [32]byte
}

func BenchmarkMaps(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	db := map[key]int{}
	keys := make([]key, 0, 10_000)

	for i := range 10_000 {
		var k key
		_, _ = rand.Read(k.store[:])
		_, _ = rand.Read(k.key[:])
		keys = append(keys, k)

		db[k] = i
	}

	snapshot1 := map[key]int{}
	snapshot2 := map[key]int{}

	for range b.N {
		b.StartTimer()
		for i := range 1000 {
			for j := i * 10; j < i*10+10; j++ {
				k := keys[j]
				v2 := snapshot2[k]
				v1 := snapshot1[k]
				v := db[k]
				snapshot2[k] = v + v1 + v2 + j
			}
			for k, v := range snapshot2 {
				snapshot1[k] = v
			}
			clear(snapshot2)
		}
		for k, v := range snapshot1 {
			db[k] = v
		}
		clear(snapshot1)
		b.StopTimer()
	}
}

func BenchmarkQuantum(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	db := New[key, int]()
	keys := make([]key, 0, 10_000)

	for i := range 10_000 {
		var k key
		_, _ = rand.Read(k.store[:])
		_, _ = rand.Read(k.key[:])
		keys = append(keys, k)

		db.Set(k, i)
	}

	for range b.N {
		b.StartTimer()
		snapshot1 := db.Next()
		for i := range 1000 {
			snapshot2 := snapshot1.Next()
			for j := i * 10; j < i*10+10; j++ {
				k := keys[j]
				v, _ := snapshot2.Get(k)
				snapshot2.Set(k, v+j)
			}
			snapshot1 = snapshot2
		}
		db = snapshot1
		b.StopTimer()
	}
}
