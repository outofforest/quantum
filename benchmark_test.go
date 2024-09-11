package quantum

import (
	"crypto/rand"
	mathrand "math/rand"
	"testing"
)

// go test -benchtime=10x -bench=BenchmarkQuantum -run=^$ -cpuprofile profile.out
// go tool pprof -http="localhost:8000" pprofbin ./profile.out

type key struct {
	store [32]byte
	key   [32]byte
}

func BenchmarkMaps(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	db := map[key]int{}
	keys := make([]key, 0, 1000_000)

	for i := range cap(keys) {
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
		for range 1000 {
			for range 30 {
				k := keys[mathrand.Intn(len(keys))]
				v2 := snapshot2[k]
				v1 := snapshot1[k]
				v := db[k]
				snapshot2[k] = v + v1 + v2
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
	keys := make([]key, 0, 1000_000)

	for i := range cap(keys) {
		var k key
		_, _ = rand.Read(k.store[:])
		_, _ = rand.Read(k.key[:])
		keys = append(keys, k)

		db.Set(k, i)
	}

	for range b.N {
		b.StartTimer()
		for range 1000 {
			db = db.Next()
			for range 30 {
				k := keys[mathrand.Intn(len(keys))]
				v, _ := db.Get(k)
				db.Set(k, v)
			}
		}
		b.StopTimer()
	}
}
