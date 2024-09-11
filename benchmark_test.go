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

const (
	keysNum = 10_000_000
	loop1   = 1000
	loop2   = 30
)

var (
	keys      []key
	dbMap     map[key]int
	dbQuantum Snapshot[key, int]
)

func init() {
	keys = make([]key, 0, keysNum)

	for range cap(keys) {
		var k key
		_, _ = rand.Read(k.store[:])
		_, _ = rand.Read(k.key[:])
		keys = append(keys, k)
	}

	dbMap = map[key]int{}
	for i, k := range keys {
		dbMap[k] = i
	}

	dbQuantum = New[key, int]()
	for i, k := range keys {
		dbQuantum.Set(k, i)
	}

	for i := len(keys) - 1; i > 0; i-- {
		j := mathrand.Intn(i + 1)
		keys[i], keys[j] = keys[j], keys[i]
	}
}

func BenchmarkMaps(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	snapshot1 := map[key]int{}
	snapshot2 := map[key]int{}

	for range b.N {
		rands := make([]int, 0, loop1*loop2)
		for range cap(rands) {
			rands = append(rands, mathrand.Intn(len(keys)))
		}

		var ri int

		b.StartTimer()
		for range loop1 {
			for range loop2 {
				k := keys[rands[ri]]
				ri++
				v2 := snapshot2[k]
				v1 := snapshot1[k]
				v := dbMap[k]
				snapshot2[k] = v + v1 + v2
			}
			for k, v := range snapshot2 {
				snapshot1[k] = v
			}
			clear(snapshot2)
		}
		for k, v := range snapshot1 {
			dbMap[k] = v
		}
		clear(snapshot1)
		b.StopTimer()
	}
}

func BenchmarkQuantum(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	for range b.N {
		rands := make([]int, 0, loop1*loop2)
		for range cap(rands) {
			rands = append(rands, mathrand.Intn(len(keys)))
		}

		var ri int

		b.StartTimer()
		for range loop1 {
			dbQuantum = dbQuantum.Next()
			for range loop2 {
				k := keys[rands[ri]]
				ri++
				v, _ := dbQuantum.Get(k)
				dbQuantum.Set(k, v)
			}
		}
		b.StopTimer()
	}
}
