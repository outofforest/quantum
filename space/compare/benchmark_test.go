package compare

import (
	"fmt"
	"io"
	"math"
	"testing"
)

var (
	m      uint64 = math.MaxUint64
	resDef        = [32]uint64{
		m, m, m, m, m, m, m, m, m, m, m, m, m, m, m, m,
		m, m, m, m, m, m, m, m, m, m, m, m, m, m, m, m,
	}
	values = []uint64{
		2, 1, 2, 3, 1, 0, 0, 4, 2, 1, 2, 3, 1, 0, 0, 4,
		2, 1, 2, 3, 1, 0, 0, 4, 2, 1, 2, 3, 1, 0, 0, 4,
	}
)

func BenchmarkCompareGo(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	res := resDef
	var foundZero bool
	var zeroIndex, count uint64

	b.StartTimer()
	for range b.N {
		count = 0
		for i, v := range values {
			switch v {
			case 0:
				if !foundZero {
					zeroIndex = uint64(i)
					foundZero = true
				}
			case 2:
				res[count] = uint64(i)
				count++
			}
		}
	}
	b.StopTimer()

	_, _ = fmt.Fprint(io.Discard, res)
	_, _ = fmt.Fprint(io.Discard, zeroIndex)
	_, _ = fmt.Fprint(io.Discard, count)
}

func BenchmarkCompareAVX(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	res := resDef
	var zeroIndex, count uint64

	b.StartTimer()
	for range b.N {
		zeroIndex, count = Compare(2, &values[0], &res[0], 32)
	}
	b.StopTimer()

	_, _ = fmt.Fprint(io.Discard, res)
	_, _ = fmt.Fprint(io.Discard, zeroIndex)
	_, _ = fmt.Fprint(io.Discard, count)
}
