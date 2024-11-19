package compare

import (
	"fmt"
	"io"
	"math"
	"testing"
)

var (
	m      uint8 = math.MaxUint8
	resDef       = [8]uint8{m, m, m, m, m, m, m, m}
	values       = []uint64{2, 1, 2, 3, 1, 0, 0, 4}
)

func BenchmarkCompareGo(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	res := resDef
	var foundZero bool
	var zeroIndex uint8

	b.StartTimer()
	for range b.N {
		var j uint8
		for i, v := range values {
			switch v {
			case 0:
				if !foundZero {
					zeroIndex = uint8(i)
					foundZero = true
				}
			case 2:
				res[j] = uint8(i)
			}
		}
	}
	b.StopTimer()

	_, _ = fmt.Fprint(io.Discard, res)
	_, _ = fmt.Fprint(io.Discard, zeroIndex)
}

func BenchmarkCompareAVX(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	res := resDef
	var zeroIndex uint8

	b.StartTimer()
	for range b.N {
		zeroIndex = Compare(2, &values[0], &res[0])
	}
	b.StopTimer()

	_, _ = fmt.Fprint(io.Discard, res)
	_, _ = fmt.Fprint(io.Discard, zeroIndex)
}
