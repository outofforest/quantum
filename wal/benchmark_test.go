package wal_test

import (
	"crypto/rand"
	"fmt"
	"io"
	"testing"

	"github.com/outofforest/quantum/wal"
)

const size = 256

var (
	data = func() []byte {
		r := make([]byte, size)
		_, _ = rand.Read(r)
		return r
	}()
)

func BenchmarkGoCopy(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	data0 := make([]byte, size)
	data1 := make([]byte, size)
	data2 := make([]byte, size)
	data3 := make([]byte, size)
	data4 := make([]byte, size)
	data5 := make([]byte, size)
	data6 := make([]byte, size)
	data7 := make([]byte, size)
	data8 := make([]byte, size)
	data9 := make([]byte, size)

	b.StartTimer()
	for range b.N {
		copy(data0, data)
		copy(data1, data)
		copy(data2, data)
		copy(data3, data)
		copy(data4, data)
		copy(data5, data)
		copy(data6, data)
		copy(data7, data)
		copy(data8, data)
		copy(data9, data)
	}
	b.StopTimer()

	_, _ = fmt.Fprint(io.Discard, data0)
	_, _ = fmt.Fprint(io.Discard, data1)
	_, _ = fmt.Fprint(io.Discard, data2)
	_, _ = fmt.Fprint(io.Discard, data3)
	_, _ = fmt.Fprint(io.Discard, data4)
	_, _ = fmt.Fprint(io.Discard, data5)
	_, _ = fmt.Fprint(io.Discard, data6)
	_, _ = fmt.Fprint(io.Discard, data7)
	_, _ = fmt.Fprint(io.Discard, data8)
	_, _ = fmt.Fprint(io.Discard, data9)
}

func BenchmarkAVX(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	data0 := make([]byte, size)
	data1 := make([]byte, size)
	data2 := make([]byte, size)
	data3 := make([]byte, size)
	data4 := make([]byte, size)
	data5 := make([]byte, size)
	data6 := make([]byte, size)
	data7 := make([]byte, size)
	data8 := make([]byte, size)
	data9 := make([]byte, size)

	b.StartTimer()
	for range b.N {
		wal.Copy(&data0[0], &data1[0], &data[0])
		wal.Copy(&data2[0], &data3[0], &data[0])
		wal.Copy(&data4[0], &data5[0], &data[0])
		wal.Copy(&data6[0], &data7[0], &data[0])
		wal.Copy(&data8[0], &data9[0], &data[0])
	}
	b.StopTimer()

	_, _ = fmt.Fprint(io.Discard, data0)
	_, _ = fmt.Fprint(io.Discard, data1)
	_, _ = fmt.Fprint(io.Discard, data2)
	_, _ = fmt.Fprint(io.Discard, data3)
	_, _ = fmt.Fprint(io.Discard, data4)
	_, _ = fmt.Fprint(io.Discard, data5)
	_, _ = fmt.Fprint(io.Discard, data6)
	_, _ = fmt.Fprint(io.Discard, data7)
	_, _ = fmt.Fprint(io.Discard, data8)
	_, _ = fmt.Fprint(io.Discard, data9)
}
