package wal_test

import (
	"crypto/rand"
	"fmt"
	"io"
	"testing"
	"unsafe"

	"github.com/outofforest/quantum/wal"
)

type item struct {
	Field00 uint32
	Field01 uint32
	Field02 uint64
	Field03 uint64
	Field04 uint16
	Field05 uint16
	Field06 uint16
	Field07 uint16
	Field08 uint64
	Field09 uint64
	Field10 uint64
	Field11 uint64
}

var (
	data = func() item {
		var i item
		_, _ = rand.Read(unsafe.Slice((*byte)(unsafe.Pointer(&i)), unsafe.Sizeof(i)))
		return i
	}()
)

func BenchmarkGoCopy(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	var data0, data1, data2, data3, data4, data5, data6, data7, data8, data9 item

	dataB := unsafe.Slice((*byte)(unsafe.Pointer(&data)), unsafe.Sizeof(data))
	data0B := unsafe.Slice((*byte)(unsafe.Pointer(&data0)), unsafe.Sizeof(data0))
	data1B := unsafe.Slice((*byte)(unsafe.Pointer(&data1)), unsafe.Sizeof(data1))
	data2B := unsafe.Slice((*byte)(unsafe.Pointer(&data2)), unsafe.Sizeof(data2))
	data3B := unsafe.Slice((*byte)(unsafe.Pointer(&data3)), unsafe.Sizeof(data3))
	data4B := unsafe.Slice((*byte)(unsafe.Pointer(&data4)), unsafe.Sizeof(data4))
	data5B := unsafe.Slice((*byte)(unsafe.Pointer(&data5)), unsafe.Sizeof(data5))
	data6B := unsafe.Slice((*byte)(unsafe.Pointer(&data6)), unsafe.Sizeof(data6))
	data7B := unsafe.Slice((*byte)(unsafe.Pointer(&data7)), unsafe.Sizeof(data7))
	data8B := unsafe.Slice((*byte)(unsafe.Pointer(&data8)), unsafe.Sizeof(data8))
	data9B := unsafe.Slice((*byte)(unsafe.Pointer(&data9)), unsafe.Sizeof(data9))

	b.StartTimer()
	for range b.N {
		copy(data0B, dataB)
		copy(data1B, dataB)
		copy(data2B, dataB)
		copy(data3B, dataB)
		copy(data4B, dataB)
		copy(data5B, dataB)
		copy(data6B, dataB)
		copy(data7B, dataB)
		copy(data8B, dataB)
		copy(data9B, dataB)
	}
	b.StopTimer()

	_, _ = fmt.Fprint(io.Discard, data0B)
	_, _ = fmt.Fprint(io.Discard, data1B)
	_, _ = fmt.Fprint(io.Discard, data2B)
	_, _ = fmt.Fprint(io.Discard, data3)
	_, _ = fmt.Fprint(io.Discard, data4B)
	_, _ = fmt.Fprint(io.Discard, data5B)
	_, _ = fmt.Fprint(io.Discard, data6B)
	_, _ = fmt.Fprint(io.Discard, data7B)
	_, _ = fmt.Fprint(io.Discard, data8B)
	_, _ = fmt.Fprint(io.Discard, data9B)
}

func BenchmarkAVX(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	var data0, data1 item

	dataB := (*byte)(unsafe.Pointer(&data))
	data0B := (*byte)(unsafe.Pointer(&data0))
	data1B := (*byte)(unsafe.Pointer(&data1))

	b.StartTimer()
	for range b.N {
		wal.Copy(data0B, data1B, dataB)
		wal.Copy(data0B, data1B, dataB)
		wal.Copy(data0B, data1B, dataB)
		wal.Copy(data0B, data1B, dataB)
		wal.Copy(data0B, data1B, dataB)
	}
	b.StopTimer()

	_, _ = fmt.Fprint(io.Discard, data0)
	_, _ = fmt.Fprint(io.Discard, data1)
}
