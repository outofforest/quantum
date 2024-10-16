package alloc

import (
	"fmt"
	"io"
	"testing"
	"unsafe"
)

const (
	length   = 1000
	elements = 10
)

func BenchmarkSliceOfByteSlices(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	buffer := make([]byte, length*elements)
	buffers := make([][]byte, 0, length)
	for i := range cap(buffers) {
		buffers = append(buffers, buffer[i*elements:(i+1)*elements])
	}

	var buf []byte

	b.StartTimer()
	for range b.N {
		for i := 0; i < length; i += 10 {
			buf = buffers[i]
			buf = buffers[i+1]
			buf = buffers[i+2]
			buf = buffers[i+3]
			buf = buffers[i+4]
			buf = buffers[i+5]
			buf = buffers[i+6]
			buf = buffers[i+7]
			buf = buffers[i+8]
			buf = buffers[i+9]
		}
	}
	b.StopTimer()

	_, _ = fmt.Fprint(io.Discard, buf)
}

func BenchmarkSliceOfBytePointers(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	buffer := make([]byte, length*elements)
	buffers := make([]*byte, 0, length)
	for i := range cap(buffers) {
		buffers = append(buffers, &buffer[i*elements])
	}

	var buf *byte

	b.StartTimer()
	for range b.N {
		for i := 0; i < length; i += 10 {
			buf = buffers[i]
			buf = buffers[i+1]
			buf = buffers[i+2]
			buf = buffers[i+3]
			buf = buffers[i+4]
			buf = buffers[i+5]
			buf = buffers[i+6]
			buf = buffers[i+7]
			buf = buffers[i+8]
			buf = buffers[i+9]
		}
	}
	b.StopTimer()

	_, _ = fmt.Fprint(io.Discard, buf)
}

func BenchmarkSliceOfUnsafePointers(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	buffer := make([]byte, length*elements)
	buffers := make([]unsafe.Pointer, 0, length)
	for i := range cap(buffers) {
		buffers = append(buffers, unsafe.Pointer(&buffer[i*elements]))
	}

	var buf unsafe.Pointer

	b.StartTimer()
	for range b.N {
		for i := 0; i < length; i += 10 {
			buf = buffers[i]
			buf = buffers[i+1]
			buf = buffers[i+2]
			buf = buffers[i+3]
			buf = buffers[i+4]
			buf = buffers[i+5]
			buf = buffers[i+6]
			buf = buffers[i+7]
			buf = buffers[i+8]
			buf = buffers[i+9]
		}
	}
	b.StopTimer()

	_, _ = fmt.Fprint(io.Discard, buf)
}

func BenchmarkUintPtr(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	buffer := make([]byte, length*elements)
	bufferP := uintptr(unsafe.Pointer(&buffer[0]))

	var buf uintptr

	b.StartTimer()
	for range b.N {
		for i := uintptr(0); i < length; i += 10 {
			buf = bufferP + i*elements
			buf = bufferP + (i+1)*elements
			buf = bufferP + (i+2)*elements
			buf = bufferP + (i+3)*elements
			buf = bufferP + (i+4)*elements
			buf = bufferP + (i+5)*elements
			buf = bufferP + (i+6)*elements
			buf = bufferP + (i+7)*elements
			buf = bufferP + (i+8)*elements
			buf = bufferP + (i+9)*elements
		}
	}
	b.StopTimer()

	_, _ = fmt.Fprint(io.Discard, buf)
}

func BenchmarkUnsafePointer(b *testing.B) {
	b.StopTimer()
	b.ResetTimer()

	buffer := make([]byte, length*elements)
	bufferP := unsafe.Pointer(&buffer[0])

	var buf unsafe.Pointer

	b.StartTimer()
	for range b.N {
		for i := uintptr(0); i < length; i += 10 {
			buf = unsafe.Add(bufferP, i*elements)
			buf = unsafe.Add(bufferP, (i+1)*elements)
			buf = unsafe.Add(bufferP, (i+2)*elements)
			buf = unsafe.Add(bufferP, (i+3)*elements)
			buf = unsafe.Add(bufferP, (i+4)*elements)
			buf = unsafe.Add(bufferP, (i+5)*elements)
			buf = unsafe.Add(bufferP, (i+6)*elements)
			buf = unsafe.Add(bufferP, (i+7)*elements)
			buf = unsafe.Add(bufferP, (i+8)*elements)
			buf = unsafe.Add(bufferP, (i+9)*elements)
		}
	}
	b.StopTimer()

	_, _ = fmt.Fprint(io.Discard, buf)
}
