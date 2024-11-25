package alloc

import (
	"os"
	"unsafe"

	"github.com/pkg/errors"
	"golang.org/x/sys/unix"
)

// Allocate allocates aligned memory.
func Allocate(size, alignment uint64, useHugePages bool) (unsafe.Pointer, func(), error) {
	opts := unix.MAP_SHARED | unix.MAP_ANONYMOUS | unix.MAP_POPULATE
	if useHugePages {
		// When using huge pages, the size must be a multiple of the hugepage size. Otherwise, munmap fails.
		opts |= unix.MAP_HUGETLB
	}
	alignmentUintptr := uintptr(alignment)
	allocatedSize := uintptr(size) + alignmentUintptr
	dataP, err := unix.MmapPtr(-1, 0, nil, allocatedSize, unix.PROT_READ|unix.PROT_WRITE, opts)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "memory allocation failed")
	}

	dataPOrig := dataP

	// Align allocated memory address to the node size. It might be required if using O_DIRECT option to open files.
	diff := uint64((uintptr(dataP)+alignmentUintptr-1)/alignmentUintptr*alignmentUintptr - uintptr(dataP))
	dataP = unsafe.Add(dataP, diff)

	return dataP, func() {
		// mmap might allocate more memory because it is always a multiple of the page size.
		// We need to provide that size to the munmap, not the original size we used for allocation, otherwise error is
		// returned and memory is not deallocated.
		// For non-hugepage setup, the case is easy because golang has a function returning the page size.
		// For hugepages there is no such function, but only two cases are possible: 2MB or 1GB. So we simply try both
		// cases.
		if useHugePages {
			// 2MB hugepages.
			if err := unmap(dataPOrig, allocatedSize, 2*1024*1024); err == nil {
				return
			}

			// 1GB hugepages.
			_ = unmap(dataPOrig, allocatedSize, 1024*1024*1024)
		}

		// Standard pages.
		_ = unmap(dataPOrig, allocatedSize, uintptr(os.Getpagesize()))
	}, nil
}

func unmap(ptr unsafe.Pointer, size, pageSize uintptr) error {
	return unix.MunmapPtr(ptr, (size+pageSize-1)/pageSize*pageSize)
}
