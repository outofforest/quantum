// Code generated by command: go run avo.go -out ../asm.s -stubs ../asm_stub.go -pkg hash. DO NOT EDIT.

package hash

// Blake3 implements blake3 for 16 4KB messages.
func Blake3(b **byte, z **byte)

// Transpose8x16 transposes 8x16 matrix made of vectors x0..x7 and stores the results in z0..z7.
func Transpose8x16(x *uint32, z *uint32)

// Transpose16x16 transposes 16x16 matrix made of vectors x0..xf and stores the results in z0..zf.
func Transpose16x16(x *uint32, z *uint32)

// G implements g function of blake3.
func G(a *[16]uint32, b *[16]uint32, c *[16]uint32, d *[16]uint32, mx *[16]uint32, my *[16]uint32)
