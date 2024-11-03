// Code generated by command: go run avo.go -out ../asm.s -stubs ../asm_stub.go -pkg checksum. DO NOT EDIT.

package checksum

// Transpose transposes 16x16 matrix made of vectors x0..xf and stores the results in z0..zf.
func Transpose(x *[16]*[16]uint32, z *[16][16]uint32)

// G implements g function of blake3.
func G(a *[16]uint32, b *[16]uint32, c *[16]uint32, d *[16]uint32, mx *[16]uint32, my *[16]uint32)

// Add computes z = x + y.
func Add(x *[16]uint32, y *[16]uint32, z *[16]uint32)

// Add computes z = x + y + y + y + y + y + y + y + y + y + y.
func Add10(x *[16]uint32, y *[16]uint32, z *[16]uint32)

// // Xor computes z = x ^ y.
func Xor(x *[16]uint32, y *[16]uint32, z *[16]uint32)

// // Xor10 computes z = x ^ y ^ y ^ y ^ y ^ y ^ y ^ y ^ y ^ y ^ y.
func Xor10(x *[16]uint32, y *[16]uint32, z *[16]uint32)

// // RotateRight7 computes z = x >>> 7.
func RotateRight7(x *[16]uint32, z *[16]uint32)

// // RotateRight107 computes z = x >>> 7 >>> 7 >>> 7 >>> 7 >>> 7 >>> 7 >>> 7 >>> 7 >>> 7 >>> 7.
func RotateRight107(x *[16]uint32, z *[16]uint32)

// // RotateRight8 computes z = x >>> 8.
func RotateRight8(x *[16]uint32, z *[16]uint32)

// // RotateRight108 computes z = x >>> 8 >>> 8 >>> 8 >>> 8 >>> 8 >>> 8 >>> 8 >>> 8 >>> 8 >>> 8.
func RotateRight108(x *[16]uint32, z *[16]uint32)

// // RotateRight12 computes z = x >>> 12.
func RotateRight12(x *[16]uint32, z *[16]uint32)

// // RotateRight1012 computes z = x >>> 12 >>> 12 >>> 12 >>> 12 >>> 12 >>> 12 >>> 12 >>> 12 >>> 12 >>> 12.
func RotateRight1012(x *[16]uint32, z *[16]uint32)

// // RotateRight16 computes z = x >>> 16.
func RotateRight16(x *[16]uint32, z *[16]uint32)

// // RotateRight1016 computes z = x >>> 16 >>> 16 >>> 16 >>> 16 >>> 16 >>> 16 >>> 16 >>> 16 >>> 16 >>> 16.
func RotateRight1016(x *[16]uint32, z *[16]uint32)
