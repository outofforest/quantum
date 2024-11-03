package main

//go:generate go run . -out ../asm.s -stubs ../asm_stub.go -pkg checksum

import (
	"fmt"

	. "github.com/mmcloughlin/avo/build"
	. "github.com/mmcloughlin/avo/operand"
	"github.com/mmcloughlin/avo/reg"
)

// Transpose transposes 16x16 matrix made of vectors x0..xf and stores the results in z0..zf.
func Transpose() {
	const (
		chunkSize = 16 * 4 // 512 bits or 16 uint32
		a         = 0xa
		b         = 0xb
		c         = 0xc
		d         = 0xd
		e         = 0xe
		f         = 0xf
	)

	TEXT("Transpose", NOSPLIT, "func(x, z *uint32)")
	Doc("Transpose transposes 16x16 matrix made of vectors x0..xf and stores the results in z0..zf.")

	/*
		00: 			00 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f
		01: 			10 11 12 13 14 15 16 17 18 19 1a 1b 1c 1d 1e 1f
		02: 			20 21 22 23 24 25 26 27 28 29 2a 2b 2c 2d 2e 2f
		03: 			30 31 32 33 34 35 36 37 38 39 3a 3b 3c 3d 3e 3f
		04: 			40 41 42 43 44 45 46 47 48 49 4a 4b 4c 4d 4e 4f
		05: 			50 51 52 53 54 55 56 57 58 59 5a 5b 5c 5d 5e 5f
		06: 			60 61 62 63 64 65 66 67 68 69 6a 6b 6c 6d 6e 6f
		07: 			70 71 72 73 74 75 76 77 78 79 7a 7b 7c 7d 7e 7f
		08: 			80 81 82 83 84 85 86 87 88 89 8a 8b 8c 8d 8e 8f
		09: 			90 91 92 93 94 95 96 97 98 99 9a 9b 9c 9d 9e 9f
		0a: 			a0 a1 a2 a3 a4 a5 a6 a7 a8 a9 aa ab ac ad ae af
		0b: 			b0 b1 b2 b3 b4 b5 b6 b7 b8 b9 ba bb bc bd be bf
		0c: 			c0 c1 c2 c3 c4 c5 c6 c7 c8 c9 ca cb cc cd ce cf
		0d: 			d0 d1 d2 d3 d4 d5 d6 d7 d8 d9 da db dc dd de df
		0e: 			e0 e1 e2 e3 e4 e5 e6 e7 e8 e9 ea eb ec ed ee ef
		0f: 			f0 f1 f2 f3 f4 f5 f6 f7 f8 f9 fa fb fc fd fe ff
	*/
	/*
		VSHUFPS
		10: 00,01,(44): 00 01 10 11 04 05 14 15 08 09 18 19 0c 0d 1c 1d
		11: 02,03,(44): 20 21 30 31 24 25 34 35 28 29 38 39 2c 2d 3c 3d
		12: 00,01,(ee): 02 03 12 13 06 07 16 17 0a 0b 1a 1b 0e 0f 1e 1f
		13: 02,03,(ee): 22 23 32 33 26 27 36 37 2a 2b 3a 3b 2e 2f 3e 3f
		14: 04,05,(44): 40 41 50 51 44 45 54 55 48 49 58 59 4c 4d 5c 5d
		15: 06,07,(44): 60 61 70 71 64 65 74 75 68 69 78 79 6c 6d 7c 7d
		16: 04,05,(ee): 42 43 52 53 46 47 56 57 4a 4b 5a 5b 4e 4f 5e 5f
		17: 06,07,(ee): 62 63 72 73 66 67 76 77 6a 6b 7a 7b 6e 6f 7e 7f
		18: 08,09,(44): 80 81 90 91 84 85 94 95 88 89 98 99 8c 8d 9c 9d
		19: 0a,0b,(44): a0 a1 b0 b1 a4 a5 b4 b5 a8 a9 b8 b9 ac ad bc bd
		1a: 08,09,(ee): 82 83 92 93 86 87 96 97 8a 8b 9a 9b 8e 8f 9e 9f
		1b: 0a,0b,(ee): a2 a3 b2 b3 a6 a7 b6 b7 aa ab ba bb ae af be bf
		1c: 0c,0d,(44): c0 c1 d0 d1 c4 c5 d4 d5 c8 c9 d8 d9 cc cd dc dd
		1d: 0e,0f,(44): e0 e1 f0 f1 e4 e5 f4 f5 e8 e9 f8 f9 ec ed fc fd
		1e: 0c,0d,(ee): c2 c3 d2 d3 c6 c7 d6 d7 ca cb da db ce cf de df
		1f: 0e,0f,(ee): e2 e3 f2 f3 e6 e7 f6 f7 ea eb fa fb ee ef fe ff
	*/
	/*
		VSHUFPS
		20: 10,11,(88): 00 10 20 30 04 14 24 34 08 18 28 38 0c 1c 2c 3c
		21: 14,15,(88): 40 50 60 70 44 54 64 74 48 58 68 78 4c 5c 6c 7c
		22: 10,11,(dd): 01 11 21 31 05 15 25 35 09 19 29 39 0d 1d 2d 3d
		23: 14,15,(dd): 41 51 61 71 45 55 65 75 49 59 69 79 4d 5d 6d 7d
		24: 12,13,(88): 02 12 22 32 06 16 26 36 0a 1a 2a 3a 0e 1e 2e 3e
		25: 16,17,(88): 42 52 62 72 46 56 66 76 4a 5a 6a 7a 4e 5e 6e 7e
		26: 12,13,(dd): 03 13 23 33 07 17 27 37 0b 1b 2b 3b 0f 1f 2f 3f
		27: 16,17,(dd): 43 53 63 73 47 57 67 77 4b 5b 6b 7b 4f 5f 6f 7f
		28: 18,19,(88): 80 90 a0 b0 84 94 a4 b4 88 98 a8 b8 8c 9c ac bc
		29: 1c,1d,(88): c0 d0 e0 f0 c4 d4 e4 f4 c8 d8 e8 f8 cc dc ec fc
		2a: 18,19,(dd): 81 91 a1 b1 85 95 a5 b5 89 99 a9 b9 8d 9d ad bd
		2b: 1c,1d,(dd): c1 d1 e1 f1 c5 d5 e5 f5 c9 d9 e9 f9 cd dd ed fd
		2c: 1a,1b,(88): 82 92 a2 b2 86 96 a6 b6 8a 9a aa ba 8e 9e ae be
		2d: 1e,1f,(88): c2 d2 e2 f2 c6 d6 e6 f6 ca da ea fa ce de ee fe
		2e: 1a,1b,(dd): 83 93 a3 b3 87 97 a7 b7 8b 9b ab bb 8f 9f af bf
		2f: 1e,1f,(dd): c3 d3 e3 f3 c7 d7 e7 f7 cb db eb fb cf df ef ff
	*/
	/*
		VSHUFI32X4
		30: 20,21,(44): 00 10 20 30 04 14 24 34 40 50 60 70 44 54 64 74
		31: 28,29,(44): 80 90 a0 b0 84 94 a4 b4 c0 d0 e0 f0 c4 d4 e4 f4
		32: 22,23,(44): 01 11 21 31 05 15 25 35 41 51 61 71 45 55 65 75
		33: 2a,2b,(44): 81 91 a1 b1 85 95 a5 b5 c1 d1 e1 f1 c5 d5 e5 f5
		34: 24,25,(44): 02 12 22 32 06 16 26 36 42 52 62 72 46 56 66 76
		35: 2c,2d,(44): 82 92 a2 b2 86 96 a6 b6 c2 d2 e2 f2 c6 d6 e6 f6
		36: 26,27,(44): 03 13 23 33 07 17 27 37 43 53 63 73 47 57 67 77
		37: 2e,2f,(44): 83 93 a3 b3 87 97 a7 b7 c3 d3 e3 f3 c7 d7 e7 f7
		38: 20,21,(ee): 08 18 28 38 0c 1c 2c 3c 48 58 68 78 4c 5c 6c 7c
		39: 28,29,(ee): 88 98 a8 b8 8c 9c ac bc c8 d8 e8 f8 cc dc ec fc
		3a: 22,23,(ee): 09 19 29 39 0d 1d 2d 3d 49 59 69 79 4d 5d 6d 7d
		3b: 2a,2b,(ee): 89 99 a9 b9 8d 9d ad bd c9 d9 e9 f9 cd dd ed fd
		3c: 24,25,(ee): 0a 1a 2a 3a 0e 1e 2e 3e 4a 5a 6a 7a 4e 5e 6e 7e
		3d: 2c,2d,(ee): 8a 9a aa ba 8e 9e ae be ca da ea fa ce de ee fe
		3e: 26,27,(ee): 0b 1b 2b 3b 0f 1f 2f 3f 4b 5b 6b 7b 4f 5f 6f 7f
		3f: 2e,2f,(ee): 8b 9b ab bb 8f 9f af bf cb db eb fb cf df ef ff
	*/
	/*
		VSHUFI32X4
		40: 30,31,(88): 00 10 20 30 40 50 60 70 80 90 a0 b0 c0 d0 e0 f0
		41: 32,33,(88): 01 11 21 31 41 51 61 71 81 91 a1 b1 c1 d1 e1 f1
		42: 34,35,(88): 02 12 22 32 42 52 62 72 82 92 a2 b2 c2 d2 e2 f2
		43: 36,37,(88): 03 13 23 33 43 53 63 73 83 93 a3 b3 c3 d3 e3 f3
		44: 30,31,(dd): 04 14 24 34 44 54 64 74 84 94 a4 b4 c4 d4 e4 f4
		45: 32,33,(dd): 05 15 25 35 45 55 65 75 85 95 a5 b5 c5 d5 e5 f5
		46: 34,35,(dd): 06 16 26 36 46 56 66 76 86 96 a6 b6 c6 d6 e6 f6
		47: 36,37,(dd): 07 17 27 37 47 57 67 77 87 97 a7 b7 c7 d7 e7 f7
		48: 38,39,(88): 08 18 28 38 48 58 68 78 88 98 a8 b8 c8 d8 e8 f8
		49: 3a,3b,(88): 09 19 29 39 49 59 69 79 89 99 a9 b9 c9 d9 e9 f9
		4a: 3c,3d,(88): 0a 1a 2a 3a 4a 5a 6a 7a 8a 9a aa ba ca da ea fa
		4b: 3e,3f,(88): 0b 1b 2b 3b 4b 5b 6b 7b 8b 9b ab bb cb db eb fb
		4c: 38,39,(dd): 0c 1c 2c 3c 4c 5c 6c 7c 8c 9c ac bc cc dc ec fc
		4d: 3a,3b,(dd): 0d 1d 2d 3d 4d 5d 6d 7d 8d 9d ad bd cd dd ed fd
		4e: 3c,3d,(dd): 0e 1e 2e 3e 4e 5e 6e 7e 8e 9e ae be ce de ee fe
		4f: 3e,3f,(dd): 0f 1f 2f 3f 4f 5f 6f 7f 8f 9f af bf cf df ef ff
	*/

	rA := [16]reg.VecVirtual{
		ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(),
	}
	rB := [16]reg.VecVirtual{
		ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(),
	}

	// Load matrix to registers

	mem := Mem{Base: Load(Param("x"), GP64())}
	for i := range rA {
		VMOVDQA64(mem.Offset(i*chunkSize), rA[i])
	}

	// 10..1f
	VSHUFPS(U8(0x44), rA[1], rA[0], rB[0])
	VSHUFPS(U8(0xee), rA[1], rA[0], rB[2])
	VSHUFPS(U8(0x44), rA[3], rA[2], rB[1])
	VSHUFPS(U8(0xee), rA[3], rA[2], rB[3])
	VSHUFPS(U8(0x44), rA[5], rA[4], rB[4])
	VSHUFPS(U8(0xee), rA[5], rA[4], rB[6])
	VSHUFPS(U8(0x44), rA[7], rA[6], rB[5])
	VSHUFPS(U8(0xee), rA[7], rA[6], rB[7])
	VSHUFPS(U8(0x44), rA[9], rA[8], rB[8])
	VSHUFPS(U8(0xee), rA[9], rA[8], rB[a])
	VSHUFPS(U8(0x44), rA[b], rA[a], rB[9])
	VSHUFPS(U8(0xee), rA[b], rA[a], rB[b])
	VSHUFPS(U8(0x44), rA[d], rA[c], rB[c])
	VSHUFPS(U8(0xee), rA[d], rA[c], rB[e])
	VSHUFPS(U8(0x44), rA[f], rA[e], rB[d])
	VSHUFPS(U8(0xee), rA[f], rA[e], rB[f])

	rA, rB = rB, rA

	// 20..2f
	VSHUFPS(U8(0x88), rA[1], rA[0], rB[0])
	VSHUFPS(U8(0xdd), rA[1], rA[0], rB[2])
	VSHUFPS(U8(0x88), rA[3], rA[2], rB[4])
	VSHUFPS(U8(0xdd), rA[3], rA[2], rB[6])
	VSHUFPS(U8(0x88), rA[5], rA[4], rB[1])
	VSHUFPS(U8(0xdd), rA[5], rA[4], rB[3])
	VSHUFPS(U8(0x88), rA[7], rA[6], rB[5])
	VSHUFPS(U8(0xdd), rA[7], rA[6], rB[7])
	VSHUFPS(U8(0x88), rA[9], rA[8], rB[8])
	VSHUFPS(U8(0xdd), rA[9], rA[8], rB[a])
	VSHUFPS(U8(0x88), rA[b], rA[a], rB[c])
	VSHUFPS(U8(0xdd), rA[b], rA[a], rB[e])
	VSHUFPS(U8(0x88), rA[d], rA[c], rB[9])
	VSHUFPS(U8(0xdd), rA[d], rA[c], rB[b])
	VSHUFPS(U8(0x88), rA[f], rA[e], rB[d])
	VSHUFPS(U8(0xdd), rA[f], rA[e], rB[f])

	rA, rB = rB, rA

	// 30..3f
	VSHUFI32X4(U8(0x44), rA[1], rA[0], rB[0])
	VSHUFI32X4(U8(0xee), rA[1], rA[0], rB[8])
	VSHUFI32X4(U8(0x44), rA[3], rA[2], rB[2])
	VSHUFI32X4(U8(0xee), rA[3], rA[2], rB[a])
	VSHUFI32X4(U8(0x44), rA[5], rA[4], rB[4])
	VSHUFI32X4(U8(0xee), rA[5], rA[4], rB[c])
	VSHUFI32X4(U8(0x44), rA[7], rA[6], rB[6])
	VSHUFI32X4(U8(0xee), rA[7], rA[6], rB[e])
	VSHUFI32X4(U8(0x44), rA[9], rA[8], rB[1])
	VSHUFI32X4(U8(0xee), rA[9], rA[8], rB[9])
	VSHUFI32X4(U8(0x44), rA[b], rA[a], rB[3])
	VSHUFI32X4(U8(0xee), rA[b], rA[a], rB[b])
	VSHUFI32X4(U8(0x44), rA[d], rA[c], rB[5])
	VSHUFI32X4(U8(0xee), rA[d], rA[c], rB[d])
	VSHUFI32X4(U8(0x44), rA[f], rA[e], rB[7])
	VSHUFI32X4(U8(0xee), rA[f], rA[e], rB[f])

	rA, rB = rB, rA

	// 40..4f
	VSHUFI32X4(U8(0x88), rA[1], rA[0], rB[0])
	VSHUFI32X4(U8(0xdd), rA[1], rA[0], rB[4])
	VSHUFI32X4(U8(0x88), rA[3], rA[2], rB[1])
	VSHUFI32X4(U8(0xdd), rA[3], rA[2], rB[5])
	VSHUFI32X4(U8(0x88), rA[5], rA[4], rB[2])
	VSHUFI32X4(U8(0xdd), rA[5], rA[4], rB[6])
	VSHUFI32X4(U8(0x88), rA[7], rA[6], rB[3])
	VSHUFI32X4(U8(0xdd), rA[7], rA[6], rB[7])
	VSHUFI32X4(U8(0x88), rA[9], rA[8], rB[8])
	VSHUFI32X4(U8(0xdd), rA[9], rA[8], rB[c])
	VSHUFI32X4(U8(0x88), rA[b], rA[a], rB[9])
	VSHUFI32X4(U8(0xdd), rA[b], rA[a], rB[d])
	VSHUFI32X4(U8(0x88), rA[d], rA[c], rB[a])
	VSHUFI32X4(U8(0xdd), rA[d], rA[c], rB[e])
	VSHUFI32X4(U8(0x88), rA[f], rA[e], rB[b])
	VSHUFI32X4(U8(0xdd), rA[f], rA[e], rB[f])

	// Store results

	mem = Mem{Base: Load(Param("z"), GP64())}
	for i := range rB {
		VMOVDQA64(rB[i], mem.Offset(i*chunkSize))
	}

	RET()
}

// G implements g function of blake3.
func G() {
	TEXT("G", NOSPLIT, "func(a, b, c, d, mx, my *[16]uint32)")
	Doc("G implements g function of blake3.")

	a, b, c, d, mx, my := ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM()

	memA := Mem{Base: Load(Param("a"), GP64())}
	memB := Mem{Base: Load(Param("b"), GP64())}
	memC := Mem{Base: Load(Param("c"), GP64())}
	memD := Mem{Base: Load(Param("d"), GP64())}

	VMOVDQA64(memA, a)
	VMOVDQA64(memB, b)
	VMOVDQA64(memC, c)
	VMOVDQA64(memD, d)
	VMOVDQA64(Mem{Base: Load(Param("mx"), GP64())}, mx)
	VMOVDQA64(Mem{Base: Load(Param("my"), GP64())}, my)

	// a += b + mx
	VPADDD(a, b, a)
	VPADDD(a, mx, a)

	// d = bits.RotateLeft32(d^a, -16)
	VPXORD(d, a, d)
	VPRORD(U8(16), d, d)

	// c += d
	VPADDD(c, d, c)

	// b = bits.RotateLeft32(b^c, -12)
	VPXORD(b, c, b)
	VPRORD(U8(12), b, b)

	// a += b + my
	VPADDD(a, b, a)
	VPADDD(a, my, a)

	// d = bits.RotateLeft32(d^a, -8)
	VPXORD(d, a, d)
	VPRORD(U8(8), d, d)

	// c += d
	VPADDD(c, d, c)

	// b = bits.RotateLeft32(b^c, -7)
	VPXORD(b, c, b)
	VPRORD(U8(7), b, b)

	VMOVDQA64(a, memA)
	VMOVDQA64(b, memB)
	VMOVDQA64(c, memC)
	VMOVDQA64(d, memD)

	RET()
}

// Add computes z = x + y.
func Add() {
	TEXT("Add", NOSPLIT, "func(x, y, z *[16]uint32)")
	Doc("Add computes z = x + y.")

	r := GP64()

	x := ZMM()
	mem := Mem{Base: Load(Param("x"), r)}
	VMOVDQA64(mem, x)

	y := ZMM()
	mem.Base = Load(Param("y"), r)
	VMOVDQA64(mem, y)

	VPADDD(x, y, x)

	mem.Base = Load(Param("z"), r)
	VMOVDQA64(x, mem)

	RET()
}

// Add10 computes z = x + y + y + y + y + y + y + y + y + y + y.
func Add10() {
	TEXT("Add10", NOSPLIT, "func(x, y, z *[16]uint32)")
	Doc("Add computes z = x + y + y + y + y + y + y + y + y + y + y.")

	r := GP64()

	x := ZMM()
	mem := Mem{Base: Load(Param("x"), r)}
	VMOVDQA64(mem, x)

	y := ZMM()
	mem.Base = Load(Param("y"), r)
	VMOVDQA64(mem, y)

	for range 10 {
		VPADDD(x, y, x)
	}

	mem.Base = Load(Param("z"), r)
	VMOVDQA64(x, mem)

	RET()
}

// Xor computes z = x ^ y.
func Xor() {
	TEXT("Xor", NOSPLIT, "func(x, y, z *[16]uint32)")
	Doc("// Xor computes z = x ^ y.")

	r := GP64()

	x := ZMM()
	mem := Mem{Base: Load(Param("x"), r)}
	VMOVDQA64(mem, x)

	y := ZMM()
	mem.Base = Load(Param("y"), r)
	VMOVDQA64(mem, y)

	VPXORD(x, y, x)

	mem.Base = Load(Param("z"), r)
	VMOVDQA64(x, mem)

	RET()
}

// Xor10 computes z = x ^ y ^ y ^ y ^ y ^ y ^ y ^ y ^ y ^ y ^ y.
func Xor10() {
	TEXT("Xor10", NOSPLIT, "func(x, y, z *[16]uint32)")
	Doc("// Xor10 computes z = x ^ y ^ y ^ y ^ y ^ y ^ y ^ y ^ y ^ y ^ y.")

	r := GP64()

	x := ZMM()
	mem := Mem{Base: Load(Param("x"), r)}
	VMOVDQA64(mem, x)

	y := ZMM()
	mem.Base = Load(Param("y"), r)
	VMOVDQA64(mem, y)

	for range 10 {
		VPXORD(x, y, x)
	}

	mem.Base = Load(Param("z"), r)
	VMOVDQA64(x, mem)

	RET()
}

// RotateRight generates functions RightRotationN computing z = x >>> N for N = 7, 8, 12, and 16.
func RotateRight(numOfBits uint8) {
	TEXT(fmt.Sprintf("RotateRight%d", numOfBits), NOSPLIT, "func(x *[16]uint32, z *[16]uint32)")
	Doc(fmt.Sprintf("// RotateRight%[1]d computes z = x >>> %[1]d.", numOfBits))

	r := GP64()

	x := ZMM()
	mem := Mem{Base: Load(Param("x"), r)}
	VMOVDQA64(mem, x)

	VPRORD(U8(numOfBits), x, x)

	mem.Base = Load(Param("z"), r)
	VMOVDQA64(x, mem)

	RET()
}

// RotateRight10 generates functions RightRotationN computing z = x >>> N >>> N >>> N >>> N >>> N >>> N >>> N >>> N >>> N >>> N
// for N = 7, 8, 12, and 16.
//
//nolint:lll
func RotateRight10(numOfBits uint8) {
	TEXT(fmt.Sprintf("RotateRight10%d", numOfBits), NOSPLIT, "func(x *[16]uint32, z *[16]uint32)")
	Doc(fmt.Sprintf("// RotateRight10%[1]d computes z = x >>> %[1]d >>> %[1]d >>> %[1]d >>> %[1]d >>> %[1]d >>> %[1]d >>> %[1]d >>> %[1]d >>> %[1]d >>> %[1]d.", numOfBits))

	r := GP64()

	x := ZMM()
	mem := Mem{Base: Load(Param("x"), r)}
	VMOVDQA64(mem, x)

	for range 10 {
		VPRORD(U8(numOfBits), x, x)
	}

	mem.Base = Load(Param("z"), r)
	VMOVDQA64(x, mem)

	RET()
}

func main() {
	Transpose()
	G()
	Add()
	Add10()
	Xor()
	Xor10()
	for _, numOfBits := range []uint8{7, 8, 12, 16} {
		RotateRight(numOfBits)
		RotateRight10(numOfBits)
	}

	Generate()
}
