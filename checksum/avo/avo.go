package main

//go:generate go run . -out ../asm.s -stubs ../asm_stub.go -pkg checksum

import (
	. "github.com/mmcloughlin/avo/build"
	. "github.com/mmcloughlin/avo/operand"
	"github.com/mmcloughlin/avo/reg"
)

const (
	uint64Size                = 8
	uint32Size                = 4
	numOfMessages             = 16
	numOfChunks               = 4
	numOfBlocksInChunk        = 16
	numOfStates               = 16
	blockSize                 = 16 * uint32Size
	iv0                uint32 = 0x6A09E667
	iv1                uint32 = 0xBB67AE85
	iv2                uint32 = 0x3C6EF372
	iv3                uint32 = 0xA54FF53A
	iv4                uint32 = 0x510E527F
	iv5                uint32 = 0x9B05688C
	iv6                uint32 = 0x1F83D9AB
	iv7                uint32 = 0x5BE0CD19
	flagChunkStart     uint32 = 1 << 0
	flagChunkEnd       uint32 = 1 << 1
	flagRoot           uint32 = 1 << 3
	a                         = 0xa
	b                         = 0xb
	c                         = 0xc
	d                         = 0xd
	e                         = 0xe
	f                         = 0xf
)

// Blake3 implements blake3 for 16 1KB messages.
func Blake3() {
	TEXT("Blake3", NOSPLIT, "func(b **byte, z **byte)")
	Doc("Blake3 implements blake3 for 16 1KB messages.")

	sInit := [16]uint32{
		iv0, iv1, iv2, iv3, iv4, iv5, iv6, iv7, iv0, iv1, iv2, iv3, 0, 0, blockSize, flagChunkStart,
	}

	// Init first s registers.
	rS1 := [numOfStates / 2]reg.VecVirtual{
		ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(),
	}
	r := GP64()
	for i := range numOfStates / 2 {
		MOVD(U32(sInit[i]), r)
		VPBROADCASTD(r.As32(), rS1[i])
	}

	rB := [numOfMessages]reg.VecVirtual{
		ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(),
		ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(),
	}

	memB := Mem{Base: Load(Param("b"), GP64())}
	for bi := range numOfChunks * numOfBlocksInChunk {
		// Load and transpose blocks.
		for i := range numOfMessages {
			m := Mem{Base: GP64()}
			MOVQ(memB.Offset((i*numOfBlocksInChunk+bi)*uint64Size), m.Base)
			VMOVDQA64(m, rB[i])
		}
		rB = transpose16x16(rB)

		// Init last s registers.
		if bi == numOfStates-1 {
			sInit[numOfStates-1] = flagChunkEnd | flagRoot
		}

		rS2 := [numOfStates / 2]reg.VecVirtual{
			ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(),
		}
		for i := numOfStates / 2; i < numOfStates; i++ {
			MOVD(U32(sInit[i]), r)
			VPBROADCASTD(r.As32(), rS2[i-numOfStates/2])
		}
		sInit[15] = 0 // flagChunkStart is set only for the first block.

		g(rS1[0], rS1[4], rS2[0], rS2[4], rB[0], rB[1])
		g(rS1[1], rS1[5], rS2[1], rS2[5], rB[2], rB[3])
		g(rS1[2], rS1[6], rS2[2], rS2[6], rB[4], rB[5])
		g(rS1[3], rS1[7], rS2[3], rS2[7], rB[6], rB[7])
		g(rS1[0], rS1[5], rS2[2], rS2[7], rB[8], rB[9])
		g(rS1[1], rS1[6], rS2[3], rS2[4], rB[a], rB[b])
		g(rS1[2], rS1[7], rS2[0], rS2[5], rB[c], rB[d])
		g(rS1[3], rS1[4], rS2[1], rS2[6], rB[e], rB[f])

		g(rS1[0], rS1[4], rS2[0], rS2[4], rB[2], rB[6])
		g(rS1[1], rS1[5], rS2[1], rS2[5], rB[3], rB[a])
		g(rS1[2], rS1[6], rS2[2], rS2[6], rB[7], rB[0])
		g(rS1[3], rS1[7], rS2[3], rS2[7], rB[4], rB[d])
		g(rS1[0], rS1[5], rS2[2], rS2[7], rB[1], rB[b])
		g(rS1[1], rS1[6], rS2[3], rS2[4], rB[c], rB[5])
		g(rS1[2], rS1[7], rS2[0], rS2[5], rB[9], rB[e])
		g(rS1[3], rS1[4], rS2[1], rS2[6], rB[f], rB[8])

		g(rS1[0], rS1[4], rS2[0], rS2[4], rB[3], rB[4])
		g(rS1[1], rS1[5], rS2[1], rS2[5], rB[a], rB[c])
		g(rS1[2], rS1[6], rS2[2], rS2[6], rB[d], rB[2])
		g(rS1[3], rS1[7], rS2[3], rS2[7], rB[7], rB[e])
		g(rS1[0], rS1[5], rS2[2], rS2[7], rB[6], rB[5])
		g(rS1[1], rS1[6], rS2[3], rS2[4], rB[9], rB[0])
		g(rS1[2], rS1[7], rS2[0], rS2[5], rB[b], rB[f])
		g(rS1[3], rS1[4], rS2[1], rS2[6], rB[8], rB[1])

		g(rS1[0], rS1[4], rS2[0], rS2[4], rB[a], rB[7])
		g(rS1[1], rS1[5], rS2[1], rS2[5], rB[c], rB[9])
		g(rS1[2], rS1[6], rS2[2], rS2[6], rB[e], rB[3])
		g(rS1[3], rS1[7], rS2[3], rS2[7], rB[d], rB[f])
		g(rS1[0], rS1[5], rS2[2], rS2[7], rB[4], rB[0])
		g(rS1[1], rS1[6], rS2[3], rS2[4], rB[b], rB[2])
		g(rS1[2], rS1[7], rS2[0], rS2[5], rB[5], rB[8])
		g(rS1[3], rS1[4], rS2[1], rS2[6], rB[1], rB[6])

		g(rS1[0], rS1[4], rS2[0], rS2[4], rB[c], rB[d])
		g(rS1[1], rS1[5], rS2[1], rS2[5], rB[9], rB[b])
		g(rS1[2], rS1[6], rS2[2], rS2[6], rB[f], rB[a])
		g(rS1[3], rS1[7], rS2[3], rS2[7], rB[e], rB[8])
		g(rS1[0], rS1[5], rS2[2], rS2[7], rB[7], rB[2])
		g(rS1[1], rS1[6], rS2[3], rS2[4], rB[5], rB[3])
		g(rS1[2], rS1[7], rS2[0], rS2[5], rB[0], rB[1])
		g(rS1[3], rS1[4], rS2[1], rS2[6], rB[6], rB[4])

		g(rS1[0], rS1[4], rS2[0], rS2[4], rB[9], rB[e])
		g(rS1[1], rS1[5], rS2[1], rS2[5], rB[b], rB[5])
		g(rS1[2], rS1[6], rS2[2], rS2[6], rB[8], rB[c])
		g(rS1[3], rS1[7], rS2[3], rS2[7], rB[f], rB[1])
		g(rS1[0], rS1[5], rS2[2], rS2[7], rB[d], rB[3])
		g(rS1[1], rS1[6], rS2[3], rS2[4], rB[0], rB[a])
		g(rS1[2], rS1[7], rS2[0], rS2[5], rB[2], rB[6])
		g(rS1[3], rS1[4], rS2[1], rS2[6], rB[4], rB[7])

		g(rS1[0], rS1[4], rS2[0], rS2[4], rB[b], rB[f])
		g(rS1[1], rS1[5], rS2[1], rS2[5], rB[5], rB[0])
		g(rS1[2], rS1[6], rS2[2], rS2[6], rB[1], rB[9])
		g(rS1[3], rS1[7], rS2[3], rS2[7], rB[8], rB[6])
		g(rS1[0], rS1[5], rS2[2], rS2[7], rB[e], rB[a])
		g(rS1[1], rS1[6], rS2[3], rS2[4], rB[2], rB[c])
		g(rS1[2], rS1[7], rS2[0], rS2[5], rB[3], rB[4])
		g(rS1[3], rS1[4], rS2[1], rS2[6], rB[7], rB[d])

		VPXORD(rS1[0], rS2[0], rS1[0])
		VPXORD(rS1[1], rS2[1], rS1[1])
		VPXORD(rS1[2], rS2[2], rS1[2])
		VPXORD(rS1[3], rS2[3], rS1[3])
		VPXORD(rS1[4], rS2[4], rS1[4])
		VPXORD(rS1[5], rS2[5], rS1[5])
		VPXORD(rS1[6], rS2[6], rS1[6])
		VPXORD(rS1[7], rS2[7], rS1[7])
	}

	rS1 = transpose8x16(rS1)

	memZ := Mem{Base: Load(Param("z"), GP64())}
	for i := range numOfMessages / 2 {
		m := Mem{Base: GP64()}
		MOVQ(memZ.Offset((2*i)*uint64Size), m.Base)
		VMOVDQA64(rS1[i].AsY(), m)

		MOVQ(memZ.Offset((2*i+1)*uint64Size), m.Base)
		VSHUFI32X4(U8(0xee), rS1[i], rS1[i], rS1[i])
		VMOVDQA64(rS1[i].AsY(), m)
	}

	RET()
}

// Transpose16x16 transposes 16x16 matrix made of vectors x0..xf and stores the results in z0..zf.
func Transpose16x16() {
	TEXT("Transpose16x16", NOSPLIT, "func(x *uint32, z *uint32)")
	Doc("Transpose16x16 transposes 16x16 matrix made of vectors x0..xf and stores the results in z0..zf.")

	// Load matrix to registers
	rX := [numOfBlocksInChunk]reg.VecVirtual{
		ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(),
	}
	memX := Mem{Base: Load(Param("x"), GP64())}
	for i := range numOfBlocksInChunk {
		VMOVDQA64(memX.Offset(i*blockSize), rX[i])
	}

	rB := transpose16x16(rX)

	// Store results
	memZ := Mem{Base: Load(Param("z"), GP64())}
	for i := range numOfBlocksInChunk {
		VMOVDQA64(rB[i], memZ.Offset(i*blockSize))
	}

	RET()
}

// Transpose8x16 transposes 8x16 matrix made of vectors x0..x7 and stores the results in z0..z7.
func Transpose8x16() {
	TEXT("Transpose8x16", NOSPLIT, "func(x *uint32, z *uint32)")
	Doc("Transpose8x16 transposes 8x16 matrix made of vectors x0..x7 and stores the results in z0..z7.")

	// Load matrix to registers
	rX := [numOfBlocksInChunk / 2]reg.VecVirtual{
		ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(), ZMM(),
	}
	memX := Mem{Base: Load(Param("x"), GP64())}
	for i := range numOfBlocksInChunk / 2 {
		VMOVDQA64(memX.Offset(i*blockSize), rX[i])
	}

	rB := transpose8x16(rX)

	// Store results
	memZ := Mem{Base: Load(Param("z"), GP64())}
	for i := range numOfBlocksInChunk / 2 {
		VMOVDQA64(rB[i], memZ.Offset(i*blockSize))
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

	g(a, b, c, d, mx, my)

	VMOVDQA64(a, memA)
	VMOVDQA64(b, memB)
	VMOVDQA64(c, memC)
	VMOVDQA64(d, memD)

	RET()
}

func transpose8x16(m [numOfStates / 2]reg.VecVirtual) [numOfMessages / 2]reg.VecVirtual {
	/*
		00: 			00 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f
		01: 			10 11 12 13 14 15 16 17 18 19 1a 1b 1c 1d 1e 1f
		02: 			20 21 22 23 24 25 26 27 28 29 2a 2b 2c 2d 2e 2f
		03: 			30 31 32 33 34 35 36 37 38 39 3a 3b 3c 3d 3e 3f
		04: 			40 41 42 43 44 45 46 47 48 49 4a 4b 4c 4d 4e 4f
		05: 			50 51 52 53 54 55 56 57 58 59 5a 5b 5c 5d 5e 5f
		06: 			60 61 62 63 64 65 66 67 68 69 6a 6b 6c 6d 6e 6f
		07: 			70 71 72 73 74 75 76 77 78 79 7a 7b 7c 7d 7e 7f
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
	*/
	/*
		VSHUFI32X4
		30: 20,21,(44): 00 10 20 30 04 14 24 34 40 50 60 70 44 54 64 74
		31: 22,23,(44): 01 11 21 31 05 15 25 35 41 51 61 71 45 55 65 75
		32: 24,25,(44): 02 12 22 32 06 16 26 36 42 52 62 72 46 56 66 76
		33: 26,27,(44): 03 13 23 33 07 17 27 37 43 53 63 73 47 57 67 77
		34: 20,21,(ee): 08 18 28 38 0c 1c 2c 3c 48 58 68 78 4c 5c 6c 7c
		35: 22,23,(ee): 09 19 29 39 0d 1d 2d 3d 49 59 69 79 4d 5d 6d 7d
		36: 24,25,(ee): 0a 1a 2a 3a 0e 1e 2e 3e 4a 5a 6a 7a 4e 5e 6e 7e
		37: 26,27,(ee): 0b 1b 2b 3b 0f 1f 2f 3f 4b 5b 6b 7b 4f 5f 6f 7f
	*/
	/*
		VSHUFI32X4
		40: 30,31,(88): 00 10 20 30 40 50 60 70 01 11 21 31 41 51 61 71
		41: 32,33,(88): 02 12 22 32 42 52 62 72 03 13 23 33 43 53 63 73
		42: 30,31,(dd): 04 14 24 34 44 54 64 74 05 15 25 35 45 55 65 75
		43: 32,33,(dd): 06 16 26 36 46 56 66 76 07 17 27 37 47 57 67 77
		44: 34,35,(88): 08 18 28 38 48 58 68 78 09 19 29 39 49 59 69 79
		45: 36,37,(88): 0a 1a 2a 3a 4a 5a 6a 7a 0b 1b 2b 3b 4b 5b 6b 7b
		46: 34,35,(dd): 0c 1c 2c 3c 4c 5c 6c 7c 0d 1d 2d 3d 4d 5d 6d 7d
		47: 36,37,(dd): 0e 1e 2e 3e 4e 5e 6e 7e 0f 1f 2f 3f 4f 5f 6f 7f
	*/

	t := ZMM()

	// 10..17
	VSHUFPS(U8(0x44), m[1], m[0], t)    // 0
	VSHUFPS(U8(0xee), m[1], m[0], m[0]) // 2
	VSHUFPS(U8(0x44), m[3], m[2], m[1]) // 1
	VSHUFPS(U8(0xee), m[3], m[2], m[2]) // 3
	VSHUFPS(U8(0x44), m[5], m[4], m[3]) // 4
	VSHUFPS(U8(0xee), m[5], m[4], m[4]) // 6
	VSHUFPS(U8(0x44), m[7], m[6], m[5]) // 5
	VSHUFPS(U8(0xee), m[7], m[6], m[6]) // 7

	// 20..27
	VSHUFPS(U8(0x88), m[1], t, m[7])    // 0
	VSHUFPS(U8(0xdd), m[1], t, t)       // 2
	VSHUFPS(U8(0x88), m[2], m[0], m[1]) // 4
	VSHUFPS(U8(0xdd), m[2], m[0], m[0]) // 6
	VSHUFPS(U8(0x88), m[5], m[3], m[2]) // 1
	VSHUFPS(U8(0xdd), m[5], m[3], m[3]) // 3
	VSHUFPS(U8(0x88), m[6], m[4], m[5]) // 5
	VSHUFPS(U8(0xdd), m[6], m[4], m[4]) // 7

	// 30..37
	VSHUFI32X4(U8(0x44), m[2], m[7], m[6]) // 0
	VSHUFI32X4(U8(0xee), m[2], m[7], m[7]) // 4
	VSHUFI32X4(U8(0x44), m[3], t, m[2])    // 1
	VSHUFI32X4(U8(0xee), m[3], t, t)       // 5
	VSHUFI32X4(U8(0x44), m[5], m[1], m[3]) // 2
	VSHUFI32X4(U8(0xee), m[5], m[1], m[1]) // 6
	VSHUFI32X4(U8(0x44), m[4], m[0], m[5]) // 3
	VSHUFI32X4(U8(0xee), m[4], m[0], m[0]) // 7

	/*
		VSHUFI32X4
		40: 30,31,(88): 00 10 20 30 40 50 60 70 01 11 21 31 41 51 61 71
		41: 32,33,(88): 02 12 22 32 42 52 62 72 03 13 23 33 43 53 63 73
		42: 30,31,(dd): 04 14 24 34 44 54 64 74 05 15 25 35 45 55 65 75
		43: 32,33,(dd): 06 16 26 36 46 56 66 76 07 17 27 37 47 57 67 77
		44: 34,35,(88): 08 18 28 38 48 58 68 78 09 19 29 39 49 59 69 79
		45: 36,37,(88): 0a 1a 2a 3a 4a 5a 6a 7a 0b 1b 2b 3b 4b 5b 6b 7b
		46: 34,35,(dd): 0c 1c 2c 3c 4c 5c 6c 7c 0d 1d 2d 3d 4d 5d 6d 7d
		47: 36,37,(dd): 0e 1e 2e 3e 4e 5e 6e 7e 0f 1f 2f 3f 4f 5f 6f 7f
	*/

	// 40..47
	VSHUFI32X4(U8(0x88), m[2], m[6], m[4]) // 0
	VSHUFI32X4(U8(0xdd), m[2], m[6], m[6]) // 2
	VSHUFI32X4(U8(0x88), m[5], m[3], m[2]) // 1
	VSHUFI32X4(U8(0xdd), m[5], m[3], m[3]) // 3
	VSHUFI32X4(U8(0x88), t, m[7], m[5])    // 4
	VSHUFI32X4(U8(0xdd), t, m[7], m[7])    // 6
	VSHUFI32X4(U8(0x88), m[0], m[1], t)    // 5
	VSHUFI32X4(U8(0xdd), m[0], m[1], m[1]) // 7

	return [numOfBlocksInChunk / 2]reg.VecVirtual{
		m[4], m[2], m[6], m[3], m[5], t, m[7], m[1],
	}
}

func transpose16x16(m [numOfMessages]reg.VecVirtual) [numOfBlocksInChunk]reg.VecVirtual {
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

	t := ZMM()

	// 10..1f
	VSHUFPS(U8(0x44), m[1], m[0], t)    // 0
	VSHUFPS(U8(0xee), m[1], m[0], m[0]) // 2
	VSHUFPS(U8(0x44), m[3], m[2], m[1]) // 1
	VSHUFPS(U8(0xee), m[3], m[2], m[2]) // 3
	VSHUFPS(U8(0x44), m[5], m[4], m[3]) // 4
	VSHUFPS(U8(0xee), m[5], m[4], m[4]) // 6
	VSHUFPS(U8(0x44), m[7], m[6], m[5]) // 5
	VSHUFPS(U8(0xee), m[7], m[6], m[6]) // 7
	VSHUFPS(U8(0x44), m[9], m[8], m[7]) // 8
	VSHUFPS(U8(0xee), m[9], m[8], m[8]) // a
	VSHUFPS(U8(0x44), m[b], m[a], m[9]) // 9
	VSHUFPS(U8(0xee), m[b], m[a], m[a]) // b
	VSHUFPS(U8(0x44), m[d], m[c], m[b]) // c
	VSHUFPS(U8(0xee), m[d], m[c], m[c]) // e
	VSHUFPS(U8(0x44), m[f], m[e], m[d]) // d
	VSHUFPS(U8(0xee), m[f], m[e], m[e]) // f

	// 20..2f
	VSHUFPS(U8(0x88), m[1], t, m[f])    // 0
	VSHUFPS(U8(0xdd), m[1], t, t)       // 2
	VSHUFPS(U8(0x88), m[2], m[0], m[1]) // 4
	VSHUFPS(U8(0xdd), m[2], m[0], m[0]) // 6
	VSHUFPS(U8(0x88), m[5], m[3], m[2]) // 1
	VSHUFPS(U8(0xdd), m[5], m[3], m[3]) // 3
	VSHUFPS(U8(0x88), m[6], m[4], m[5]) // 5
	VSHUFPS(U8(0xdd), m[6], m[4], m[4]) // 7
	VSHUFPS(U8(0x88), m[9], m[7], m[6]) // 8
	VSHUFPS(U8(0xdd), m[9], m[7], m[7]) // a
	VSHUFPS(U8(0x88), m[a], m[8], m[9]) // c
	VSHUFPS(U8(0xdd), m[a], m[8], m[8]) // e
	VSHUFPS(U8(0x88), m[d], m[b], m[a]) // 9
	VSHUFPS(U8(0xdd), m[d], m[b], m[b]) // b
	VSHUFPS(U8(0x88), m[e], m[c], m[d]) // d
	VSHUFPS(U8(0xdd), m[e], m[c], m[c]) // f

	// 30..3f
	VSHUFI32X4(U8(0x44), m[2], m[f], m[e]) // 0
	VSHUFI32X4(U8(0xee), m[2], m[f], m[f]) // 8
	VSHUFI32X4(U8(0x44), m[3], t, m[2])    // 2
	VSHUFI32X4(U8(0xee), m[3], t, t)       // a
	VSHUFI32X4(U8(0x44), m[5], m[1], m[3]) // 4
	VSHUFI32X4(U8(0xee), m[5], m[1], m[1]) // c
	VSHUFI32X4(U8(0x44), m[4], m[0], m[5]) // 6
	VSHUFI32X4(U8(0xee), m[4], m[0], m[0]) // e
	VSHUFI32X4(U8(0x44), m[a], m[6], m[4]) // 1
	VSHUFI32X4(U8(0xee), m[a], m[6], m[6]) // 9
	VSHUFI32X4(U8(0x44), m[b], m[7], m[a]) // 3
	VSHUFI32X4(U8(0xee), m[b], m[7], m[7]) // b
	VSHUFI32X4(U8(0x44), m[d], m[9], m[b]) // 5
	VSHUFI32X4(U8(0xee), m[d], m[9], m[9]) // d
	VSHUFI32X4(U8(0x44), m[c], m[8], m[d]) // 7
	VSHUFI32X4(U8(0xee), m[c], m[8], m[8]) // f

	// 40..4f
	VSHUFI32X4(U8(0x88), m[4], m[e], m[c]) // 0
	VSHUFI32X4(U8(0xdd), m[4], m[e], m[e]) // 4
	VSHUFI32X4(U8(0x88), m[a], m[2], m[4]) // 1
	VSHUFI32X4(U8(0xdd), m[a], m[2], m[2]) // 5
	VSHUFI32X4(U8(0x88), m[b], m[3], m[a]) // 2
	VSHUFI32X4(U8(0xdd), m[b], m[3], m[3]) // 6
	VSHUFI32X4(U8(0x88), m[d], m[5], m[b]) // 3
	VSHUFI32X4(U8(0xdd), m[d], m[5], m[5]) // 7
	VSHUFI32X4(U8(0x88), m[6], m[f], m[d]) // 8
	VSHUFI32X4(U8(0xdd), m[6], m[f], m[f]) // c
	VSHUFI32X4(U8(0x88), m[7], t, m[6])    // 9
	VSHUFI32X4(U8(0xdd), m[7], t, t)       // d
	VSHUFI32X4(U8(0x88), m[9], m[1], m[7]) // a
	VSHUFI32X4(U8(0xdd), m[9], m[1], m[1]) // e
	VSHUFI32X4(U8(0x88), m[8], m[0], m[9]) // b
	VSHUFI32X4(U8(0xdd), m[8], m[0], m[0]) // f

	return [numOfBlocksInChunk]reg.VecVirtual{
		m[c], m[4], m[a], m[b], m[e], m[2], m[3], m[5], m[d], m[6], m[7], m[9], m[f], t, m[1], m[0],
	}
}

func g(a, b, c, d, mx, my reg.VecVirtual) {
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
}

func main() {
	Blake3()
	Transpose8x16()
	Transpose16x16()
	G()

	Generate()
}
