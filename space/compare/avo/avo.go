package main

//go:generate go run . -out ../asm.s -stubs ../asm_stub.go -pkg compare

import (
	"math"

	. "github.com/mmcloughlin/avo/build"
	. "github.com/mmcloughlin/avo/operand"
	"github.com/mmcloughlin/avo/reg"
)

const (
	uint64Size         = 8
	numOfValuesInChunk = 8
	chunkSize          = numOfValuesInChunk * uint64Size

	labelReturn    = "return"
	labelExitZero  = "exitZero"
	labelZeroFound = "zeroFound"
)

// Compare compares uint64 array against value.
func Compare() {
	const (
		labelLoopChunks   = "loopChunks"
		labelLoopBits     = "loopBits"
		labelExitLoopBits = "exitLoopBits"

		outputZeroIndex = 0
		outputCount     = 1
	)

	TEXT("Compare", NOSPLIT, "func(v uint64, x *uint64, z *uint64, count uint64) (uint64, uint64)")
	Doc("Compare compares uint64 array against value.")

	// Load counters.
	rChunkCounter := GP64()
	Load(Param("count"), rChunkCounter)
	rIndexCounter := GP64()
	MOVD(U64(0), rIndexCounter)
	rOutputCounter := GP64()
	MOVD(U64(0), rOutputCounter)

	// Prepare zero index.
	// Set zero index to max uint64 to detect situation when 0 is not found.
	rZeroIndex := GP64()
	MOVD(U64(math.MaxUint64), rZeroIndex)

	// Prepare rCmp0 register to compare with 0.
	r0 := GP64()
	MOVD(U64(0), r0)
	rCmp0 := ZMM()
	VPBROADCASTQ(r0, rCmp0)

	// Prepare rCmpV register to compare with v.
	rV := Load(Param("v"), GP64())
	rCmpV := ZMM()
	VPBROADCASTQ(rV, rCmpV)

	memZ := Mem{Base: Load(Param("z"), GP64())}
	memX := Mem{Base: Load(Param("x"), GP64())}

	chunkLoop(
		labelLoopChunks, labelLoopBits, labelExitLoopBits,
		memX, memZ,
		rChunkCounter, rIndexCounter, rOutputCounter, rZeroIndex,
		rCmpV, rCmp0,
		true,
	)

	Label(labelZeroFound)

	chunkLoop(
		labelLoopChunks+"2", labelLoopBits+"2", labelExitLoopBits+"2",
		memX, memZ,
		rChunkCounter, rIndexCounter, rOutputCounter, rZeroIndex,
		rCmpV, rCmp0,
		false,
	)

	Label(labelReturn)

	Store(rZeroIndex, ReturnIndex(outputZeroIndex))
	Store(rOutputCounter, ReturnIndex(outputCount))

	RET()
}

func chunkLoop(
	labelLoopChunks, labelLoopBits, labelExitLoopBits string,
	memX, memZ Mem,
	rChunkCounter, rIndexCounter, rOutputCounter, rZeroIndex reg.GPVirtual,
	rCmpV, rCmp0 reg.VecVirtual,
	findZero bool,
) {
	Label(labelLoopChunks)

	// Return if there are no more chunks.
	TESTQ(rChunkCounter, rChunkCounter)
	JZ(LabelRef(labelReturn))
	DECQ(rChunkCounter)

	// Load chunk and go to the next input.
	rX := ZMM()
	VMOVDQU64(memX, rX)
	ADDQ(U8(chunkSize), memX.Base)

	// Compare values.
	rKMask := K()
	VPCMPEQQ(rX, rCmpV, rKMask)

	rMask := GP64()
	rIndex := GP64()

	MOVD(U64(0), rMask)
	KMOVB(rKMask, rMask.As32())

	Label(labelLoopBits)
	TESTQ(rMask, rMask)
	JZ(LabelRef(labelExitLoopBits))

	// Find index of first 1 bit and reset it to 0.
	BSFQ(rMask, rIndex)
	BTRQ(rIndex, rMask)

	// Store result and go to the next output.
	ADDQ(rIndexCounter, rIndex)
	MOVD(rIndex, memZ)
	ADDQ(U8(uint64Size), memZ.Base)
	INCQ(rOutputCounter)

	JMP(LabelRef(labelLoopBits))

	Label(labelExitLoopBits)

	if findZero {
		// Compare with 0.
		VPCMPEQQ(rX, rCmp0, rKMask)
		KMOVB(rKMask, rMask.As32())

		// Exit if 0 is not found.
		TESTQ(rMask, rMask)
		JZ(LabelRef(labelExitZero))

		// Return index of first 0.
		BSFQ(rMask, rZeroIndex)
		ADDQ(rIndexCounter, rZeroIndex)

		ADDQ(U8(numOfValuesInChunk), rIndexCounter)
		JMP(LabelRef(labelZeroFound))

		Label(labelExitZero)
	}

	ADDQ(U8(numOfValuesInChunk), rIndexCounter)
	JMP(LabelRef(labelLoopChunks))
}

func main() {
	Compare()

	Generate()
}
