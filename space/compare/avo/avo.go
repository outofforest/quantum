package main

//go:generate go run . -out ../asm.s -stubs ../asm_stub.go -pkg compare

import (
	"fmt"
	"math"

	. "github.com/mmcloughlin/avo/build"
	. "github.com/mmcloughlin/avo/operand"
	"github.com/mmcloughlin/avo/reg"
)

const (
	uint64Size = 8

	labelLoopChunks   = "loopChunks%d%t"
	labelLoopBits     = "loopBits%d%t"
	labelExitLoopBits = "exitLoopBits%d%t"
	labelExitZero     = "exitZero%d"
	labelZeroFound    = "zeroFound%d"
	labelExit         = "exit%d"
)

// Compare compares uint64 array against value.
func Compare() {
	const (
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
	rMaxUint64, rZeroIndex := GP64(), GP64()
	MOVD(U64(math.MaxUint64), rZeroIndex)
	MOVD(U64(math.MaxUint64), rMaxUint64)

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

	// 8 elements

	processChunks(
		8,
		memX, memZ,
		rChunkCounter, rIndexCounter, rOutputCounter, rZeroIndex,
		ZMM(), rCmpV, rCmp0,
		true,
	)

	Label(fmt.Sprintf(labelZeroFound, 8))

	processChunks(
		8,
		memX, memZ,
		rChunkCounter, rIndexCounter, rOutputCounter, rZeroIndex,
		ZMM(), rCmpV, rCmp0,
		false,
	)

	Label(fmt.Sprintf(labelExit, 8))

	// 4 elements

	CMPQ(rMaxUint64, rZeroIndex)
	JNE(LabelRef(fmt.Sprintf(labelZeroFound, 4)))

	processChunks(
		4,
		memX, memZ,
		rChunkCounter, rIndexCounter, rOutputCounter, rZeroIndex,
		YMM(), rCmpV.AsY(), rCmp0.AsY(),
		true,
	)

	Label(fmt.Sprintf(labelZeroFound, 4))

	processChunks(
		4,
		memX, memZ,
		rChunkCounter, rIndexCounter, rOutputCounter, rZeroIndex,
		YMM(), rCmpV.AsY(), rCmp0.AsY(),
		false,
	)

	Label(fmt.Sprintf(labelExit, 4))

	// 2 elements

	CMPQ(rMaxUint64, rZeroIndex)
	JNE(LabelRef(fmt.Sprintf(labelZeroFound, 2)))

	processChunks(
		2,
		memX, memZ,
		rChunkCounter, rIndexCounter, rOutputCounter, rZeroIndex,
		XMM(), rCmpV.AsX(), rCmp0.AsX(),
		true,
	)

	Label(fmt.Sprintf(labelZeroFound, 2))

	processChunks(
		2,
		memX, memZ,
		rChunkCounter, rIndexCounter, rOutputCounter, rZeroIndex,
		XMM(), rCmpV.AsX(), rCmp0.AsX(),
		false,
	)

	Label(fmt.Sprintf(labelExit, 2))

	// 1 element

	TESTQ(rChunkCounter, rChunkCounter)
	JZ(LabelRef(fmt.Sprintf(labelExit, 1)))

	rX := GP64()
	MOVQ(memX, rX)
	CMPQ(rX, rV)
	JNE(LabelRef("checkZero"))

	MOVQ(rIndexCounter, memZ)

	Label("checkZero")

	CMPQ(rMaxUint64, rZeroIndex)
	JNE(LabelRef(fmt.Sprintf(labelExit, 1)))

	TESTQ(rMaxUint64, rX)
	JNZ(LabelRef(fmt.Sprintf(labelExit, 1)))

	MOVQ(rIndexCounter, rZeroIndex)

	Label(fmt.Sprintf(labelExit, 1))

	// Return

	Store(rZeroIndex, ReturnIndex(outputZeroIndex))
	Store(rOutputCounter, ReturnIndex(outputCount))

	RET()
}

func processChunks(
	numOfValuesInChunk uint8,
	memX, memZ Mem,
	rChunkCounter, rIndexCounter, rOutputCounter, rZeroIndex reg.GPVirtual,
	rX, rCmpV, rCmp0 reg.Register,
	findZero bool,
) {
	if numOfValuesInChunk == 8 {
		Label(fmt.Sprintf(labelLoopChunks, numOfValuesInChunk, findZero))
	}

	// Return if there are no more chunks.
	CMPQ(rChunkCounter, U8(numOfValuesInChunk))
	JL(LabelRef(fmt.Sprintf(labelExit, numOfValuesInChunk)))
	SUBQ(U8(numOfValuesInChunk), rChunkCounter)

	// Load chunk and go to the next input.
	VMOVDQU64(memX, rX)
	ADDQ(U8(numOfValuesInChunk*uint64Size), memX.Base)

	// Compare values.
	rKMask := K()
	VPCMPEQQ(rX, rCmpV, rKMask)

	rMask := GP64()
	rIndex := GP64()

	MOVD(U64(0), rMask)
	KMOVB(rKMask, rMask.As32())

	Label(fmt.Sprintf(labelLoopBits, numOfValuesInChunk, findZero))
	TESTQ(rMask, rMask)
	JZ(LabelRef(fmt.Sprintf(labelExitLoopBits, numOfValuesInChunk, findZero)))

	// Find index of first 1 bit and reset it to 0.
	BSFQ(rMask, rIndex)
	BTRQ(rIndex, rMask)

	// Store result and go to the next output.
	ADDQ(rIndexCounter, rIndex)
	MOVD(rIndex, memZ)
	ADDQ(U8(uint64Size), memZ.Base)
	INCQ(rOutputCounter)

	JMP(LabelRef(fmt.Sprintf(labelLoopBits, numOfValuesInChunk, findZero)))

	Label(fmt.Sprintf(labelExitLoopBits, numOfValuesInChunk, findZero))

	if findZero {
		// Compare with 0.
		VPCMPEQQ(rX, rCmp0, rKMask)
		KMOVB(rKMask, rMask.As32())

		// Exit if 0 is not found.
		TESTQ(rMask, rMask)
		JZ(LabelRef(fmt.Sprintf(labelExitZero, numOfValuesInChunk)))

		// Return index of first 0.
		BSFQ(rMask, rZeroIndex)
		ADDQ(rIndexCounter, rZeroIndex)

		ADDQ(U8(numOfValuesInChunk), rIndexCounter)
		JMP(LabelRef(fmt.Sprintf(labelZeroFound, numOfValuesInChunk)))

		Label(fmt.Sprintf(labelExitZero, numOfValuesInChunk))
	}

	ADDQ(U8(numOfValuesInChunk), rIndexCounter)

	if numOfValuesInChunk == 8 {
		JMP(LabelRef(fmt.Sprintf(labelLoopChunks, numOfValuesInChunk, findZero)))
	} else {
		JMP(LabelRef(fmt.Sprintf(labelExit, numOfValuesInChunk)))
	}
}

func main() {
	Compare()

	Generate()
}
