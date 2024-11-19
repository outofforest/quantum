package main

//go:generate go run . -out ../asm.s -stubs ../asm_stub.go -pkg compare

import (
	. "github.com/mmcloughlin/avo/build"
	. "github.com/mmcloughlin/avo/operand"
)

const uint8Size = 1

// Compare compares uint64 array against value.
func Compare() {
	TEXT("Compare", NOSPLIT, "func(v uint64, x *uint64, z *uint8) uint8")
	Doc("Compare compares uint64 array against value.")

	memX := Mem{Base: Load(Param("x"), GP64())}
	rX := ZMM()
	VMOVDQU64(memX, rX)

	memZ := Mem{Base: Load(Param("z"), GP64())}

	rV := Load(Param("v"), GP64())
	rCmp := ZMM()
	rK := K()

	VPBROADCASTQ(rV, rCmp)
	VPCMPEQQ(rX, rCmp, rK)

	rR := GP64()
	rI := GP64()

	MOVD(U64(0), rR)
	KMOVB(rK, rR.As32())

	Label("loop")
	TESTQ(rR, rR)
	JZ(LabelRef("return"))

	BSFQ(rR, rI)
	BTRQ(rI, rR)

	MOVB(rI.As8(), memZ)
	ADDQ(U8(uint8Size), memZ.Base)

	JMP(LabelRef("loop"))

	Label("return")

	r0 := GP64()
	MOVD(U64(0), r0)

	VPBROADCASTQ(r0, rCmp)
	VPCMPEQQ(rX, rCmp, rK)
	KMOVB(rK, rR.As32())
	TESTQ(rR, rR)
	JZ(LabelRef("return2"))

	BSFQ(rR, rI)
	Store(rI.As8(), ReturnIndex(0))

	Label("return2")
	RET()
}

func main() {
	Compare()

	Generate()
}
