package main

//go:generate go run . -out ../asm.s -stubs ../asm_stub.go -pkg wal

import (
	. "github.com/mmcloughlin/avo/build"
	. "github.com/mmcloughlin/avo/operand"
)

// Copy copies data.
func Copy() {
	TEXT("Copy", NOSPLIT, "func(x *byte, y *byte, z *byte)")
	Doc("Copy copies data.")

	r := ZMM()
	memZ := Mem{Base: Load(Param("z"), GP64())}
	VMOVDQU64(memZ, r)

	memX := Mem{Base: Load(Param("x"), GP64())}
	VMOVDQU64(r, memX)
	memY := Mem{Base: Load(Param("y"), GP64())}
	VMOVDQU64(r, memY)

	RET()
}

func main() {
	Copy()

	Generate()
}
