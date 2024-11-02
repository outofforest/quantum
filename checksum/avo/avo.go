package main

//go:generate go run . -out ../asm.s -stubs ../asm_stub.go -pkg checksum

import (
	. "github.com/mmcloughlin/avo/build"
	. "github.com/mmcloughlin/avo/operand"
)

func main() {
	TEXT("Add", NOSPLIT, "func(x, y, z *[16]uint32)")
	Doc("Add adds x and y vectors. Result is stored in vector z.")

	r := GP64()

	x := ZMM()
	mem := Mem{Base: Load(Param("x"), r)}
	VMOVDQU64(mem, x)

	y := ZMM()
	mem.Base = Load(Param("y"), r)
	VMOVDQU64(mem, y)

	VPADDD(x, y, x)

	mem.Base = Load(Param("z"), r)
	VMOVDQU64(x, mem)

	RET()
	Generate()
}
