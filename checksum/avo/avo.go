package main

//go:generate go run . -out ../asm.s -stubs ../asm_stub.go -pkg checksum

import (
	. "github.com/mmcloughlin/avo/build"
	. "github.com/mmcloughlin/avo/operand"
)

func main() {
	TEXT("Add", NOSPLIT, "func(x, y, z *[8]uint32)")
	Doc("Add adds x and y vectors. Result is stored in vector z.")

	r := GP64()

	x := YMM()
	mem := Mem{Base: Load(Param("x"), r)}
	VMOVDQU(mem, x)

	y := YMM()
	mem.Base = Load(Param("y"), r)
	VMOVDQU(mem, y)

	VPADDD(x, y, x)

	mem.Base = Load(Param("z"), r)
	VMOVDQU(x, mem)

	RET()
	Generate()
}
