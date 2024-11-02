package main

//go:generate go run . -out ../asm.s -stubs ../asm_stub.go -pkg checksum

import (
	"fmt"

	. "github.com/mmcloughlin/avo/build"
	. "github.com/mmcloughlin/avo/operand"
)

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

// RotateRight generates functions RightRotationN computing z = x >>> N for N = 7, 8, 12, and 16.
func RotateRight() {
	for _, numOfBits := range []uint8{7, 8, 12, 16} {
		rotateRight(numOfBits)
	}
}

func rotateRight(numOfBits uint8) {
	const uint32Length = 32

	TEXT(fmt.Sprintf("RotateRight%d", numOfBits), NOSPLIT, "func(x *[16]uint32, z *[16]uint32)")
	Doc(fmt.Sprintf("// RotateRight computes z = x >>> %d.", numOfBits))

	r := GP64()

	x := ZMM()
	mem := Mem{Base: Load(Param("x"), r)}
	VMOVDQA64(mem, x)

	x2 := ZMM()
	VPSRLD(U8(numOfBits), x, x2)
	VPSLLD(U8(uint32Length-numOfBits), x, x)
	VPORD(x, x2, x)

	mem.Base = Load(Param("z"), r)
	VMOVDQA64(x, mem)

	RET()
}

func main() {
	Add()
	Xor()
	RotateRight()

	Generate()
}
