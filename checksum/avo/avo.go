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
