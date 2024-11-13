// Code generated by command: go run avo.go -out ../asm.s -stubs ../asm_stub.go -pkg hash. DO NOT EDIT.

#include "textflag.h"

// func Blake3(b **byte, z **byte)
// Requires: AVX512F, AVX512VL
TEXT ·Blake3(SB), NOSPLIT, $0-16
	MOVD         $0x6a09e667, AX
	VPBROADCASTD AX, Z0
	MOVD         $0xbb67ae85, AX
	VPBROADCASTD AX, Z1
	MOVD         $0x3c6ef372, AX
	VPBROADCASTD AX, Z2
	MOVD         $0xa54ff53a, AX
	VPBROADCASTD AX, Z3
	MOVD         $0x510e527f, AX
	VPBROADCASTD AX, Z4
	MOVD         $0x9b05688c, AX
	VPBROADCASTD AX, Z5
	MOVD         $0x1f83d9ab, AX
	VPBROADCASTD AX, Z6
	MOVD         $0x5be0cd19, AX
	VPBROADCASTD AX, Z7
	MOVD         $0x6a09e667, AX
	MOVD         $0xbb67ae85, CX
	MOVD         $0x3c6ef372, DX
	MOVD         $0xa54ff53a, BX
	MOVD         $0x00000000, SI
	MOVD         $0x00000000, DI
	MOVD         $0x00000040, R8
	MOVD         $0x00000000, R9
	MOVQ         b+0(FP), R10
	MOVB         $0x40, R11

loopStart:
	MOVQ         (R10), R12
	VMOVDQU64    (R12), Z8
	MOVQ         512(R10), R12
	VMOVDQU64    (R12), Z9
	MOVQ         1024(R10), R12
	VMOVDQU64    (R12), Z10
	MOVQ         1536(R10), R12
	VMOVDQU64    (R12), Z11
	MOVQ         2048(R10), R12
	VMOVDQU64    (R12), Z12
	MOVQ         2560(R10), R12
	VMOVDQU64    (R12), Z13
	MOVQ         3072(R10), R12
	VMOVDQU64    (R12), Z14
	MOVQ         3584(R10), R12
	VMOVDQU64    (R12), Z15
	MOVQ         4096(R10), R12
	VMOVDQU64    (R12), Z16
	MOVQ         4608(R10), R12
	VMOVDQU64    (R12), Z17
	MOVQ         5120(R10), R12
	VMOVDQU64    (R12), Z18
	MOVQ         5632(R10), R12
	VMOVDQU64    (R12), Z19
	MOVQ         6144(R10), R12
	VMOVDQU64    (R12), Z20
	MOVQ         6656(R10), R12
	VMOVDQU64    (R12), Z21
	MOVQ         7168(R10), R12
	VMOVDQU64    (R12), Z22
	MOVQ         7680(R10), R12
	VMOVDQU64    (R12), Z23
	ADDQ         $0x08, R10
	VSHUFPS      $0x44, Z9, Z8, Z24
	VSHUFPS      $0xee, Z9, Z8, Z8
	VSHUFPS      $0x44, Z11, Z10, Z9
	VSHUFPS      $0xee, Z11, Z10, Z10
	VSHUFPS      $0x44, Z13, Z12, Z11
	VSHUFPS      $0xee, Z13, Z12, Z12
	VSHUFPS      $0x44, Z15, Z14, Z13
	VSHUFPS      $0xee, Z15, Z14, Z14
	VSHUFPS      $0x44, Z17, Z16, Z15
	VSHUFPS      $0xee, Z17, Z16, Z16
	VSHUFPS      $0x44, Z19, Z18, Z17
	VSHUFPS      $0xee, Z19, Z18, Z18
	VSHUFPS      $0x44, Z21, Z20, Z19
	VSHUFPS      $0xee, Z21, Z20, Z20
	VSHUFPS      $0x44, Z23, Z22, Z21
	VSHUFPS      $0xee, Z23, Z22, Z22
	VSHUFPS      $0x88, Z9, Z24, Z23
	VSHUFPS      $0xdd, Z9, Z24, Z24
	VSHUFPS      $0x88, Z10, Z8, Z9
	VSHUFPS      $0xdd, Z10, Z8, Z8
	VSHUFPS      $0x88, Z13, Z11, Z10
	VSHUFPS      $0xdd, Z13, Z11, Z11
	VSHUFPS      $0x88, Z14, Z12, Z13
	VSHUFPS      $0xdd, Z14, Z12, Z12
	VSHUFPS      $0x88, Z17, Z15, Z14
	VSHUFPS      $0xdd, Z17, Z15, Z15
	VSHUFPS      $0x88, Z18, Z16, Z17
	VSHUFPS      $0xdd, Z18, Z16, Z16
	VSHUFPS      $0x88, Z21, Z19, Z18
	VSHUFPS      $0xdd, Z21, Z19, Z19
	VSHUFPS      $0x88, Z22, Z20, Z21
	VSHUFPS      $0xdd, Z22, Z20, Z20
	VSHUFI32X4   $0x44, Z10, Z23, Z22
	VSHUFI32X4   $0xee, Z10, Z23, Z23
	VSHUFI32X4   $0x44, Z11, Z24, Z10
	VSHUFI32X4   $0xee, Z11, Z24, Z24
	VSHUFI32X4   $0x44, Z13, Z9, Z11
	VSHUFI32X4   $0xee, Z13, Z9, Z9
	VSHUFI32X4   $0x44, Z12, Z8, Z13
	VSHUFI32X4   $0xee, Z12, Z8, Z8
	VSHUFI32X4   $0x44, Z18, Z14, Z12
	VSHUFI32X4   $0xee, Z18, Z14, Z14
	VSHUFI32X4   $0x44, Z19, Z15, Z18
	VSHUFI32X4   $0xee, Z19, Z15, Z15
	VSHUFI32X4   $0x44, Z21, Z17, Z19
	VSHUFI32X4   $0xee, Z21, Z17, Z17
	VSHUFI32X4   $0x44, Z20, Z16, Z21
	VSHUFI32X4   $0xee, Z20, Z16, Z16
	VSHUFI32X4   $0x88, Z12, Z22, Z20
	VSHUFI32X4   $0xdd, Z12, Z22, Z22
	VSHUFI32X4   $0x88, Z18, Z10, Z12
	VSHUFI32X4   $0xdd, Z18, Z10, Z10
	VSHUFI32X4   $0x88, Z19, Z11, Z18
	VSHUFI32X4   $0xdd, Z19, Z11, Z11
	VSHUFI32X4   $0x88, Z21, Z13, Z19
	VSHUFI32X4   $0xdd, Z21, Z13, Z13
	VSHUFI32X4   $0x88, Z14, Z23, Z21
	VSHUFI32X4   $0xdd, Z14, Z23, Z23
	VSHUFI32X4   $0x88, Z15, Z24, Z14
	VSHUFI32X4   $0xdd, Z15, Z24, Z24
	VSHUFI32X4   $0x88, Z17, Z9, Z15
	VSHUFI32X4   $0xdd, Z17, Z9, Z9
	VSHUFI32X4   $0x88, Z16, Z8, Z17
	VSHUFI32X4   $0xdd, Z16, Z8, Z8
	VPBROADCASTD AX, Z16
	VPBROADCASTD CX, Z25
	VPBROADCASTD DX, Z26
	VPBROADCASTD BX, Z27
	VPBROADCASTD SI, Z28
	VPBROADCASTD DI, Z29
	VPBROADCASTD R8, Z30
	VPBROADCASTD R9, Z31
	VPADDD       Z0, Z4, Z0
	VPADDD       Z0, Z20, Z0
	VPXORD       Z28, Z0, Z28
	VPRORD       $0x10, Z28, Z28
	VPADDD       Z16, Z28, Z16
	VPXORD       Z4, Z16, Z4
	VPRORD       $0x0c, Z4, Z4
	VPADDD       Z0, Z4, Z0
	VPADDD       Z0, Z12, Z0
	VPXORD       Z28, Z0, Z28
	VPRORD       $0x08, Z28, Z28
	VPADDD       Z16, Z28, Z16
	VPXORD       Z4, Z16, Z4
	VPRORD       $0x07, Z4, Z4
	VPADDD       Z1, Z5, Z1
	VPADDD       Z1, Z18, Z1
	VPXORD       Z29, Z1, Z29
	VPRORD       $0x10, Z29, Z29
	VPADDD       Z25, Z29, Z25
	VPXORD       Z5, Z25, Z5
	VPRORD       $0x0c, Z5, Z5
	VPADDD       Z1, Z5, Z1
	VPADDD       Z1, Z19, Z1
	VPXORD       Z29, Z1, Z29
	VPRORD       $0x08, Z29, Z29
	VPADDD       Z25, Z29, Z25
	VPXORD       Z5, Z25, Z5
	VPRORD       $0x07, Z5, Z5
	VPADDD       Z2, Z6, Z2
	VPADDD       Z2, Z22, Z2
	VPXORD       Z30, Z2, Z30
	VPRORD       $0x10, Z30, Z30
	VPADDD       Z26, Z30, Z26
	VPXORD       Z6, Z26, Z6
	VPRORD       $0x0c, Z6, Z6
	VPADDD       Z2, Z6, Z2
	VPADDD       Z2, Z10, Z2
	VPXORD       Z30, Z2, Z30
	VPRORD       $0x08, Z30, Z30
	VPADDD       Z26, Z30, Z26
	VPXORD       Z6, Z26, Z6
	VPRORD       $0x07, Z6, Z6
	VPADDD       Z3, Z7, Z3
	VPADDD       Z3, Z11, Z3
	VPXORD       Z31, Z3, Z31
	VPRORD       $0x10, Z31, Z31
	VPADDD       Z27, Z31, Z27
	VPXORD       Z7, Z27, Z7
	VPRORD       $0x0c, Z7, Z7
	VPADDD       Z3, Z7, Z3
	VPADDD       Z3, Z13, Z3
	VPXORD       Z31, Z3, Z31
	VPRORD       $0x08, Z31, Z31
	VPADDD       Z27, Z31, Z27
	VPXORD       Z7, Z27, Z7
	VPRORD       $0x07, Z7, Z7
	VPADDD       Z0, Z5, Z0
	VPADDD       Z0, Z21, Z0
	VPXORD       Z31, Z0, Z31
	VPRORD       $0x10, Z31, Z31
	VPADDD       Z26, Z31, Z26
	VPXORD       Z5, Z26, Z5
	VPRORD       $0x0c, Z5, Z5
	VPADDD       Z0, Z5, Z0
	VPADDD       Z0, Z14, Z0
	VPXORD       Z31, Z0, Z31
	VPRORD       $0x08, Z31, Z31
	VPADDD       Z26, Z31, Z26
	VPXORD       Z5, Z26, Z5
	VPRORD       $0x07, Z5, Z5
	VPADDD       Z1, Z6, Z1
	VPADDD       Z1, Z15, Z1
	VPXORD       Z28, Z1, Z28
	VPRORD       $0x10, Z28, Z28
	VPADDD       Z27, Z28, Z27
	VPXORD       Z6, Z27, Z6
	VPRORD       $0x0c, Z6, Z6
	VPADDD       Z1, Z6, Z1
	VPADDD       Z1, Z17, Z1
	VPXORD       Z28, Z1, Z28
	VPRORD       $0x08, Z28, Z28
	VPADDD       Z27, Z28, Z27
	VPXORD       Z6, Z27, Z6
	VPRORD       $0x07, Z6, Z6
	VPADDD       Z2, Z7, Z2
	VPADDD       Z2, Z23, Z2
	VPXORD       Z29, Z2, Z29
	VPRORD       $0x10, Z29, Z29
	VPADDD       Z16, Z29, Z16
	VPXORD       Z7, Z16, Z7
	VPRORD       $0x0c, Z7, Z7
	VPADDD       Z2, Z7, Z2
	VPADDD       Z2, Z24, Z2
	VPXORD       Z29, Z2, Z29
	VPRORD       $0x08, Z29, Z29
	VPADDD       Z16, Z29, Z16
	VPXORD       Z7, Z16, Z7
	VPRORD       $0x07, Z7, Z7
	VPADDD       Z3, Z4, Z3
	VPADDD       Z3, Z9, Z3
	VPXORD       Z30, Z3, Z30
	VPRORD       $0x10, Z30, Z30
	VPADDD       Z25, Z30, Z25
	VPXORD       Z4, Z25, Z4
	VPRORD       $0x0c, Z4, Z4
	VPADDD       Z3, Z4, Z3
	VPADDD       Z3, Z8, Z3
	VPXORD       Z30, Z3, Z30
	VPRORD       $0x08, Z30, Z30
	VPADDD       Z25, Z30, Z25
	VPXORD       Z4, Z25, Z4
	VPRORD       $0x07, Z4, Z4
	VPADDD       Z0, Z4, Z0
	VPADDD       Z0, Z18, Z0
	VPXORD       Z28, Z0, Z28
	VPRORD       $0x10, Z28, Z28
	VPADDD       Z16, Z28, Z16
	VPXORD       Z4, Z16, Z4
	VPRORD       $0x0c, Z4, Z4
	VPADDD       Z0, Z4, Z0
	VPADDD       Z0, Z11, Z0
	VPXORD       Z28, Z0, Z28
	VPRORD       $0x08, Z28, Z28
	VPADDD       Z16, Z28, Z16
	VPXORD       Z4, Z16, Z4
	VPRORD       $0x07, Z4, Z4
	VPADDD       Z1, Z5, Z1
	VPADDD       Z1, Z19, Z1
	VPXORD       Z29, Z1, Z29
	VPRORD       $0x10, Z29, Z29
	VPADDD       Z25, Z29, Z25
	VPXORD       Z5, Z25, Z5
	VPRORD       $0x0c, Z5, Z5
	VPADDD       Z1, Z5, Z1
	VPADDD       Z1, Z15, Z1
	VPXORD       Z29, Z1, Z29
	VPRORD       $0x08, Z29, Z29
	VPADDD       Z25, Z29, Z25
	VPXORD       Z5, Z25, Z5
	VPRORD       $0x07, Z5, Z5
	VPADDD       Z2, Z6, Z2
	VPADDD       Z2, Z13, Z2
	VPXORD       Z30, Z2, Z30
	VPRORD       $0x10, Z30, Z30
	VPADDD       Z26, Z30, Z26
	VPXORD       Z6, Z26, Z6
	VPRORD       $0x0c, Z6, Z6
	VPADDD       Z2, Z6, Z2
	VPADDD       Z2, Z20, Z2
	VPXORD       Z30, Z2, Z30
	VPRORD       $0x08, Z30, Z30
	VPADDD       Z26, Z30, Z26
	VPXORD       Z6, Z26, Z6
	VPRORD       $0x07, Z6, Z6
	VPADDD       Z3, Z7, Z3
	VPADDD       Z3, Z22, Z3
	VPXORD       Z31, Z3, Z31
	VPRORD       $0x10, Z31, Z31
	VPADDD       Z27, Z31, Z27
	VPXORD       Z7, Z27, Z7
	VPRORD       $0x0c, Z7, Z7
	VPADDD       Z3, Z7, Z3
	VPADDD       Z3, Z24, Z3
	VPXORD       Z31, Z3, Z31
	VPRORD       $0x08, Z31, Z31
	VPADDD       Z27, Z31, Z27
	VPXORD       Z7, Z27, Z7
	VPRORD       $0x07, Z7, Z7
	VPADDD       Z0, Z5, Z0
	VPADDD       Z0, Z12, Z0
	VPXORD       Z31, Z0, Z31
	VPRORD       $0x10, Z31, Z31
	VPADDD       Z26, Z31, Z26
	VPXORD       Z5, Z26, Z5
	VPRORD       $0x0c, Z5, Z5
	VPADDD       Z0, Z5, Z0
	VPADDD       Z0, Z17, Z0
	VPXORD       Z31, Z0, Z31
	VPRORD       $0x08, Z31, Z31
	VPADDD       Z26, Z31, Z26
	VPXORD       Z5, Z26, Z5
	VPRORD       $0x07, Z5, Z5
	VPADDD       Z1, Z6, Z1
	VPADDD       Z1, Z23, Z1
	VPXORD       Z28, Z1, Z28
	VPRORD       $0x10, Z28, Z28
	VPADDD       Z27, Z28, Z27
	VPXORD       Z6, Z27, Z6
	VPRORD       $0x0c, Z6, Z6
	VPADDD       Z1, Z6, Z1
	VPADDD       Z1, Z10, Z1
	VPXORD       Z28, Z1, Z28
	VPRORD       $0x08, Z28, Z28
	VPADDD       Z27, Z28, Z27
	VPXORD       Z6, Z27, Z6
	VPRORD       $0x07, Z6, Z6
	VPADDD       Z2, Z7, Z2
	VPADDD       Z2, Z14, Z2
	VPXORD       Z29, Z2, Z29
	VPRORD       $0x10, Z29, Z29
	VPADDD       Z16, Z29, Z16
	VPXORD       Z7, Z16, Z7
	VPRORD       $0x0c, Z7, Z7
	VPADDD       Z2, Z7, Z2
	VPADDD       Z2, Z9, Z2
	VPXORD       Z29, Z2, Z29
	VPRORD       $0x08, Z29, Z29
	VPADDD       Z16, Z29, Z16
	VPXORD       Z7, Z16, Z7
	VPRORD       $0x07, Z7, Z7
	VPADDD       Z3, Z4, Z3
	VPADDD       Z3, Z8, Z3
	VPXORD       Z30, Z3, Z30
	VPRORD       $0x10, Z30, Z30
	VPADDD       Z25, Z30, Z25
	VPXORD       Z4, Z25, Z4
	VPRORD       $0x0c, Z4, Z4
	VPADDD       Z3, Z4, Z3
	VPADDD       Z3, Z21, Z3
	VPXORD       Z30, Z3, Z30
	VPRORD       $0x08, Z30, Z30
	VPADDD       Z25, Z30, Z25
	VPXORD       Z4, Z25, Z4
	VPRORD       $0x07, Z4, Z4
	VPADDD       Z0, Z4, Z0
	VPADDD       Z0, Z19, Z0
	VPXORD       Z28, Z0, Z28
	VPRORD       $0x10, Z28, Z28
	VPADDD       Z16, Z28, Z16
	VPXORD       Z4, Z16, Z4
	VPRORD       $0x0c, Z4, Z4
	VPADDD       Z0, Z4, Z0
	VPADDD       Z0, Z22, Z0
	VPXORD       Z28, Z0, Z28
	VPRORD       $0x08, Z28, Z28
	VPADDD       Z16, Z28, Z16
	VPXORD       Z4, Z16, Z4
	VPRORD       $0x07, Z4, Z4
	VPADDD       Z1, Z5, Z1
	VPADDD       Z1, Z15, Z1
	VPXORD       Z29, Z1, Z29
	VPRORD       $0x10, Z29, Z29
	VPADDD       Z25, Z29, Z25
	VPXORD       Z5, Z25, Z5
	VPRORD       $0x0c, Z5, Z5
	VPADDD       Z1, Z5, Z1
	VPADDD       Z1, Z23, Z1
	VPXORD       Z29, Z1, Z29
	VPRORD       $0x08, Z29, Z29
	VPADDD       Z25, Z29, Z25
	VPXORD       Z5, Z25, Z5
	VPRORD       $0x07, Z5, Z5
	VPADDD       Z2, Z6, Z2
	VPADDD       Z2, Z24, Z2
	VPXORD       Z30, Z2, Z30
	VPRORD       $0x10, Z30, Z30
	VPADDD       Z26, Z30, Z26
	VPXORD       Z6, Z26, Z6
	VPRORD       $0x0c, Z6, Z6
	VPADDD       Z2, Z6, Z2
	VPADDD       Z2, Z18, Z2
	VPXORD       Z30, Z2, Z30
	VPRORD       $0x08, Z30, Z30
	VPADDD       Z26, Z30, Z26
	VPXORD       Z6, Z26, Z6
	VPRORD       $0x07, Z6, Z6
	VPADDD       Z3, Z7, Z3
	VPADDD       Z3, Z13, Z3
	VPXORD       Z31, Z3, Z31
	VPRORD       $0x10, Z31, Z31
	VPADDD       Z27, Z31, Z27
	VPXORD       Z7, Z27, Z7
	VPRORD       $0x0c, Z7, Z7
	VPADDD       Z3, Z7, Z3
	VPADDD       Z3, Z9, Z3
	VPXORD       Z31, Z3, Z31
	VPRORD       $0x08, Z31, Z31
	VPADDD       Z27, Z31, Z27
	VPXORD       Z7, Z27, Z7
	VPRORD       $0x07, Z7, Z7
	VPADDD       Z0, Z5, Z0
	VPADDD       Z0, Z11, Z0
	VPXORD       Z31, Z0, Z31
	VPRORD       $0x10, Z31, Z31
	VPADDD       Z26, Z31, Z26
	VPXORD       Z5, Z26, Z5
	VPRORD       $0x0c, Z5, Z5
	VPADDD       Z0, Z5, Z0
	VPADDD       Z0, Z10, Z0
	VPXORD       Z31, Z0, Z31
	VPRORD       $0x08, Z31, Z31
	VPADDD       Z26, Z31, Z26
	VPXORD       Z5, Z26, Z5
	VPRORD       $0x07, Z5, Z5
	VPADDD       Z1, Z6, Z1
	VPADDD       Z1, Z14, Z1
	VPXORD       Z28, Z1, Z28
	VPRORD       $0x10, Z28, Z28
	VPADDD       Z27, Z28, Z27
	VPXORD       Z6, Z27, Z6
	VPRORD       $0x0c, Z6, Z6
	VPADDD       Z1, Z6, Z1
	VPADDD       Z1, Z20, Z1
	VPXORD       Z28, Z1, Z28
	VPRORD       $0x08, Z28, Z28
	VPADDD       Z27, Z28, Z27
	VPXORD       Z6, Z27, Z6
	VPRORD       $0x07, Z6, Z6
	VPADDD       Z2, Z7, Z2
	VPADDD       Z2, Z17, Z2
	VPXORD       Z29, Z2, Z29
	VPRORD       $0x10, Z29, Z29
	VPADDD       Z16, Z29, Z16
	VPXORD       Z7, Z16, Z7
	VPRORD       $0x0c, Z7, Z7
	VPADDD       Z2, Z7, Z2
	VPADDD       Z2, Z8, Z2
	VPXORD       Z29, Z2, Z29
	VPRORD       $0x08, Z29, Z29
	VPADDD       Z16, Z29, Z16
	VPXORD       Z7, Z16, Z7
	VPRORD       $0x07, Z7, Z7
	VPADDD       Z3, Z4, Z3
	VPADDD       Z3, Z21, Z3
	VPXORD       Z30, Z3, Z30
	VPRORD       $0x10, Z30, Z30
	VPADDD       Z25, Z30, Z25
	VPXORD       Z4, Z25, Z4
	VPRORD       $0x0c, Z4, Z4
	VPADDD       Z3, Z4, Z3
	VPADDD       Z3, Z12, Z3
	VPXORD       Z30, Z3, Z30
	VPRORD       $0x08, Z30, Z30
	VPADDD       Z25, Z30, Z25
	VPXORD       Z4, Z25, Z4
	VPRORD       $0x07, Z4, Z4
	VPADDD       Z0, Z4, Z0
	VPADDD       Z0, Z15, Z0
	VPXORD       Z28, Z0, Z28
	VPRORD       $0x10, Z28, Z28
	VPADDD       Z16, Z28, Z16
	VPXORD       Z4, Z16, Z4
	VPRORD       $0x0c, Z4, Z4
	VPADDD       Z0, Z4, Z0
	VPADDD       Z0, Z13, Z0
	VPXORD       Z28, Z0, Z28
	VPRORD       $0x08, Z28, Z28
	VPADDD       Z16, Z28, Z16
	VPXORD       Z4, Z16, Z4
	VPRORD       $0x07, Z4, Z4
	VPADDD       Z1, Z5, Z1
	VPADDD       Z1, Z23, Z1
	VPXORD       Z29, Z1, Z29
	VPRORD       $0x10, Z29, Z29
	VPADDD       Z25, Z29, Z25
	VPXORD       Z5, Z25, Z5
	VPRORD       $0x0c, Z5, Z5
	VPADDD       Z1, Z5, Z1
	VPADDD       Z1, Z14, Z1
	VPXORD       Z29, Z1, Z29
	VPRORD       $0x08, Z29, Z29
	VPADDD       Z25, Z29, Z25
	VPXORD       Z5, Z25, Z5
	VPRORD       $0x07, Z5, Z5
	VPADDD       Z2, Z6, Z2
	VPADDD       Z2, Z9, Z2
	VPXORD       Z30, Z2, Z30
	VPRORD       $0x10, Z30, Z30
	VPADDD       Z26, Z30, Z26
	VPXORD       Z6, Z26, Z6
	VPRORD       $0x0c, Z6, Z6
	VPADDD       Z2, Z6, Z2
	VPADDD       Z2, Z19, Z2
	VPXORD       Z30, Z2, Z30
	VPRORD       $0x08, Z30, Z30
	VPADDD       Z26, Z30, Z26
	VPXORD       Z6, Z26, Z6
	VPRORD       $0x07, Z6, Z6
	VPADDD       Z3, Z7, Z3
	VPADDD       Z3, Z24, Z3
	VPXORD       Z31, Z3, Z31
	VPRORD       $0x10, Z31, Z31
	VPADDD       Z27, Z31, Z27
	VPXORD       Z7, Z27, Z7
	VPRORD       $0x0c, Z7, Z7
	VPADDD       Z3, Z7, Z3
	VPADDD       Z3, Z8, Z3
	VPXORD       Z31, Z3, Z31
	VPRORD       $0x08, Z31, Z31
	VPADDD       Z27, Z31, Z27
	VPXORD       Z7, Z27, Z7
	VPRORD       $0x07, Z7, Z7
	VPADDD       Z0, Z5, Z0
	VPADDD       Z0, Z22, Z0
	VPXORD       Z31, Z0, Z31
	VPRORD       $0x10, Z31, Z31
	VPADDD       Z26, Z31, Z26
	VPXORD       Z5, Z26, Z5
	VPRORD       $0x0c, Z5, Z5
	VPADDD       Z0, Z5, Z0
	VPADDD       Z0, Z20, Z0
	VPXORD       Z31, Z0, Z31
	VPRORD       $0x08, Z31, Z31
	VPADDD       Z26, Z31, Z26
	VPXORD       Z5, Z26, Z5
	VPRORD       $0x07, Z5, Z5
	VPADDD       Z1, Z6, Z1
	VPADDD       Z1, Z17, Z1
	VPXORD       Z28, Z1, Z28
	VPRORD       $0x10, Z28, Z28
	VPADDD       Z27, Z28, Z27
	VPXORD       Z6, Z27, Z6
	VPRORD       $0x0c, Z6, Z6
	VPADDD       Z1, Z6, Z1
	VPADDD       Z1, Z18, Z1
	VPXORD       Z28, Z1, Z28
	VPRORD       $0x08, Z28, Z28
	VPADDD       Z27, Z28, Z27
	VPXORD       Z6, Z27, Z6
	VPRORD       $0x07, Z6, Z6
	VPADDD       Z2, Z7, Z2
	VPADDD       Z2, Z10, Z2
	VPXORD       Z29, Z2, Z29
	VPRORD       $0x10, Z29, Z29
	VPADDD       Z16, Z29, Z16
	VPXORD       Z7, Z16, Z7
	VPRORD       $0x0c, Z7, Z7
	VPADDD       Z2, Z7, Z2
	VPADDD       Z2, Z21, Z2
	VPXORD       Z29, Z2, Z29
	VPRORD       $0x08, Z29, Z29
	VPADDD       Z16, Z29, Z16
	VPXORD       Z7, Z16, Z7
	VPRORD       $0x07, Z7, Z7
	VPADDD       Z3, Z4, Z3
	VPADDD       Z3, Z12, Z3
	VPXORD       Z30, Z3, Z30
	VPRORD       $0x10, Z30, Z30
	VPADDD       Z25, Z30, Z25
	VPXORD       Z4, Z25, Z4
	VPRORD       $0x0c, Z4, Z4
	VPADDD       Z3, Z4, Z3
	VPADDD       Z3, Z11, Z3
	VPXORD       Z30, Z3, Z30
	VPRORD       $0x08, Z30, Z30
	VPADDD       Z25, Z30, Z25
	VPXORD       Z4, Z25, Z4
	VPRORD       $0x07, Z4, Z4
	VPADDD       Z0, Z4, Z0
	VPADDD       Z0, Z23, Z0
	VPXORD       Z28, Z0, Z28
	VPRORD       $0x10, Z28, Z28
	VPADDD       Z16, Z28, Z16
	VPXORD       Z4, Z16, Z4
	VPRORD       $0x0c, Z4, Z4
	VPADDD       Z0, Z4, Z0
	VPADDD       Z0, Z24, Z0
	VPXORD       Z28, Z0, Z28
	VPRORD       $0x08, Z28, Z28
	VPADDD       Z16, Z28, Z16
	VPXORD       Z4, Z16, Z4
	VPRORD       $0x07, Z4, Z4
	VPADDD       Z1, Z5, Z1
	VPADDD       Z1, Z14, Z1
	VPXORD       Z29, Z1, Z29
	VPRORD       $0x10, Z29, Z29
	VPADDD       Z25, Z29, Z25
	VPXORD       Z5, Z25, Z5
	VPRORD       $0x0c, Z5, Z5
	VPADDD       Z1, Z5, Z1
	VPADDD       Z1, Z17, Z1
	VPXORD       Z29, Z1, Z29
	VPRORD       $0x08, Z29, Z29
	VPADDD       Z25, Z29, Z25
	VPXORD       Z5, Z25, Z5
	VPRORD       $0x07, Z5, Z5
	VPADDD       Z2, Z6, Z2
	VPADDD       Z2, Z8, Z2
	VPXORD       Z30, Z2, Z30
	VPRORD       $0x10, Z30, Z30
	VPADDD       Z26, Z30, Z26
	VPXORD       Z6, Z26, Z6
	VPRORD       $0x0c, Z6, Z6
	VPADDD       Z2, Z6, Z2
	VPADDD       Z2, Z15, Z2
	VPXORD       Z30, Z2, Z30
	VPRORD       $0x08, Z30, Z30
	VPADDD       Z26, Z30, Z26
	VPXORD       Z6, Z26, Z6
	VPRORD       $0x07, Z6, Z6
	VPADDD       Z3, Z7, Z3
	VPADDD       Z3, Z9, Z3
	VPXORD       Z31, Z3, Z31
	VPRORD       $0x10, Z31, Z31
	VPADDD       Z27, Z31, Z27
	VPXORD       Z7, Z27, Z7
	VPRORD       $0x0c, Z7, Z7
	VPADDD       Z3, Z7, Z3
	VPADDD       Z3, Z21, Z3
	VPXORD       Z31, Z3, Z31
	VPRORD       $0x08, Z31, Z31
	VPADDD       Z27, Z31, Z27
	VPXORD       Z7, Z27, Z7
	VPRORD       $0x07, Z7, Z7
	VPADDD       Z0, Z5, Z0
	VPADDD       Z0, Z13, Z0
	VPXORD       Z31, Z0, Z31
	VPRORD       $0x10, Z31, Z31
	VPADDD       Z26, Z31, Z26
	VPXORD       Z5, Z26, Z5
	VPRORD       $0x0c, Z5, Z5
	VPADDD       Z0, Z5, Z0
	VPADDD       Z0, Z18, Z0
	VPXORD       Z31, Z0, Z31
	VPRORD       $0x08, Z31, Z31
	VPADDD       Z26, Z31, Z26
	VPXORD       Z5, Z26, Z5
	VPRORD       $0x07, Z5, Z5
	VPADDD       Z1, Z6, Z1
	VPADDD       Z1, Z10, Z1
	VPXORD       Z28, Z1, Z28
	VPRORD       $0x10, Z28, Z28
	VPADDD       Z27, Z28, Z27
	VPXORD       Z6, Z27, Z6
	VPRORD       $0x0c, Z6, Z6
	VPADDD       Z1, Z6, Z1
	VPADDD       Z1, Z19, Z1
	VPXORD       Z28, Z1, Z28
	VPRORD       $0x08, Z28, Z28
	VPADDD       Z27, Z28, Z27
	VPXORD       Z6, Z27, Z6
	VPRORD       $0x07, Z6, Z6
	VPADDD       Z2, Z7, Z2
	VPADDD       Z2, Z20, Z2
	VPXORD       Z29, Z2, Z29
	VPRORD       $0x10, Z29, Z29
	VPADDD       Z16, Z29, Z16
	VPXORD       Z7, Z16, Z7
	VPRORD       $0x0c, Z7, Z7
	VPADDD       Z2, Z7, Z2
	VPADDD       Z2, Z12, Z2
	VPXORD       Z29, Z2, Z29
	VPRORD       $0x08, Z29, Z29
	VPADDD       Z16, Z29, Z16
	VPXORD       Z7, Z16, Z7
	VPRORD       $0x07, Z7, Z7
	VPADDD       Z3, Z4, Z3
	VPADDD       Z3, Z11, Z3
	VPXORD       Z30, Z3, Z30
	VPRORD       $0x10, Z30, Z30
	VPADDD       Z25, Z30, Z25
	VPXORD       Z4, Z25, Z4
	VPRORD       $0x0c, Z4, Z4
	VPADDD       Z3, Z4, Z3
	VPADDD       Z3, Z22, Z3
	VPXORD       Z30, Z3, Z30
	VPRORD       $0x08, Z30, Z30
	VPADDD       Z25, Z30, Z25
	VPXORD       Z4, Z25, Z4
	VPRORD       $0x07, Z4, Z4
	VPADDD       Z0, Z4, Z0
	VPADDD       Z0, Z14, Z0
	VPXORD       Z28, Z0, Z28
	VPRORD       $0x10, Z28, Z28
	VPADDD       Z16, Z28, Z16
	VPXORD       Z4, Z16, Z4
	VPRORD       $0x0c, Z4, Z4
	VPADDD       Z0, Z4, Z0
	VPADDD       Z0, Z9, Z0
	VPXORD       Z28, Z0, Z28
	VPRORD       $0x08, Z28, Z28
	VPADDD       Z16, Z28, Z16
	VPXORD       Z4, Z16, Z4
	VPRORD       $0x07, Z4, Z4
	VPADDD       Z1, Z5, Z1
	VPADDD       Z1, Z17, Z1
	VPXORD       Z29, Z1, Z29
	VPRORD       $0x10, Z29, Z29
	VPADDD       Z25, Z29, Z25
	VPXORD       Z5, Z25, Z5
	VPRORD       $0x0c, Z5, Z5
	VPADDD       Z1, Z5, Z1
	VPADDD       Z1, Z10, Z1
	VPXORD       Z29, Z1, Z29
	VPRORD       $0x08, Z29, Z29
	VPADDD       Z25, Z29, Z25
	VPXORD       Z5, Z25, Z5
	VPRORD       $0x07, Z5, Z5
	VPADDD       Z2, Z6, Z2
	VPADDD       Z2, Z21, Z2
	VPXORD       Z30, Z2, Z30
	VPRORD       $0x10, Z30, Z30
	VPADDD       Z26, Z30, Z26
	VPXORD       Z6, Z26, Z6
	VPRORD       $0x0c, Z6, Z6
	VPADDD       Z2, Z6, Z2
	VPADDD       Z2, Z23, Z2
	VPXORD       Z30, Z2, Z30
	VPRORD       $0x08, Z30, Z30
	VPADDD       Z26, Z30, Z26
	VPXORD       Z6, Z26, Z6
	VPRORD       $0x07, Z6, Z6
	VPADDD       Z3, Z7, Z3
	VPADDD       Z3, Z8, Z3
	VPXORD       Z31, Z3, Z31
	VPRORD       $0x10, Z31, Z31
	VPADDD       Z27, Z31, Z27
	VPXORD       Z7, Z27, Z7
	VPRORD       $0x0c, Z7, Z7
	VPADDD       Z3, Z7, Z3
	VPADDD       Z3, Z12, Z3
	VPXORD       Z31, Z3, Z31
	VPRORD       $0x08, Z31, Z31
	VPADDD       Z27, Z31, Z27
	VPXORD       Z7, Z27, Z7
	VPRORD       $0x07, Z7, Z7
	VPADDD       Z0, Z5, Z0
	VPADDD       Z0, Z24, Z0
	VPXORD       Z31, Z0, Z31
	VPRORD       $0x10, Z31, Z31
	VPADDD       Z26, Z31, Z26
	VPXORD       Z5, Z26, Z5
	VPRORD       $0x0c, Z5, Z5
	VPADDD       Z0, Z5, Z0
	VPADDD       Z0, Z19, Z0
	VPXORD       Z31, Z0, Z31
	VPRORD       $0x08, Z31, Z31
	VPADDD       Z26, Z31, Z26
	VPXORD       Z5, Z26, Z5
	VPRORD       $0x07, Z5, Z5
	VPADDD       Z1, Z6, Z1
	VPADDD       Z1, Z20, Z1
	VPXORD       Z28, Z1, Z28
	VPRORD       $0x10, Z28, Z28
	VPADDD       Z27, Z28, Z27
	VPXORD       Z6, Z27, Z6
	VPRORD       $0x0c, Z6, Z6
	VPADDD       Z1, Z6, Z1
	VPADDD       Z1, Z15, Z1
	VPXORD       Z28, Z1, Z28
	VPRORD       $0x08, Z28, Z28
	VPADDD       Z27, Z28, Z27
	VPXORD       Z6, Z27, Z6
	VPRORD       $0x07, Z6, Z6
	VPADDD       Z2, Z7, Z2
	VPADDD       Z2, Z18, Z2
	VPXORD       Z29, Z2, Z29
	VPRORD       $0x10, Z29, Z29
	VPADDD       Z16, Z29, Z16
	VPXORD       Z7, Z16, Z7
	VPRORD       $0x0c, Z7, Z7
	VPADDD       Z2, Z7, Z2
	VPADDD       Z2, Z11, Z2
	VPXORD       Z29, Z2, Z29
	VPRORD       $0x08, Z29, Z29
	VPADDD       Z16, Z29, Z16
	VPXORD       Z7, Z16, Z7
	VPRORD       $0x07, Z7, Z7
	VPADDD       Z3, Z4, Z3
	VPADDD       Z3, Z22, Z3
	VPXORD       Z30, Z3, Z30
	VPRORD       $0x10, Z30, Z30
	VPADDD       Z25, Z30, Z25
	VPXORD       Z4, Z25, Z4
	VPRORD       $0x0c, Z4, Z4
	VPADDD       Z3, Z4, Z3
	VPADDD       Z3, Z13, Z3
	VPXORD       Z30, Z3, Z30
	VPRORD       $0x08, Z30, Z30
	VPADDD       Z25, Z30, Z25
	VPXORD       Z4, Z25, Z4
	VPRORD       $0x07, Z4, Z4
	VPADDD       Z0, Z4, Z0
	VPADDD       Z0, Z17, Z0
	VPXORD       Z28, Z0, Z28
	VPRORD       $0x10, Z28, Z28
	VPADDD       Z16, Z28, Z16
	VPXORD       Z4, Z16, Z4
	VPRORD       $0x0c, Z4, Z4
	VPADDD       Z0, Z4, Z0
	VPADDD       Z0, Z8, Z0
	VPXORD       Z28, Z0, Z28
	VPRORD       $0x08, Z28, Z28
	VPADDD       Z16, Z28, Z16
	VPXORD       Z4, Z16, Z4
	VPRORD       $0x07, Z4, Z4
	VPADDD       Z1, Z5, Z1
	VPADDD       Z1, Z10, Z1
	VPXORD       Z29, Z1, Z29
	VPRORD       $0x10, Z29, Z29
	VPADDD       Z25, Z29, Z25
	VPXORD       Z5, Z25, Z5
	VPRORD       $0x0c, Z5, Z5
	VPADDD       Z1, Z5, Z1
	VPADDD       Z1, Z20, Z1
	VPXORD       Z29, Z1, Z29
	VPRORD       $0x08, Z29, Z29
	VPADDD       Z25, Z29, Z25
	VPXORD       Z5, Z25, Z5
	VPRORD       $0x07, Z5, Z5
	VPADDD       Z2, Z6, Z2
	VPADDD       Z2, Z12, Z2
	VPXORD       Z30, Z2, Z30
	VPRORD       $0x10, Z30, Z30
	VPADDD       Z26, Z30, Z26
	VPXORD       Z6, Z26, Z6
	VPRORD       $0x0c, Z6, Z6
	VPADDD       Z2, Z6, Z2
	VPADDD       Z2, Z14, Z2
	VPXORD       Z30, Z2, Z30
	VPRORD       $0x08, Z30, Z30
	VPADDD       Z26, Z30, Z26
	VPXORD       Z6, Z26, Z6
	VPRORD       $0x07, Z6, Z6
	VPADDD       Z3, Z7, Z3
	VPADDD       Z3, Z21, Z3
	VPXORD       Z31, Z3, Z31
	VPRORD       $0x10, Z31, Z31
	VPADDD       Z27, Z31, Z27
	VPXORD       Z7, Z27, Z7
	VPRORD       $0x0c, Z7, Z7
	VPADDD       Z3, Z7, Z3
	VPADDD       Z3, Z11, Z3
	VPXORD       Z31, Z3, Z31
	VPRORD       $0x08, Z31, Z31
	VPADDD       Z27, Z31, Z27
	VPXORD       Z7, Z27, Z7
	VPRORD       $0x07, Z7, Z7
	VPADDD       Z0, Z5, Z0
	VPADDD       Z0, Z9, Z0
	VPXORD       Z31, Z0, Z31
	VPRORD       $0x10, Z31, Z31
	VPADDD       Z26, Z31, Z26
	VPXORD       Z5, Z26, Z5
	VPRORD       $0x0c, Z5, Z5
	VPADDD       Z0, Z5, Z0
	VPADDD       Z0, Z15, Z0
	VPXORD       Z31, Z0, Z31
	VPRORD       $0x08, Z31, Z31
	VPADDD       Z26, Z31, Z26
	VPXORD       Z5, Z26, Z5
	VPRORD       $0x07, Z5, Z5
	VPADDD       Z1, Z6, Z1
	VPADDD       Z1, Z18, Z1
	VPXORD       Z28, Z1, Z28
	VPRORD       $0x10, Z28, Z28
	VPADDD       Z27, Z28, Z27
	VPXORD       Z6, Z27, Z6
	VPRORD       $0x0c, Z6, Z6
	VPADDD       Z1, Z6, Z1
	VPADDD       Z1, Z23, Z1
	VPXORD       Z28, Z1, Z28
	VPRORD       $0x08, Z28, Z28
	VPADDD       Z27, Z28, Z27
	VPXORD       Z6, Z27, Z6
	VPRORD       $0x07, Z6, Z6
	VPADDD       Z2, Z7, Z2
	VPADDD       Z2, Z19, Z2
	VPXORD       Z29, Z2, Z29
	VPRORD       $0x10, Z29, Z29
	VPADDD       Z16, Z29, Z16
	VPXORD       Z7, Z16, Z7
	VPRORD       $0x0c, Z7, Z7
	VPADDD       Z2, Z7, Z2
	VPADDD       Z2, Z22, Z2
	VPXORD       Z29, Z2, Z29
	VPRORD       $0x08, Z29, Z29
	VPADDD       Z16, Z29, Z16
	VPXORD       Z7, Z16, Z7
	VPRORD       $0x07, Z7, Z7
	VPADDD       Z3, Z4, Z3
	VPADDD       Z3, Z13, Z3
	VPXORD       Z30, Z3, Z30
	VPRORD       $0x10, Z30, Z30
	VPADDD       Z25, Z30, Z25
	VPXORD       Z4, Z25, Z4
	VPRORD       $0x0c, Z4, Z4
	VPADDD       Z3, Z4, Z3
	VPADDD       Z3, Z24, Z3
	VPXORD       Z30, Z3, Z30
	VPRORD       $0x08, Z30, Z30
	VPADDD       Z25, Z30, Z25
	VPXORD       Z4, Z25, Z4
	VPRORD       $0x07, Z4, Z4
	VPXORD       Z0, Z16, Z0
	VPXORD       Z1, Z25, Z1
	VPXORD       Z2, Z26, Z2
	VPXORD       Z3, Z27, Z3
	VPXORD       Z4, Z28, Z4
	VPXORD       Z5, Z29, Z5
	VPXORD       Z6, Z30, Z6
	VPXORD       Z7, Z31, Z7
	DECB         R11
	JNZ          loopStart
	VSHUFPS      $0x44, Z1, Z0, Z8
	VSHUFPS      $0xee, Z1, Z0, Z0
	VSHUFPS      $0x44, Z3, Z2, Z1
	VSHUFPS      $0xee, Z3, Z2, Z2
	VSHUFPS      $0x44, Z5, Z4, Z3
	VSHUFPS      $0xee, Z5, Z4, Z4
	VSHUFPS      $0x44, Z7, Z6, Z5
	VSHUFPS      $0xee, Z7, Z6, Z6
	VSHUFPS      $0x88, Z1, Z8, Z7
	VSHUFPS      $0xdd, Z1, Z8, Z8
	VSHUFPS      $0x88, Z2, Z0, Z1
	VSHUFPS      $0xdd, Z2, Z0, Z0
	VSHUFPS      $0x88, Z5, Z3, Z2
	VSHUFPS      $0xdd, Z5, Z3, Z3
	VSHUFPS      $0x88, Z6, Z4, Z5
	VSHUFPS      $0xdd, Z6, Z4, Z4
	VSHUFI32X4   $0x44, Z2, Z7, Z6
	VSHUFI32X4   $0xee, Z2, Z7, Z7
	VSHUFI32X4   $0x44, Z3, Z8, Z2
	VSHUFI32X4   $0xee, Z3, Z8, Z8
	VSHUFI32X4   $0x44, Z5, Z1, Z3
	VSHUFI32X4   $0xee, Z5, Z1, Z1
	VSHUFI32X4   $0x44, Z4, Z0, Z5
	VSHUFI32X4   $0xee, Z4, Z0, Z0
	VSHUFI32X4   $0x88, Z2, Z6, Z4
	VSHUFI32X4   $0xdd, Z2, Z6, Z6
	VSHUFI32X4   $0x88, Z5, Z3, Z2
	VSHUFI32X4   $0xdd, Z5, Z3, Z3
	VSHUFI32X4   $0x88, Z8, Z7, Z5
	VSHUFI32X4   $0xdd, Z8, Z7, Z7
	VSHUFI32X4   $0x88, Z0, Z1, Z8
	VSHUFI32X4   $0xdd, Z0, Z1, Z1
	MOVQ         z+8(FP), AX
	MOVQ         (AX), CX
	VMOVDQU64    Y4, (CX)
	MOVQ         8(AX), CX
	VSHUFI32X4   $0xee, Z4, Z4, Z4
	VMOVDQU64    Y4, (CX)
	MOVQ         16(AX), CX
	VMOVDQU64    Y2, (CX)
	MOVQ         24(AX), CX
	VSHUFI32X4   $0xee, Z2, Z2, Z2
	VMOVDQU64    Y2, (CX)
	MOVQ         32(AX), CX
	VMOVDQU64    Y6, (CX)
	MOVQ         40(AX), CX
	VSHUFI32X4   $0xee, Z6, Z6, Z6
	VMOVDQU64    Y6, (CX)
	MOVQ         48(AX), CX
	VMOVDQU64    Y3, (CX)
	MOVQ         56(AX), CX
	VSHUFI32X4   $0xee, Z3, Z3, Z3
	VMOVDQU64    Y3, (CX)
	MOVQ         64(AX), CX
	VMOVDQU64    Y5, (CX)
	MOVQ         72(AX), CX
	VSHUFI32X4   $0xee, Z5, Z5, Z5
	VMOVDQU64    Y5, (CX)
	MOVQ         80(AX), CX
	VMOVDQU64    Y8, (CX)
	MOVQ         88(AX), CX
	VSHUFI32X4   $0xee, Z8, Z8, Z8
	VMOVDQU64    Y8, (CX)
	MOVQ         96(AX), CX
	VMOVDQU64    Y7, (CX)
	MOVQ         104(AX), CX
	VSHUFI32X4   $0xee, Z7, Z7, Z7
	VMOVDQU64    Y7, (CX)
	MOVQ         112(AX), CX
	VMOVDQU64    Y1, (CX)
	MOVQ         120(AX), CX
	VSHUFI32X4   $0xee, Z1, Z1, Z1
	VMOVDQU64    Y1, (CX)
	RET

// func Transpose8x16(x *uint32, z *uint32)
// Requires: AVX512F
TEXT ·Transpose8x16(SB), NOSPLIT, $0-16
	MOVQ       x+0(FP), AX
	VMOVDQU64  (AX), Z0
	VMOVDQU64  64(AX), Z1
	VMOVDQU64  128(AX), Z2
	VMOVDQU64  192(AX), Z3
	VMOVDQU64  256(AX), Z4
	VMOVDQU64  320(AX), Z5
	VMOVDQU64  384(AX), Z6
	VMOVDQU64  448(AX), Z7
	VSHUFPS    $0x44, Z1, Z0, Z8
	VSHUFPS    $0xee, Z1, Z0, Z0
	VSHUFPS    $0x44, Z3, Z2, Z1
	VSHUFPS    $0xee, Z3, Z2, Z2
	VSHUFPS    $0x44, Z5, Z4, Z3
	VSHUFPS    $0xee, Z5, Z4, Z4
	VSHUFPS    $0x44, Z7, Z6, Z5
	VSHUFPS    $0xee, Z7, Z6, Z6
	VSHUFPS    $0x88, Z1, Z8, Z7
	VSHUFPS    $0xdd, Z1, Z8, Z8
	VSHUFPS    $0x88, Z2, Z0, Z1
	VSHUFPS    $0xdd, Z2, Z0, Z0
	VSHUFPS    $0x88, Z5, Z3, Z2
	VSHUFPS    $0xdd, Z5, Z3, Z3
	VSHUFPS    $0x88, Z6, Z4, Z5
	VSHUFPS    $0xdd, Z6, Z4, Z4
	VSHUFI32X4 $0x44, Z2, Z7, Z6
	VSHUFI32X4 $0xee, Z2, Z7, Z7
	VSHUFI32X4 $0x44, Z3, Z8, Z2
	VSHUFI32X4 $0xee, Z3, Z8, Z8
	VSHUFI32X4 $0x44, Z5, Z1, Z3
	VSHUFI32X4 $0xee, Z5, Z1, Z1
	VSHUFI32X4 $0x44, Z4, Z0, Z5
	VSHUFI32X4 $0xee, Z4, Z0, Z0
	VSHUFI32X4 $0x88, Z2, Z6, Z4
	VSHUFI32X4 $0xdd, Z2, Z6, Z6
	VSHUFI32X4 $0x88, Z5, Z3, Z2
	VSHUFI32X4 $0xdd, Z5, Z3, Z3
	VSHUFI32X4 $0x88, Z8, Z7, Z5
	VSHUFI32X4 $0xdd, Z8, Z7, Z7
	VSHUFI32X4 $0x88, Z0, Z1, Z8
	VSHUFI32X4 $0xdd, Z0, Z1, Z1
	MOVQ       z+8(FP), AX
	VMOVDQU64  Z4, (AX)
	VMOVDQU64  Z2, 64(AX)
	VMOVDQU64  Z6, 128(AX)
	VMOVDQU64  Z3, 192(AX)
	VMOVDQU64  Z5, 256(AX)
	VMOVDQU64  Z8, 320(AX)
	VMOVDQU64  Z7, 384(AX)
	VMOVDQU64  Z1, 448(AX)
	RET

// func Transpose16x16(x *uint32, z *uint32)
// Requires: AVX512F
TEXT ·Transpose16x16(SB), NOSPLIT, $0-16
	MOVQ       x+0(FP), AX
	VMOVDQU64  (AX), Z0
	VMOVDQU64  64(AX), Z1
	VMOVDQU64  128(AX), Z2
	VMOVDQU64  192(AX), Z3
	VMOVDQU64  256(AX), Z4
	VMOVDQU64  320(AX), Z5
	VMOVDQU64  384(AX), Z6
	VMOVDQU64  448(AX), Z7
	VMOVDQU64  512(AX), Z8
	VMOVDQU64  576(AX), Z9
	VMOVDQU64  640(AX), Z10
	VMOVDQU64  704(AX), Z11
	VMOVDQU64  768(AX), Z12
	VMOVDQU64  832(AX), Z13
	VMOVDQU64  896(AX), Z14
	VMOVDQU64  960(AX), Z15
	VSHUFPS    $0x44, Z1, Z0, Z16
	VSHUFPS    $0xee, Z1, Z0, Z0
	VSHUFPS    $0x44, Z3, Z2, Z1
	VSHUFPS    $0xee, Z3, Z2, Z2
	VSHUFPS    $0x44, Z5, Z4, Z3
	VSHUFPS    $0xee, Z5, Z4, Z4
	VSHUFPS    $0x44, Z7, Z6, Z5
	VSHUFPS    $0xee, Z7, Z6, Z6
	VSHUFPS    $0x44, Z9, Z8, Z7
	VSHUFPS    $0xee, Z9, Z8, Z8
	VSHUFPS    $0x44, Z11, Z10, Z9
	VSHUFPS    $0xee, Z11, Z10, Z10
	VSHUFPS    $0x44, Z13, Z12, Z11
	VSHUFPS    $0xee, Z13, Z12, Z12
	VSHUFPS    $0x44, Z15, Z14, Z13
	VSHUFPS    $0xee, Z15, Z14, Z14
	VSHUFPS    $0x88, Z1, Z16, Z15
	VSHUFPS    $0xdd, Z1, Z16, Z16
	VSHUFPS    $0x88, Z2, Z0, Z1
	VSHUFPS    $0xdd, Z2, Z0, Z0
	VSHUFPS    $0x88, Z5, Z3, Z2
	VSHUFPS    $0xdd, Z5, Z3, Z3
	VSHUFPS    $0x88, Z6, Z4, Z5
	VSHUFPS    $0xdd, Z6, Z4, Z4
	VSHUFPS    $0x88, Z9, Z7, Z6
	VSHUFPS    $0xdd, Z9, Z7, Z7
	VSHUFPS    $0x88, Z10, Z8, Z9
	VSHUFPS    $0xdd, Z10, Z8, Z8
	VSHUFPS    $0x88, Z13, Z11, Z10
	VSHUFPS    $0xdd, Z13, Z11, Z11
	VSHUFPS    $0x88, Z14, Z12, Z13
	VSHUFPS    $0xdd, Z14, Z12, Z12
	VSHUFI32X4 $0x44, Z2, Z15, Z14
	VSHUFI32X4 $0xee, Z2, Z15, Z15
	VSHUFI32X4 $0x44, Z3, Z16, Z2
	VSHUFI32X4 $0xee, Z3, Z16, Z16
	VSHUFI32X4 $0x44, Z5, Z1, Z3
	VSHUFI32X4 $0xee, Z5, Z1, Z1
	VSHUFI32X4 $0x44, Z4, Z0, Z5
	VSHUFI32X4 $0xee, Z4, Z0, Z0
	VSHUFI32X4 $0x44, Z10, Z6, Z4
	VSHUFI32X4 $0xee, Z10, Z6, Z6
	VSHUFI32X4 $0x44, Z11, Z7, Z10
	VSHUFI32X4 $0xee, Z11, Z7, Z7
	VSHUFI32X4 $0x44, Z13, Z9, Z11
	VSHUFI32X4 $0xee, Z13, Z9, Z9
	VSHUFI32X4 $0x44, Z12, Z8, Z13
	VSHUFI32X4 $0xee, Z12, Z8, Z8
	VSHUFI32X4 $0x88, Z4, Z14, Z12
	VSHUFI32X4 $0xdd, Z4, Z14, Z14
	VSHUFI32X4 $0x88, Z10, Z2, Z4
	VSHUFI32X4 $0xdd, Z10, Z2, Z2
	VSHUFI32X4 $0x88, Z11, Z3, Z10
	VSHUFI32X4 $0xdd, Z11, Z3, Z3
	VSHUFI32X4 $0x88, Z13, Z5, Z11
	VSHUFI32X4 $0xdd, Z13, Z5, Z5
	VSHUFI32X4 $0x88, Z6, Z15, Z13
	VSHUFI32X4 $0xdd, Z6, Z15, Z15
	VSHUFI32X4 $0x88, Z7, Z16, Z6
	VSHUFI32X4 $0xdd, Z7, Z16, Z16
	VSHUFI32X4 $0x88, Z9, Z1, Z7
	VSHUFI32X4 $0xdd, Z9, Z1, Z1
	VSHUFI32X4 $0x88, Z8, Z0, Z9
	VSHUFI32X4 $0xdd, Z8, Z0, Z0
	MOVQ       z+8(FP), AX
	VMOVDQU64  Z12, (AX)
	VMOVDQU64  Z4, 64(AX)
	VMOVDQU64  Z10, 128(AX)
	VMOVDQU64  Z11, 192(AX)
	VMOVDQU64  Z14, 256(AX)
	VMOVDQU64  Z2, 320(AX)
	VMOVDQU64  Z3, 384(AX)
	VMOVDQU64  Z5, 448(AX)
	VMOVDQU64  Z13, 512(AX)
	VMOVDQU64  Z6, 576(AX)
	VMOVDQU64  Z7, 640(AX)
	VMOVDQU64  Z9, 704(AX)
	VMOVDQU64  Z15, 768(AX)
	VMOVDQU64  Z16, 832(AX)
	VMOVDQU64  Z1, 896(AX)
	VMOVDQU64  Z0, 960(AX)
	RET

// func G(a *[16]uint32, b *[16]uint32, c *[16]uint32, d *[16]uint32, mx *[16]uint32, my *[16]uint32)
// Requires: AVX512F
TEXT ·G(SB), NOSPLIT, $0-48
	MOVQ      a+0(FP), AX
	MOVQ      b+8(FP), CX
	MOVQ      c+16(FP), DX
	MOVQ      d+24(FP), BX
	VMOVDQU64 (AX), Z0
	VMOVDQU64 (CX), Z1
	VMOVDQU64 (DX), Z2
	VMOVDQU64 (BX), Z3
	MOVQ      mx+32(FP), SI
	VMOVDQU64 (SI), Z4
	MOVQ      my+40(FP), SI
	VMOVDQU64 (SI), Z5
	VPADDD    Z0, Z1, Z0
	VPADDD    Z0, Z4, Z0
	VPXORD    Z3, Z0, Z3
	VPRORD    $0x10, Z3, Z3
	VPADDD    Z2, Z3, Z2
	VPXORD    Z1, Z2, Z1
	VPRORD    $0x0c, Z1, Z1
	VPADDD    Z0, Z1, Z0
	VPADDD    Z0, Z5, Z0
	VPXORD    Z3, Z0, Z3
	VPRORD    $0x08, Z3, Z3
	VPADDD    Z2, Z3, Z2
	VPXORD    Z1, Z2, Z1
	VPRORD    $0x07, Z1, Z1
	VMOVDQU64 Z0, (AX)
	VMOVDQU64 Z1, (CX)
	VMOVDQU64 Z2, (DX)
	VMOVDQU64 Z3, (BX)
	RET
