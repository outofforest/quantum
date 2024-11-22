// Code generated by command: go run avo.go -out ../asm.s -stubs ../asm_stub.go -pkg wal. DO NOT EDIT.

#include "textflag.h"

// func Copy(x *byte, y *byte, z *byte)
// Requires: AVX512F
TEXT ·Copy(SB), NOSPLIT, $0-24
	MOVQ      z+16(FP), AX
	MOVQ      x+0(FP), CX
	MOVQ      y+8(FP), DX
	VMOVDQU64 (AX), Z0
	VMOVDQU64 Z0, (CX)
	VMOVDQU64 Z0, (DX)
	ADDQ      $0x40, AX
	ADDQ      $0x40, CX
	ADDQ      $0x40, DX
	VMOVDQU64 (AX), Z0
	VMOVDQU64 Z0, (CX)
	VMOVDQU64 Z0, (DX)
	ADDQ      $0x40, AX
	ADDQ      $0x40, CX
	ADDQ      $0x40, DX
	VMOVDQU64 (AX), Z0
	VMOVDQU64 Z0, (CX)
	VMOVDQU64 Z0, (DX)
	ADDQ      $0x40, AX
	ADDQ      $0x40, CX
	ADDQ      $0x40, DX
	VMOVDQU64 (AX), Z0
	VMOVDQU64 Z0, (CX)
	VMOVDQU64 Z0, (DX)
	RET
