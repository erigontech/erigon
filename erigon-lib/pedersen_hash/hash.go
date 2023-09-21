//go:build linux
// +build linux

package hash

/*
#cgo CXXFLAGS: -std=c++17
#include <stdlib.h>
#include "hash.h"
*/
import "C"

import (
	"encoding/hex"
	"fmt"
)

func reverseHexEndianRepresentation(s string) string {
	rns := []rune(s)
	for i, j := 0, len(rns)-2; i < j; i, j = i+2, j-2 {
		rns[i], rns[j] = rns[j], rns[i]
		rns[i+1], rns[j+1] = rns[j+1], rns[i+1]
	}
	return string(rns)
}

func Hash(input1, input2 string) (string, error) {
	input1Dec, _ := hex.DecodeString(reverseHexEndianRepresentation(input1))
	input2Dec, _ := hex.DecodeString(reverseHexEndianRepresentation(input2))
	in1 := C.CBytes(input1Dec)
	in2 := C.CBytes(input2Dec)
	var o [1024]byte
	out := C.CBytes(o[:])
	upIn1 := in1
	upIn2 := in2
	upOut := out
	defer func() {
		C.free(upIn1)
		C.free(upIn2)
		C.free(upOut)
	}()
	res := C.CHash(
		(*C.char)(upIn1),
		(*C.char)(upIn2),
		(*C.char)(upOut))
	if res != 0 {
		return "", fmt.Errorf("Pedersen hash encountered an error: %s\n", C.GoBytes(out, 1024))
	}

	hashResult := "0x" + reverseHexEndianRepresentation(
		hex.EncodeToString(C.GoBytes(out, 32)))

	return hashResult, nil
}
