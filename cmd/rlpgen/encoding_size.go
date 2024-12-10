package main

import (
	"bytes"
	"fmt"
	"go/types"
)

func matchTypeToEncodingSize(b *bytes.Buffer, baseTyp types.Type, fieldName, tags string) {

	// TODO(racytech): see if we can use tags for optional, conditional and other types of encoding

	_, ok := baseTyp.(*types.Struct)
	if ok { // if it's a struct
		// TODO(racytech): think about a ways to handle nested structs, for now leave it uninmplemented
		// hints: 1. nested structs should have EncodingSize pre-generated, just add
		//			 `size += nestedStruct.EncodingSize()` to the buffer
		//		  2. create recursive function that goes over the each field in the nested struct and calculates the
		//			 size of the struct (unlikely)
		panic("nested struct unimplemented")
	} else {
		if ptyp, ok := baseTyp.(*types.Pointer); ok { // check for pointer first
			pointerTypeEncodingSize(b, ptyp, fieldName)
		} else if btyp, ok := baseTyp.(*types.Basic); ok {
			basicTypeEncodingSize(b, btyp, fieldName)
		} else if styp, ok := baseTyp.(*types.Slice); ok {
			sliceTypeEncodingSize(b, styp, fieldName)
		} else if atyp, ok := baseTyp.(*types.Array); ok {
			arrayTypeEncodingSize(b, atyp, fieldName)
		} else {
			msg := fmt.Sprintf("unimplemented encodingSize for: %s\n", baseTyp.String())
			panic(msg)
		}
	}
}

func pointerTypeEncodingSize(b *bytes.Buffer, typ *types.Pointer, fieldName string) {
	panic("unimplemented pointer type")
}
func basicTypeEncodingSize(b *bytes.Buffer, typ *types.Basic, fieldName string) {
	fmt.Fprintf(b, "    size += ")
	// fmt.Println("info: ", typ.Info())
	switch typ.Info() {
	case types.IsBoolean:
		fmt.Println("unimplemented: add EcnodeBool function to RLP package")
	case types.IsInteger:
		fmt.Fprintf(b, "rlp.IntLenExcludingHead(uint64(obj.%s)) + 1\n", fieldName)
	case types.IsInteger | types.IsUnsigned:
		fmt.Fprintf(b, "rlp.IntLenExcludingHead(obj.%s) + 1\n", fieldName)
	default:
		msg := fmt.Sprintf("unimplemented info: %d\n", typ.Info())
		panic(msg)
	}

}
func sliceTypeEncodingSize(b *bytes.Buffer, typ *types.Slice, fieldName string) {
	requiresRLP2 = true
	fmt.Fprintf(b, "    size += rlp2.StringLen(obj.%s)\n", fieldName)
}
func arrayTypeEncodingSize(b *bytes.Buffer, typ *types.Array, fieldName string) {
	fmt.Fprintf(b, "    size += ")
	switch typ.Len() {
	case 8: // Nonce
		fmt.Fprintf(b, "9\n")
	case 20: // Address
		fmt.Fprintf(b, "21\n")
	case 32: // Hash
		fmt.Fprintf(b, "33\n")
	case 256: // Bloom
		fmt.Fprintf(b, "259\n")
	default:
		requiresRLP2 = true
		fmt.Fprintf(b, "rlp2.StringLen(obj.%s[:])\n", fieldName)
	}
}
