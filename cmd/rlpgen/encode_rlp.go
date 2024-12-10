package main

import (
	"bytes"
	"fmt"
	"go/types"
)

const singleByteWrite = `    if _, err := w.Write(b[:1]); err != nil {
	return err
}
`

func matchTypeToRlpEncoding(b *bytes.Buffer, baseTyp types.Type, fieldName, tags string) {

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
			pointerTypeEncoding(b, ptyp, fieldName)
		} else if btyp, ok := baseTyp.(*types.Basic); ok {
			basicTypeEncoding(b, btyp, fieldName)
		} else if styp, ok := baseTyp.(*types.Slice); ok {
			sliceTypeEncoding(b, styp, fieldName)
		} else if atyp, ok := baseTyp.(*types.Array); ok {
			arrayTypeEncoding(b, atyp, fieldName)
		} else {
			panic(baseTyp.String())
		}
	}
}

func pointerTypeEncoding(b *bytes.Buffer, typ *types.Pointer, fieldName string) {
	fmt.Println(typ.String())
}
func basicTypeEncoding(b *bytes.Buffer, typ *types.Basic, fieldName string) {
	switch typ.Info() {
	case types.IsBoolean:
		fmt.Println("unimplemented: add EcnodeBool function to RLP package")
	case types.IsInteger:
		fmt.Fprintf(b, "    if err := rlp.EncodeInt(uint64(obj.%s), w, b[:]); err != nil {\n", fieldName)
		fmt.Fprintf(b, "        return err\n")
		fmt.Fprintf(b, "    }\n")
	case types.IsInteger | types.IsUnsigned:
		fmt.Fprintf(b, "    if err := rlp.EncodeInt(obj.%s, w, b[:]); err != nil {\n", fieldName)
		fmt.Fprintf(b, "        return err\n")
		fmt.Fprintf(b, "    }\n")
	}

}
func sliceTypeEncoding(b *bytes.Buffer, typ *types.Slice, fieldName string) {
	fmt.Fprintf(b, "    if err := rlp.EncodeString(obj.%s); err != nil {\n", fieldName)
	fmt.Fprintf(b, "        return err\n")
	fmt.Fprintf(b, "    }\n")
}
func arrayTypeEncoding(b *bytes.Buffer, typ *types.Array, fieldName string) {
	switch typ.Len() {
	case 8: // Nonce
		fmt.Fprintf(b, "    b[0] = 128 + 8\n")
		fmt.Fprint(b, singleByteWrite)
	case 20: // Address
		fmt.Fprintf(b, "    b[0] = 128 + 20\n")
		fmt.Fprint(b, singleByteWrite)
	case 32: // Hash
		fmt.Fprintf(b, "    b[0] = 128 + 32\n")
		fmt.Fprint(b, singleByteWrite)
	case 256: // Bloom
		fmt.Fprintf(b, "    b[0] = 183 + 3\n")
		fmt.Fprintf(b, "    b[1] = 1")
		fmt.Fprintf(b, "    b[2] = 0")
		fmt.Fprintf(b, "    if _, err := w.Write(b[:3]); err != nil {\n")
		fmt.Fprintf(b, "        return err\n")
		fmt.Fprintf(b, "    }\n")
	default:
		fmt.Fprintf(b, "    if err := rlp.EncodeStringSizePrefix(len(obj.%s)); err != nil {\n", fieldName)
		fmt.Fprintf(b, "        return err\n")
		fmt.Fprintf(b, "    }\n")
	}
	fmt.Fprintf(b, "    if _, err := w.Write(obj.%s[:]); err != nil {\n", fieldName)
	fmt.Fprintf(b, "        return err\n")
	fmt.Fprintf(b, "    }\n")
}
