// Copyright 2025 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"bytes"
	"fmt"
	"go/types"
)

var decodeBufAdded bool
var intSizeAdded bool   // for encoding size
var intEncodeAdded bool // for rlp encoding

// create decoder buffer if not added yet
func addDecodeBuf(b *bytes.Buffer) {
	if !decodeBufAdded {
		fmt.Fprint(b, "    var b []byte\n")
		decodeBufAdded = true
	}
}

// add List start check
func startListDecode(b *bytes.Buffer, fieldName string) {
	fmt.Fprintf(b, "    _, err = s.List()\n")
	fmt.Fprintf(b, "    if err != nil {\n")
	fmt.Fprintf(b, "        return fmt.Errorf(\"error decoding field %s - expected list start, err: %%w\", err)\n", fieldName)
	fmt.Fprintf(b, "    }\n")
}

// add List end check
func endListDecode(b *bytes.Buffer, fieldName string) {
	fmt.Fprintf(b, "    if err = s.ListEnd(); err != nil {\n")
	fmt.Fprintf(b, "        return fmt.Errorf(\"error decoding field %s - fail to close list, err: %%w\", err)\n", fieldName)
	fmt.Fprintf(b, "    }\n")
}

// add reusable int for encoding size usage
func addIntSize(b *bytes.Buffer) {
	if !intSizeAdded {
		fmt.Fprint(b, "    gidx := 0\n")
		intSizeAdded = true
	} else {
		fmt.Fprint(b, "    gidx = 0\n")
	}
}

// add reusable int for encoding usage
func addIntEncode(b *bytes.Buffer) {
	if !intEncodeAdded {
		fmt.Fprint(b, "    gidx := 0\n")
		intEncodeAdded = true
	} else {
		fmt.Fprint(b, "    gidx = 0\n")
	}
}

func decodeErrorMsg(filedName string) string {
	return fmt.Sprintf("return fmt.Errorf(\"error decoding field %s, err: %%w\", err)", filedName)
}

func decodeLenMismatch(want int) string {
	return fmt.Sprintf("return fmt.Errorf(\"error decoded length mismatch, expected: %d, got: %%d\", len(b))", want)
}

// 1. add package to imports if the to-be encoded field is not in the same package
// e.g do not import "github.com/erigontech/erigon/execution/types" if the field is types.BlockNonce
func addToImports(named *types.Named) (typ string) {
	if named.Obj().Pkg().Name() != pkgSrc.Name() {
		_imports[named.Obj().Pkg().Path()] = true
		typ = named.Obj().Pkg().Name() + "." + named.Obj().Name()
	} else {
		typ = named.Obj().Name()
	}
	return
}

func uint64CastTo(kind types.BasicKind) string {
	var cast string
	switch kind {
	case types.Int16:
		cast = "int16"
	case types.Int32:
		cast = "int32"
	case types.Int:
		cast = "int"
	case types.Int64:
		cast = "int64"
	case types.Uint16:
		cast = "uint16"
	case types.Uint32:
		cast = "uint32"
	case types.Uint:
		cast = "uint"
	case types.Uint64:
		return "i := n"
	default:
		panic(fmt.Sprintf("unhandled basic kind: %d", kind))
	}
	return fmt.Sprintf("i := %s(n)", cast)
}

func uintHandle(b1, b2, b3 *bytes.Buffer, fieldType types.Type, fieldName string) {
	var kind types.BasicKind
	if basic, ok := fieldType.(*types.Basic); !ok {
		_exit("uintHandle: expected fieldType to be Basic")
	} else {
		kind = basic.Kind()
	}

	// size
	fmt.Fprintf(b1, "    size += rlp.IntLenExcludingHead(uint64(obj.%s)) + 1\n", fieldName)

	// encode
	fmt.Fprintf(b2, "    if err := rlp.EncodeInt(uint64(obj.%s), w, b[:]); err != nil {\n", fieldName)
	fmt.Fprintf(b2, "        return err\n")
	fmt.Fprintf(b2, "    }\n")

	// decode
	if kind != types.Uint64 {
		cast := uint64CastTo(kind)
		fmt.Fprintf(b3, "    if n, err := s.Uint(); err != nil {\n")
		fmt.Fprintf(b3, "        %s\n", decodeErrorMsg(fieldName))
		fmt.Fprintf(b3, "    } else {\n")
		fmt.Fprintf(b3, "        %s\n", cast)
		fmt.Fprintf(b3, "        obj.%s = i\n", fieldName)
		fmt.Fprintf(b3, "    }\n")
	} else {
		fmt.Fprintf(b3, "    if obj.%s, err = s.Uint(); err != nil {\n", fieldName)
		fmt.Fprintf(b3, "        %s\n", decodeErrorMsg(fieldName))
		fmt.Fprintf(b3, "    }\n")
	}
}

func uintPtrHandle(b1, b2, b3 *bytes.Buffer, fieldType types.Type, fieldName string) {
	var kind types.BasicKind
	if ptr, ok := fieldType.(*types.Pointer); !ok {
		_exit("uintPtrHandle: expected fieldType to be Pointer")
	} else {
		if basic, ok := ptr.Elem().(*types.Basic); !ok {
			_exit("uintPtrHandle: expected fieldType to be Pointer Basic")
		} else {
			kind = basic.Kind()
		}
	}

	// size
	fmt.Fprintf(b1, "    if obj.%s != nil {\n", fieldName)
	fmt.Fprintf(b1, "        size += rlp.IntLenExcludingHead(uint64(*obj.%s)) + 1\n", fieldName)
	fmt.Fprintf(b1, "    }\n")

	// encode
	fmt.Fprintf(b2, "    if obj.%s != nil {\n", fieldName)
	fmt.Fprintf(b2, "        if err := rlp.EncodeInt(uint64(*obj.%s), w, b[:]); err != nil {\n", fieldName)
	fmt.Fprintf(b2, "            return err\n")
	fmt.Fprintf(b2, "        }\n")
	fmt.Fprintf(b2, "    }\n")

	// decode
	cast := uint64CastTo(kind)
	fmt.Fprintf(b3, "    if n, err := s.Uint(); err != nil {\n")
	fmt.Fprintf(b3, "        %s\n", decodeErrorMsg(fieldName))
	fmt.Fprintf(b3, "    } else {\n")
	fmt.Fprintf(b3, "        %s\n", cast)
	fmt.Fprintf(b3, "        obj.%s = &i\n", fieldName)
	fmt.Fprintf(b3, "    }\n")
}

func bigIntHandle(b1, b2, b3 *bytes.Buffer, fieldType types.Type, fieldName string) {
	if named, ok := fieldType.(*types.Named); !ok {
		_exit("bigIntHandle: expected filedType to be Named")
	} else {
		_ = addToImports(named)
	}
	// size
	fmt.Fprintf(b1, "    size += rlp.BigIntLenExcludingHead(&obj.%s) + 1\n", fieldName)

	// encode
	fmt.Fprintf(b2, "    if err := rlp.EncodeBigInt(&obj.%s, w, b[:]); err != nil {\n", fieldName)
	fmt.Fprintf(b2, "        return err\n")
	fmt.Fprintf(b2, "    }\n")

	// decode
	addDecodeBuf(b3)
	fmt.Fprintf(b3, "    if b, err = s.Uint256Bytes(); err != nil {\n")
	fmt.Fprintf(b3, "        %s\n", decodeErrorMsg(fieldName))
	fmt.Fprintf(b3, "    }\n")
	fmt.Fprintf(b3, "    obj.%s = *(new(big.Int).SetBytes(b))\n", fieldName)
}

func bigIntPtrHandle(b1, b2, b3 *bytes.Buffer, fieldType types.Type, fieldName string) {
	if ptr, ok := fieldType.(*types.Pointer); !ok {
		_exit("bigIntPtrHandle: expected fieldType to be Pointer")
	} else {
		if named, ok := ptr.Elem().(*types.Named); !ok {
			_exit("bigIntPtrHandle: expected filedType to be Pointer Named")
		} else {
			_ = addToImports(named)
		}
	}

	// size
	fmt.Fprintf(b1, "    size += 1\n")
	fmt.Fprintf(b1, "    if obj.%s != nil {\n", fieldName)
	fmt.Fprintf(b1, "        size += rlp.BigIntLenExcludingHead(obj.%s)\n", fieldName)
	fmt.Fprintf(b1, "    }\n")

	// encode
	fmt.Fprintf(b2, "    if err := rlp.EncodeBigInt(obj.%s, w, b[:]); err != nil {\n", fieldName)
	fmt.Fprintf(b2, "        return err\n")
	fmt.Fprintf(b2, "    }\n")

	// decode
	addDecodeBuf(b3)
	fmt.Fprintf(b3, "    if b, err = s.Uint256Bytes(); err != nil {\n")
	fmt.Fprintf(b3, "        %s\n", decodeErrorMsg(fieldName))
	fmt.Fprintf(b3, "    }\n")
	fmt.Fprintf(b3, "    obj.%s = new(big.Int).SetBytes(b)\n", fieldName)
}

func uint256Handle(b1, b2, b3 *bytes.Buffer, fieldType types.Type, fieldName string) {
	if named, ok := fieldType.(*types.Named); !ok {
		_exit("uint256Handle: expected filedType to be Named")
	} else {
		_ = addToImports(named)
	}

	// size
	fmt.Fprintf(b1, "    size += rlp.Uint256LenExcludingHead(&obj.%s) + 1\n", fieldName)

	// encode
	fmt.Fprintf(b2, "    if err := rlp.EncodeUint256(&obj.%s, w, b[:]); err != nil {\n", fieldName)
	fmt.Fprintf(b2, "        return err\n")
	fmt.Fprintf(b2, "    }\n")

	// decode
	addDecodeBuf(b3)
	fmt.Fprintf(b3, "    if b, err = s.Uint256Bytes(); err != nil {\n")
	fmt.Fprintf(b3, "        %s\n", decodeErrorMsg(fieldName))
	fmt.Fprintf(b3, "    }\n")
	fmt.Fprintf(b3, "    obj.%s = *(new(uint256.Int).SetBytes(b))\n", fieldName)
}

func uint256PtrHandle(b1, b2, b3 *bytes.Buffer, fieldType types.Type, fieldName string) {
	if ptr, ok := fieldType.(*types.Pointer); !ok {
		_exit("uint256PtrHandle: expected fieldType to be Pointer")
	} else {
		if named, ok := ptr.Elem().(*types.Named); !ok {
			_exit("uint256PtrHandle: expected filedType to be Pointer Named")
		} else {
			_ = addToImports(named)
		}
	}

	// size
	fmt.Fprintf(b1, "    size += rlp.Uint256LenExcludingHead(obj.%s) + 1\n", fieldName)

	// encode
	fmt.Fprintf(b2, "    if err := rlp.EncodeUint256(obj.%s, w, b[:]); err != nil {\n", fieldName)
	fmt.Fprintf(b2, "        return err\n")
	fmt.Fprintf(b2, "    }\n")

	// decode
	addDecodeBuf(b3)
	fmt.Fprintf(b3, "    if b, err = s.Uint256Bytes(); err != nil {\n")
	fmt.Fprintf(b3, "        %s\n", decodeErrorMsg(fieldName))
	fmt.Fprintf(b3, "    }\n")
	fmt.Fprintf(b3, "    obj.%s = new(uint256.Int).SetBytes(b)\n", fieldName)
}

func _shortArrayHandle(b1, b2, b3 *bytes.Buffer, fieldName string, size int) { // TODO change the name
	// arr sizes < 56

	// size
	fmt.Fprintf(b1, "    size += %d + 1\n", size)

	// encode
	fmt.Fprintf(b2, "    b[0] = 128 + %d\n", size)
	fmt.Fprintf(b2, "    if _, err := w.Write(b[:1]); err != nil {\n")
	fmt.Fprintf(b2, "        return err\n")
	fmt.Fprintf(b2, "    }\n")
	fmt.Fprintf(b2, "    if _, err := w.Write(obj.%s[:]); err != nil {\n", fieldName)
	fmt.Fprintf(b2, "        return err\n")
	fmt.Fprintf(b2, "    }\n")

	// decode
	addDecodeBuf(b3)
	fmt.Fprintf(b3, "    if b, err = s.Bytes(); err != nil {\n")
	fmt.Fprintf(b3, "        %s\n", decodeErrorMsg(fieldName))
	fmt.Fprintf(b3, "    }\n")
	fmt.Fprintf(b3, "    if len(b) > 0 && len(b) != %d {\n", size)
	fmt.Fprintf(b3, "        %s\n", decodeLenMismatch(size))
	fmt.Fprintf(b3, "    }\n")
	fmt.Fprintf(b3, "    copy(obj.%s[:], b)\n", fieldName)
}

func _shortArrayPtrHandle(b1, b2, b3 *bytes.Buffer, fieldType types.Type, fieldName string, size int) error {
	// arr sizes < 56
	var typ string
	if ptr, ok := fieldType.(*types.Pointer); !ok {
		_exit("_shortArrayPtrHandle: expected fieldType to be Pointer")
	} else {
		if named, ok := ptr.Elem().(*types.Named); !ok {
			_exit("_shortArrayPtrHandle: expected filedType to be Pointer Named")
		} else {
			typ = addToImports(named)
		}
	}

	// size
	fmt.Fprintf(b1, "    size += 1\n")
	fmt.Fprintf(b1, "    if obj.%s != nil {\n", fieldName)
	fmt.Fprintf(b1, "        size += %d\n", size)
	fmt.Fprintf(b1, "    }\n")

	// encode
	fmt.Fprintf(b2, "    if obj.%s != nil {\n", fieldName)
	fmt.Fprintf(b2, "        b[0] = 128 + %d\n", size)
	fmt.Fprintf(b2, "    } else {\n")
	fmt.Fprintf(b2, "        b[0] = 128\n")
	fmt.Fprintf(b2, "    }\n")
	fmt.Fprintf(b2, "    if _, err := w.Write(b[:1]); err != nil {\n")
	fmt.Fprintf(b2, "        return err\n")
	fmt.Fprintf(b2, "    }\n")
	fmt.Fprintf(b2, "    if obj.%s != nil {\n", fieldName)
	fmt.Fprintf(b2, "        if _, err := w.Write(obj.%s[:]); err != nil {\n", fieldName)
	fmt.Fprintf(b2, "            return err\n")
	fmt.Fprintf(b2, "        }\n")
	fmt.Fprintf(b2, "    }\n")

	// decode
	addDecodeBuf(b3)
	fmt.Fprintf(b3, "    if b, err = s.Bytes(); err != nil {\n")
	fmt.Fprintf(b3, "        %s\n", decodeErrorMsg(fieldName))
	fmt.Fprintf(b3, "    }\n")
	fmt.Fprintf(b3, "    if len(b) > 0 && len(b) != %d {\n", size)
	fmt.Fprintf(b3, "        %s\n", decodeLenMismatch(size))
	fmt.Fprintf(b3, "    }\n")
	fmt.Fprintf(b3, "    obj.%s = &%s{}\n", fieldName, typ)
	fmt.Fprintf(b3, "    copy((*obj.%s)[:], b)\n", fieldName)

	return nil
}

func blockNonceHandle(b1, b2, b3 *bytes.Buffer, _ types.Type, fieldName string) {
	_shortArrayHandle(b1, b2, b3, fieldName, 8)
}

func blockNoncePtrHandle(b1, b2, b3 *bytes.Buffer, fieldType types.Type, fieldName string) {
	_shortArrayPtrHandle(b1, b2, b3, fieldType, fieldName, 8)
}

func addressHandle(b1, b2, b3 *bytes.Buffer, _ types.Type, fieldName string) {
	_shortArrayHandle(b1, b2, b3, fieldName, 20)
}

func addressPtrHandle(b1, b2, b3 *bytes.Buffer, fieldType types.Type, fieldName string) {
	_shortArrayPtrHandle(b1, b2, b3, fieldType, fieldName, 20)
}

func hashHandle(b1, b2, b3 *bytes.Buffer, _ types.Type, fieldName string) {
	_shortArrayHandle(b1, b2, b3, fieldName, 32)
}

func hashPtrHandle(b1, b2, b3 *bytes.Buffer, fieldType types.Type, fieldName string) {
	_shortArrayPtrHandle(b1, b2, b3, fieldType, fieldName, 32)
}

func bloomHandle(b1, b2, b3 *bytes.Buffer, _ types.Type, fieldName string) {
	// size
	fmt.Fprintf(b1, "    size += 259\n")

	// encode
	fmt.Fprintf(b2, "    b[0] = 183 + 2\n")
	fmt.Fprintf(b2, "    b[1] = 1\n")
	fmt.Fprintf(b2, "    b[2] = 0\n")
	fmt.Fprintf(b2, "    if _, err := w.Write(b[:3]); err != nil {\n")
	fmt.Fprintf(b2, "        return err\n")
	fmt.Fprintf(b2, "    }\n")
	fmt.Fprintf(b2, "    if _, err := w.Write(obj.%s[:]); err != nil {\n", fieldName)
	fmt.Fprintf(b2, "        return err\n")
	fmt.Fprintf(b2, "    }\n")

	// decode
	addDecodeBuf(b3)
	fmt.Fprintf(b3, "    if b, err = s.Bytes(); err != nil {\n")
	fmt.Fprintf(b3, "        %s\n", decodeErrorMsg(fieldName))
	fmt.Fprintf(b3, "    }\n")
	fmt.Fprintf(b3, "    if len(b) > 0 && len(b) != 256 {\n")
	fmt.Fprintf(b3, "        %s\n", decodeLenMismatch(256))
	fmt.Fprintf(b3, "    }\n")
	fmt.Fprintf(b3, "    copy(obj.%s[:], b)\n", fieldName)
}

func bloomPtrHandle(b1, b2, b3 *bytes.Buffer, fieldType types.Type, fieldName string) {
	var typ string
	if ptr, ok := fieldType.(*types.Pointer); !ok {
		_exit("bloomPtrHandle: expected fieldType to be Pointer")
	} else {
		if named, ok := ptr.Elem().(*types.Named); !ok {
			_exit("bloomPtrHandle: expected filedType to be Pointer Named")
		} else {
			typ = addToImports(named)
		}
	}

	// size
	fmt.Fprintf(b1, "    size += 1\n")
	fmt.Fprintf(b1, "    if obj.%s != nil {\n", fieldName)
	fmt.Fprintf(b1, "        size += 258\n")
	fmt.Fprintf(b1, "    }\n")

	// encode
	fmt.Fprintf(b2, "    if obj.%s != nil {\n", fieldName)
	fmt.Fprintf(b2, "        b[0] = 183 + 2\n")
	fmt.Fprintf(b2, "        b[1] = 1\n")
	fmt.Fprintf(b2, "        b[2] = 0\n")
	fmt.Fprintf(b2, "    } else {\n")
	fmt.Fprintf(b2, "        b[0] = 128\n")
	fmt.Fprintf(b2, "    }\n")
	fmt.Fprintf(b2, "    if obj.%s != nil {\n", fieldName)
	fmt.Fprintf(b2, "        if _, err := w.Write(b[:3]); err != nil {\n")
	fmt.Fprintf(b2, "            return err\n")
	fmt.Fprintf(b2, "        }\n")
	fmt.Fprintf(b2, "        if _, err := w.Write(obj.%s[:]); err != nil {\n", fieldName)
	fmt.Fprintf(b2, "            return err\n")
	fmt.Fprintf(b2, "        }\n")
	fmt.Fprintf(b2, "    } else {\n")
	fmt.Fprintf(b2, "        if _, err := w.Write(b[:1]); err != nil {\n")
	fmt.Fprintf(b2, "            return err\n")
	fmt.Fprintf(b2, "        }\n")
	fmt.Fprintf(b2, "    }\n")

	// decode
	addDecodeBuf(b3)
	fmt.Fprintf(b3, "    if b, err = s.Bytes(); err != nil {\n")
	fmt.Fprintf(b3, "        %s\n", decodeErrorMsg(fieldName))
	fmt.Fprintf(b3, "    }\n")
	fmt.Fprintf(b3, "    if len(b) > 0 && len(b) != 256 {\n")
	fmt.Fprintf(b3, "        %s\n", decodeLenMismatch(256))
	fmt.Fprintf(b3, "    }\n")
	fmt.Fprintf(b3, "    obj.%s = &%s{}\n", fieldName, typ)
	fmt.Fprintf(b3, "    copy((*obj.%s)[:], b)\n", fieldName)
}

func byteSliceHandle(b1, b2, b3 *bytes.Buffer, _ types.Type, fieldName string) {
	// size
	fmt.Fprintf(b1, "    size += rlp.StringLen(obj.%s)\n", fieldName)

	// encode
	fmt.Fprintf(b2, "    if err := rlp.EncodeString(obj.%s, w, b[:]); err != nil {\n", fieldName)
	fmt.Fprintf(b2, "        return err\n")
	fmt.Fprintf(b2, "    }\n")

	// decode
	addDecodeBuf(b3)
	fmt.Fprintf(b3, "    if obj.%s, err = s.Bytes(); err != nil {\n", fieldName)
	fmt.Fprintf(b3, "        %s\n", decodeErrorMsg(fieldName))
	fmt.Fprintf(b3, "    }\n")
}

func byteSlicePtrHandle(b1, b2, b3 *bytes.Buffer, _ types.Type, fieldName string) {
	// size
	fmt.Fprintf(b1, "    if obj.%s != nil {\n", fieldName)
	fmt.Fprintf(b1, "        size += rlp.StringLen(*obj.%s)\n", fieldName)
	fmt.Fprintf(b1, "    } else {\n")
	fmt.Fprintf(b1, "        size += 1\n")
	fmt.Fprintf(b1, "    }\n")

	// encode
	fmt.Fprintf(b2, "    if obj.%s != nil {\n", fieldName)
	fmt.Fprintf(b2, "        if err := rlp.EncodeString(*obj.%s, w, b[:]); err != nil {\n", fieldName)
	fmt.Fprintf(b2, "            return err\n")
	fmt.Fprintf(b2, "        }\n")
	fmt.Fprintf(b2, "    } else {\n")
	fmt.Fprintf(b2, "        if err := rlp.EncodeString([]byte{}, w, b[:]); err != nil {\n")
	fmt.Fprintf(b2, "            return err\n")
	fmt.Fprintf(b2, "        }\n")
	fmt.Fprintf(b2, "    }\n")

	// decode
	addDecodeBuf(b3)

	fmt.Fprintf(b3, "    if b, err = s.Bytes(); err != nil {\n")
	fmt.Fprintf(b3, "        %s\n", decodeErrorMsg(fieldName))
	fmt.Fprintf(b3, "    }\n")
	fmt.Fprintf(b3, "    obj.%s = &[]byte{}\n", fieldName)
	fmt.Fprintf(b3, "    *obj.%s = append(*obj.%s, b...)\n", fieldName, fieldName)
}

func byteSliceSliceHandle(b1, b2, b3 *bytes.Buffer, _ types.Type, fieldName string) {
	// size
	fmt.Fprintf(b1, "    size += rlp.ByteSliceSliceSize(obj.%s)\n", fieldName)

	// encode
	fmt.Fprintf(b2, "    if err := rlp.EncodeByteSliceSlice(obj.%s, w, b[:]); err != nil {\n", fieldName)
	fmt.Fprintf(b2, "        return err\n")
	fmt.Fprintf(b2, "    }\n")

	// decode
	addDecodeBuf(b3)
	startListDecode(b3, fieldName)

	fmt.Fprintf(b3, "    obj.%s = [][]byte{}\n", fieldName)
	fmt.Fprintf(b3, "    for b, err = s.Bytes(); err == nil; b, err = s.Bytes() {\n")
	fmt.Fprintf(b3, "        obj.%s = append(obj.%s, b)\n", fieldName, fieldName)
	fmt.Fprintf(b3, "    }\n")

	endListDecode(b3, fieldName)
}

func _shortArraySliceHandle(b1, b2, b3 *bytes.Buffer, fieldType types.Type, fieldName string, size int) {
	var typ string
	if slc, ok := fieldType.(*types.Slice); !ok {
		_exit("_shortArraySliceHandle: expected filedType to be Slice")
	} else {
		if named, ok := slc.Elem().(*types.Named); !ok {
			_exit("_shortArraySliceHandle: expected filedType to be Slice Named")
		} else {
			typ = addToImports(named)
		}
	}

	// size
	addIntSize(b1)
	fmt.Fprintf(b1, "    gidx = (%d + 1) * len(obj.%s)\n", size, fieldName)
	fmt.Fprintf(b1, "    size += rlp.ListPrefixLen(gidx) + gidx\n")

	// encode
	addIntEncode(b2)
	fmt.Fprintf(b2, "    gidx = (%d + 1) * len(obj.%s)\n", size, fieldName)
	fmt.Fprintf(b2, "    if err := rlp.EncodeStructSizePrefix(gidx, w, b[:]); err != nil {\n")
	fmt.Fprintf(b2, "        return err\n")
	fmt.Fprintf(b2, "    }\n")
	fmt.Fprintf(b2, "    for i := 0; i < len(obj.%s); i++ {\n", fieldName)
	fmt.Fprintf(b2, "        if err := rlp.EncodeString(obj.%s[i][:], w, b[:]); err != nil {\n", fieldName)
	fmt.Fprintf(b2, "            return err\n")
	fmt.Fprintf(b2, "        }\n")
	fmt.Fprintf(b2, "    }\n")

	// decode
	addDecodeBuf(b3)
	startListDecode(b3, fieldName)

	fmt.Fprintf(b3, "    obj.%s = []%s{}\n", fieldName, typ)
	fmt.Fprintf(b3, "    for b, err = s.Bytes(); err == nil; b, err = s.Bytes() {\n")
	fmt.Fprintf(b3, "        if len(b) > 0 && len(b) != %d {\n", size)
	fmt.Fprintf(b3, "            %s\n", decodeLenMismatch(size))
	fmt.Fprintf(b3, "        }\n")
	fmt.Fprintf(b3, "        var s %s\n", typ)
	fmt.Fprintf(b3, "        copy(s[:], b)\n")
	fmt.Fprintf(b3, "        obj.%s = append(obj.%s, s)\n", fieldName, fieldName)
	fmt.Fprintf(b3, "    }\n")

	endListDecode(b3, fieldName)
}

func _shortArrayPtrSliceHandle(b1, b2, b3 *bytes.Buffer, fieldType types.Type, fieldName string, size int) {
	// size < 56

	var typ string
	if slc, ok := fieldType.(*types.Slice); !ok {
		_exit("_shortArrayPtrSliceHandle: expected filedType to be Slice")
	} else {
		if ptr, ok := slc.Elem().(*types.Pointer); !ok {
			_exit("_shortArrayPtrSliceHandle: expected filedType to be Slice Pointer")
		} else {
			if named, ok := ptr.Elem().(*types.Named); !ok {
				_exit("_shortArrayPtrSliceHandle: expected filedType to be Slice Pointer Named")
			} else {
				typ = addToImports(named)
			}
		}
	}

	// size
	addIntSize(b1)
	fmt.Fprintf(b1, "    for i := 0; i < len(obj.%s); i++ {\n", fieldName)
	fmt.Fprintf(b1, "        if obj.%s[i] != nil {\n", fieldName)
	fmt.Fprintf(b1, "            gidx += %d + 1\n", size)
	fmt.Fprintf(b1, "        } else {\n")
	fmt.Fprintf(b1, "            gidx += 1\n")
	fmt.Fprintf(b1, "        }\n")
	fmt.Fprintf(b1, "    }\n")
	fmt.Fprintf(b1, "    size += rlp.ListPrefixLen(gidx) + gidx\n")

	// encode
	addIntEncode(b2)
	fmt.Fprintf(b2, "    for i := 0; i < len(obj.%s); i++ {\n", fieldName)
	fmt.Fprintf(b2, "        if obj.%s[i] != nil {\n", fieldName)
	fmt.Fprintf(b2, "            gidx += %d + 1\n", size)
	fmt.Fprintf(b2, "        } else {\n")
	fmt.Fprintf(b2, "            gidx += 1\n")
	fmt.Fprintf(b2, "        }\n")
	fmt.Fprintf(b2, "    }\n")
	fmt.Fprintf(b2, "    if err := rlp.EncodeStructSizePrefix(gidx, w, b[:]); err != nil {\n")
	fmt.Fprintf(b2, "        return err\n")
	fmt.Fprintf(b2, "    }\n")
	fmt.Fprintf(b2, "    for i := 0; i < len(obj.%s); i++ {\n", fieldName)
	fmt.Fprintf(b2, "        if obj.%s[i] != nil {\n", fieldName)
	fmt.Fprintf(b2, "            if err := rlp.EncodeString(obj.%s[i][:], w, b[:]); err != nil {\n", fieldName)
	fmt.Fprintf(b2, "                return err\n")
	fmt.Fprintf(b2, "            }\n")
	fmt.Fprintf(b2, "        } else {\n")
	fmt.Fprintf(b2, "            if err := rlp.EncodeString([]byte{}, w, b[:]); err != nil {\n")
	fmt.Fprintf(b2, "                return err\n")
	fmt.Fprintf(b2, "            }\n")
	fmt.Fprintf(b2, "        }\n")
	fmt.Fprintf(b2, "    }\n")

	// decode
	addDecodeBuf(b3)
	startListDecode(b3, fieldName)

	fmt.Fprintf(b3, "    obj.%s = []*%s{}\n", fieldName, typ)
	fmt.Fprintf(b3, "    for b, err = s.Bytes(); err == nil; b, err = s.Bytes() {\n")
	fmt.Fprintf(b3, "        var s %s\n", typ)
	fmt.Fprintf(b3, "        if len(b) > 0 && len(b) != %d {\n", size)
	fmt.Fprintf(b3, "            %s\n", decodeLenMismatch(size))
	fmt.Fprintf(b3, "        } else if len(b) == %d {\n", size)
	fmt.Fprintf(b3, "            copy(s[:], b)\n")
	fmt.Fprintf(b3, "            obj.%s = append(obj.%s, &s)\n", fieldName, fieldName)
	fmt.Fprintf(b3, "        } else if len(b) == 0{\n")
	fmt.Fprintf(b3, "            obj.%s = append(obj.%s, nil)\n", fieldName, fieldName)
	fmt.Fprintf(b3, "        } else {\n")
	fmt.Fprintf(b3, "            %s\n", decodeLenMismatch(size))
	fmt.Fprintf(b3, "        }\n")
	fmt.Fprintf(b3, "    }\n")

	endListDecode(b3, fieldName)
}

func blockNonceSliceHandle(b1, b2, b3 *bytes.Buffer, fieldType types.Type, fieldName string) {
	_shortArraySliceHandle(b1, b2, b3, fieldType, fieldName, 8)
}

func blockNoncePtrSliceHandle(b1, b2, b3 *bytes.Buffer, fieldType types.Type, fieldName string) {
	_shortArrayPtrSliceHandle(b1, b2, b3, fieldType, fieldName, 8)
}

func addressSliceHandle(b1, b2, b3 *bytes.Buffer, fieldType types.Type, fieldName string) {
	_shortArraySliceHandle(b1, b2, b3, fieldType, fieldName, 20)
}

func addressPtrSliceHandle(b1, b2, b3 *bytes.Buffer, fieldType types.Type, fieldName string) {
	_shortArrayPtrSliceHandle(b1, b2, b3, fieldType, fieldName, 20)
}

func hashSliceHandle(b1, b2, b3 *bytes.Buffer, fieldType types.Type, fieldName string) {
	_shortArraySliceHandle(b1, b2, b3, fieldType, fieldName, 32)
}

func hashPtrSliceHandle(b1, b2, b3 *bytes.Buffer, fieldType types.Type, fieldName string) {
	_shortArrayPtrSliceHandle(b1, b2, b3, fieldType, fieldName, 32)
}

func byteArrayHandle(b1, b2, b3 *bytes.Buffer, fieldType types.Type, fieldName string) {
	var size int
	if arr, ok := fieldType.(*types.Array); !ok {
		_exit("byteArrayHandle: expected filedType to be Array")
	} else {
		size = int(arr.Len())
	}

	// size
	fmt.Fprintf(b1, "    size += %d + rlp.ListPrefixLen(%d)\n", size, size)

	// encode
	fmt.Fprintf(b2, "    if err := rlp.EncodeString(obj.%s[:], w, b[:]); err != nil {\n", fieldName)
	fmt.Fprintf(b2, "        return err\n")
	fmt.Fprintf(b2, "    }\n")

	// decode
	addDecodeBuf(b3)
	fmt.Fprintf(b3, "    if b, err = s.Bytes(); err != nil {\n")
	fmt.Fprintf(b3, "        return err\n")
	fmt.Fprintf(b3, "    }\n")
	fmt.Fprintf(b3, "    copy(obj.%s[:], b)\n", fieldName)
}

func byteArrayPtrHandle(b1, b2, b3 *bytes.Buffer, fieldType types.Type, fieldName string) {

	var size int
	if ptr, ok := fieldType.(*types.Pointer); !ok {
		_exit("byteArrayPtrHandle: expected filedType to be Pointer")
	} else {
		if arr, ok := ptr.Elem().(*types.Array); !ok {
			_exit("byteArrayPtrHandle: expected filedType to be Pointer Array")
		} else {
			size = int(arr.Len())
		}
	}

	// size
	fmt.Fprintf(b1, "    if obj.%s != nil {\n", fieldName)
	fmt.Fprintf(b1, "        size += %d\n", size)
	fmt.Fprintf(b1, "    }\n")
	fmt.Fprintf(b1, "    size += rlp.ListPrefixLen(%d)\n", size)

	// encode
	fmt.Fprintf(b2, "    if obj.%s != nil {\n", fieldName)
	fmt.Fprintf(b2, "        if err := rlp.EncodeString(obj.%s[:], w, b[:]); err != nil {\n", fieldName)
	fmt.Fprintf(b2, "            return err\n")
	fmt.Fprintf(b2, "        }\n")
	fmt.Fprintf(b2, "    } else {\n")
	fmt.Fprintf(b2, "        if err := rlp.EncodeString([]byte{}, w, b[:]); err != nil {\n")
	fmt.Fprintf(b2, "            return err\n")
	fmt.Fprintf(b2, "        }\n")
	fmt.Fprintf(b2, "    }\n")

	// decode
	addDecodeBuf(b3)
	fmt.Fprintf(b3, "    if b, err = s.Bytes(); err != nil {\n")
	fmt.Fprintf(b3, "        return err\n")
	fmt.Fprintf(b3, "    }\n")
	fmt.Fprintf(b3, "    if len(b) > 0 {\n")
	fmt.Fprintf(b3, "        obj.%s = &[%d]byte{}\n", fieldName, size)
	fmt.Fprintf(b3, "        copy((*obj.%s)[:], b)\n", fieldName)
	fmt.Fprintf(b3, "    }\n")
}
