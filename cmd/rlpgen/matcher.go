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
	"go/types"
)

// handle should write encoding size of type as well as encoding and decoding logic for the type
type handle func(b1, b2, b3 *bytes.Buffer, fieldType types.Type, fieldName string)

// func foofunc(b1, b2, b3 *bytes.Buffer, fieldType types.Type, fieldName string) {}

// all possible types that this generator can handle for the time being.
// to add a new type add a string representation of type here and write the handle function for it in the `handlers.go`
var handlers = map[string]handle{
	"uint64":              uintHandle,
	"*uint64":             uintPtrHandle,
	"big.Int":             bigIntHandle,
	"*big.Int":            bigIntPtrHandle,
	"uint256.Int":         uint256Handle,
	"*uint256.Int":        uint256PtrHandle,
	"types.BlockNonce":    blockNonceHandle,
	"*types.BlockNonce":   blockNoncePtrHandle,
	"common.Address":      addressHandle,
	"*common.Address":     addressPtrHandle,
	"common.Hash":         hashHandle,
	"*common.Hash":        hashPtrHandle,
	"types.Bloom":         bloomHandle,
	"*types.Bloom":        bloomPtrHandle,
	"[]byte":              byteSliceHandle,
	"*[]byte":             byteSlicePtrHandle,
	"[][]byte":            byteSliceSliceHandle,
	"[]types.BlockNonce":  blockNonceSliceHandle,
	"[]*types.BlockNonce": blockNoncePtrSliceHandle,
	"[]common.Address":    addressSliceHandle,
	"[]*common.Address":   addressPtrSliceHandle,
	"[]common.Hash":       hashSliceHandle,
	"[]*common.Hash":      hashPtrSliceHandle,
	"[n]byte":             byteArrayHandle,
	"*[n]byte":            byteArrayPtrHandle,
}

// recursive function, constructs string representation of a type. array represented as [n]
func matchTypeToString(fieldType types.Type, in string) string {
	if named, ok := fieldType.(*types.Named); ok {
		return in + named.Obj().Pkg().Name() + "." + named.Obj().Name()
	} else if ptr, ok := fieldType.(*types.Pointer); ok {
		return matchTypeToString(ptr.Elem(), in+"*")
	} else if slc, ok := fieldType.(*types.Slice); ok {
		return matchTypeToString(slc.Elem(), in+"[]")
	} else if arr, ok := fieldType.(*types.Array); ok {
		return matchTypeToString(arr.Elem(), in+"[n]")
	} else if basic, ok := fieldType.(*types.Basic); ok {
		return in + basic.Name()
	} else {
		panic("_matchTypeToString: unhandled match")
	}
}

// matches string representation of a type to a corresponding function
func matchStrTypeToFunc(strType string) handle {
	switch strType {
	case "int16", "int32", "int", "int64", "uint16", "uint32", "uint", "uint64":
		return handlers["uint64"]
	case "*int16", "*int32", "*int", "*int64", "*uint16", "*uint32", "*uint", "*uint64":
		return handlers["*uint64"]
	default:
		if fn, ok := handlers[strType]; ok {
			return fn
		}
		panic("no handle added for type: " + strType)
	}
}
