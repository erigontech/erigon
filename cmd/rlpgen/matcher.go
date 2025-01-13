package main

import (
	"bytes"
	"fmt"
	"go/types"
)

type handle func(b1, b2, b3 *bytes.Buffer, fieldType types.Type, fieldName string)

// func foofunc(b1, b2, b3 *bytes.Buffer, fieldType types.Type, fieldName string) {}

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
	"[]*[]byte":           byteSlicePtrSliceHandle,
	"[]types.BlockNonce":  blockNonceSliceHandle,
	"[]*types.BlockNonce": blockNoncePtrSliceHandle,
	"[]common.Address":    addressSliceHandle,
	"[]*common.Address":   addressPtrSliceHandle,
	"[]common.Hash":       hashSliceHandle,
	"[]*common.Hash":      hashPtrSliceHandle,
	"[n]byte":             byteArrayHandle,
	"*[n]byte":            byteArrayPtrHandle,
}

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
		panic(fmt.Sprintf("no handle added for type: %s", strType))
	}
}
