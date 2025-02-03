package rlp

import "github.com/erigontech/erigon-lib/common"

func GenericEncodingSize(v interface{}) (size int) {
	switch v := v.(type) {
	case bool:
		return 1
	case int8, int16, int32, int64, int, uint8, uint16, uint32, uint64, uint:
		_u64 := v.(uint64)
		return IntLenExcludingHead(_u64) + 1
	case common.Address:
		return 21
	case common.Hash:
		return 33
	default:
		panic("GenericEncodingSize: unhandled case")
	}
}
