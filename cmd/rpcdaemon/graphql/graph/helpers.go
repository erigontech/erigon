package graph

import (
	"encoding/hex"
	"fmt"
	"reflect"
	"strconv"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func convertDataToStringP(abstractMap map[string]any, field string) *string {
	if abstractMap[field] == nil {
		return nil
	}
	var result string

	switch v := abstractMap[field].(type) {
	case int64:
		result = strconv.FormatInt(v, 10)
	case *hexutil.Big:
		if reflect.ValueOf(abstractMap[field]).IsZero() {
			return nil
		}
		result = v.String()
	case hexutil.Bytes:
		result = v.String()
	case hexutil.Uint:
		result = v.String()
	case hexutil.Uint64:
		result = v.String()
	case *common.Address:
		if reflect.ValueOf(abstractMap[field]).IsZero() {
			return nil
		}
		result = v.String()
	case accounts.Address:
		result = v.String()
	case common.Address:
		result = v.String()
	case common.Hash:
		result = v.String()
	case *common.Hash:
		if v == nil {
			return nil
		}
		result = v.String()
	case types.Bloom:
		result = hex.EncodeToString(v.Bytes())
	case types.BlockNonce:
		result = "0x" + fmt.Sprintf("%016x", v.Uint64())
	case []uint8:
		result = "0x" + hex.EncodeToString(v)
	case *uint256.Int:
		if reflect.ValueOf(abstractMap[field]).IsZero() {
			return nil
		}
		result = v.Hex()
	case uint64:
		result = "0x" + strconv.FormatInt(int64(v), 16)
	default:
		result = "unhandled"
	}

	return &result
}

func convertDataToIntP(abstractMap map[string]any, field string) *int {
	if abstractMap[field] == nil {
		return nil
	}
	var result int

	switch v := abstractMap[field].(type) {
	case hexutil.Uint64:
		resultUint, err := hexutil.DecodeUint64(v.String())
		if err != nil {
			result = 0
		} else {
			result = int(resultUint)
		}
	case hexutil.Uint:
		resultUint, err := hexutil.DecodeUint64(v.String())
		if err != nil {
			result = 0
		} else {
			result = int(resultUint)
		}
	case int:
		result = v
	default:
		result = 0
	}

	return &result
}

func convertDataToUint64P(abstractMap map[string]any, field string) *uint64 {
	if abstractMap[field] == nil {
		return nil
	}
	var result uint64

	switch v := abstractMap[field].(type) {
	case hexutil.Uint64:
		resultUint, err := hexutil.DecodeUint64(v.String())
		if err != nil {
			result = 0
		} else {
			result = resultUint
		}
	case *hexutil.Uint64:
		result = uint64(*v)
	case hexutil.Uint:
		resultUint, err := hexutil.DecodeUint64(v.String())
		if err != nil {
			result = 0
		} else {
			result = resultUint
		}
	case *hexutil.Big:
		result = v.ToInt().Uint64()
	case int:
		result = uint64(v)
	case uint64:
		result = v
	default:
		result = 0
	}

	return &result
}
