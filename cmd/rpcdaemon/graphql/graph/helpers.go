package graph

import (
	"encoding/hex"
	"fmt"
	"reflect"
	"strconv"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/types"
)

func convertDataToStringP(abstractMap map[string]interface{}, field string) *string {
	var result string

	switch abstractMap[field].(type) {
	case int64:
		result = strconv.FormatInt(abstractMap[field].(int64), 10)
		break
	case *hexutil.Big:
		result = abstractMap[field].(*hexutil.Big).String()
		break
	case hexutil.Bytes:
		result = abstractMap[field].(hexutil.Bytes).String()
		break
	case hexutil.Uint:
		result = abstractMap[field].(hexutil.Uint).String()
		break
	case hexutil.Uint64:
		result = abstractMap[field].(hexutil.Uint64).String()
		break
	case *libcommon.Address:
		result = abstractMap[field].(*libcommon.Address).String()
		break
	case libcommon.Address:
		result = abstractMap[field].(libcommon.Address).String()
		break
	case libcommon.Hash:
		result = abstractMap[field].(libcommon.Hash).String()
		break
	case types.Bloom:
		result = hex.EncodeToString(abstractMap[field].(types.Bloom).Bytes())
		break
	case types.BlockNonce:
		result = "0x" + strconv.FormatInt(int64(abstractMap[field].(types.BlockNonce).Uint64()), 16)
		break
	default:
		fmt.Println("string", field, abstractMap[field], reflect.TypeOf(abstractMap[field]))
		result = "unhandled"
	}
	return &result
}

func convertDataToIntP(abstractMap map[string]interface{}, field string) *int {
	var result int

	switch abstractMap[field].(type) {
	case hexutil.Uint64:
		resultUint, err := hexutil.DecodeUint64(abstractMap[field].(hexutil.Uint64).String())
		if err != nil {
			result = 0
		} else {
			result = int(resultUint)
		}
		break
	case hexutil.Uint:
		resultUint, err := hexutil.DecodeUint64(abstractMap[field].(hexutil.Uint).String())
		if err != nil {
			result = 0
		} else {
			result = int(resultUint)
		}
		break
	case int:
		result = abstractMap[field].(int)
		break

	default:
		fmt.Println("int", field, abstractMap[field], reflect.TypeOf(abstractMap[field]))
		result = 0
	}

	return &result
}

func convertDataToUInt64P(abstractMap map[string]interface{}, field string) *uint64 {
	var result uint64

	switch abstractMap[field].(type) {
	case hexutil.Uint64:
		resultUint, err := hexutil.DecodeUint64(abstractMap[field].(hexutil.Uint64).String())
		if err != nil {
			result = 0
		} else {
			result = uint64(resultUint)
		}
	case hexutil.Uint:
		resultUint, err := hexutil.DecodeUint64(abstractMap[field].(hexutil.Uint).String())
		if err != nil {
			result = 0
		} else {
			result = uint64(resultUint)
		}
	case *hexutil.Big:
		result = abstractMap[field].(*hexutil.Big).ToInt().Uint64()
	case int:
		result = abstractMap[field].(uint64)
		break

	default:
		fmt.Println("uint64", field, abstractMap[field], reflect.TypeOf(abstractMap[field]))
		result = 0
	}

	return &result
}

func convertDataToInt64P(abstractMap map[string]interface{}, field string) *int64 {
	var result int64

	switch abstractMap[field].(type) {
	case hexutil.Uint64:
		resultUint, err := hexutil.DecodeUint64(abstractMap[field].(hexutil.Uint64).String())
		if err != nil {
			result = 0
		} else {
			result = int64(resultUint)
		}
	case hexutil.Uint:
		resultUint, err := hexutil.DecodeUint64(abstractMap[field].(hexutil.Uint).String())
		if err != nil {
			result = 0
		} else {
			result = int64(resultUint)
		}
	case *hexutil.Big:
		result = abstractMap[field].(*hexutil.Big).ToInt().Int64()
	case int:
		result = abstractMap[field].(int64)
		break

	default:
		fmt.Println("int64", field, abstractMap[field], reflect.TypeOf(abstractMap[field]))
		result = 0
	}

	return &result
}

func convertStrHexToDec(hexString *string) *string {
	var result string

	resUInt64, err := hexutil.DecodeUint64(*hexString)
	if err != nil {
		fmt.Println(err)
		result = "0"
	}
	result = strconv.FormatUint(resUInt64, 10)

	return &result
}
