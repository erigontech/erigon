package commands

import (
	"context"
	"reflect"
	"strings"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/starknet"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/rpc"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type StarknetGrpcCallArgs struct {
	Inputs      string
	Address     string
	Function    string
	Code        string
	BlockHash   string
	BlockNumber int64
	Network     string
}

type StarknetCallRequest struct {
	ContractAddress    common.Address32
	EntryPointSelector string
	CallData           []string
}

func (s StarknetGrpcCallArgs) ToMapAny() (result map[string]*anypb.Any) {
	result = make(map[string]*anypb.Any)

	v := reflect.ValueOf(s)
	typeOfS := v.Type()

	for i := 0; i < v.NumField(); i++ {
		fieldName := strings.ToLower(typeOfS.Field(i).Name)
		switch v.Field(i).Kind() {
		case reflect.Int64:
			result[fieldName], _ = anypb.New(wrapperspb.Int64(v.Field(i).Interface().(int64)))
		default:
			result[fieldName], _ = anypb.New(wrapperspb.String(v.Field(i).Interface().(string)))
		}
	}
	return result
}

// Call implements starknet_call.
func (api *StarknetImpl) Call(ctx context.Context, request StarknetCallRequest, blockNrOrHash rpc.BlockNumberOrHash) ([]string, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	code, err := api.GetCode(ctx, request.ContractAddress.ToCommonAddress(), blockNrOrHash)
	if err != nil {
		return nil, err
	}

	requestParams := &StarknetGrpcCallArgs{
		Inputs:   strings.Join(request.CallData, ","),
		Address:  request.ContractAddress.String(),
		Function: request.EntryPointSelector,
		Code:     code.String(),
	}

	if blockNrOrHash.BlockHash != nil {
		requestParams.BlockHash = blockNrOrHash.BlockHash.String()
	}

	if blockNrOrHash.BlockNumber != nil {
		requestParams.BlockNumber = blockNrOrHash.BlockNumber.Int64()
	}

	requestParamsMap := requestParams.ToMapAny()

	grpcRequest := &starknet.CallRequest{
		Method: "starknet_call",
		Params: requestParamsMap,
	}

	response, err := api.client.Call(ctx, grpcRequest)
	if err != nil {
		return nil, err
	}

	var result []string
	for _, v := range response.Result {
		s := wrapperspb.String("")
		v.UnmarshalTo(s)
		result = append(result, s.GetValue())
	}

	return result, nil
}
