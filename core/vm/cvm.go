package vm

import (
	"context"
	"fmt"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/starknet"
	"github.com/ledgerwatch/erigon/common"
)

func NewCVM(state IntraBlockState, starknetGrpcClient starknet.CAIROVMClient) *CVM {
	cvm := &CVM{
		intraBlockState:    state,
		starknetGrpcClient: starknetGrpcClient,
	}

	return cvm
}

type CVM struct {
	config             Config
	intraBlockState    IntraBlockState
	starknetGrpcClient starknet.CAIROVMClient
}

func (cvm *CVM) Create(caller ContractRef, code, salt []byte) ([]byte, common.Address, error) {
	addressRequest := &starknet.AddressRequest{
		Salt:                string(salt),
		ContractDefinition:  string(code),
		ConstructorCalldata: make([]uint32, 10),
		CallerAddress:       caller.Address().String(),
	}

	addressResponse, err := cvm.starknetGrpcClient.Address(context.Background(), addressRequest)
	if err != nil {
		return nil, common.Address{}, err
	}

	address32 := common.HexToAddress32(addressResponse.GetAddress())
	address := address32.ToCommonAddress()

	cvm.intraBlockState.SetCode(address, code)
	fmt.Println(">>>> Create Starknet Contract", address32.Hex())
	return code, address, nil

	//TODO:: execute cairo construct
	//ret, err := cvm.run(code)
}

func (cvm *CVM) Config() Config {
	return cvm.config
}

func (cvm *CVM) IntraBlockState() IntraBlockState {
	return cvm.intraBlockState
}

//func (cvm *CVM) run(code []byte) ([]byte, error) {
//	// TODO:: call grpc cairo
//	return code, nil
//}
