package vm

import (
	"fmt"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/starknet"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/crypto"
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

func (cvm *CVM) Create(caller ContractRef, code []byte) ([]byte, common.Address, error) {
	address := crypto.CreateAddress(caller.Address(), cvm.intraBlockState.GetNonce(caller.Address()))
	//address := cvm.starknetService.Call()

	cvm.intraBlockState.SetCode(address, code)
	fmt.Println(">>>> Create Starknet Contract", address.Hex())
	return code, common.Address{}, nil

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
