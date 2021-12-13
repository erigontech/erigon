package vm

import (
	"github.com/ledgerwatch/erigon/common"
)

func NewCVM(state IntraBlockState) *CVM {
	cvm := &CVM{
		intraBlockState: state,
	}

	return cvm
}

type CVM struct {
	config          Config
	intraBlockState IntraBlockState
}

func (cvm *CVM) Create(code []byte) ([]byte, common.Address, error) {
	address := common.Address{}
	cvm.intraBlockState.SetCode(address, code)
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
