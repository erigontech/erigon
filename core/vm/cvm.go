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
	ret, err := cvm.run(code)

	if err == nil {
		address := common.Address{}
		cvm.intraBlockState.SetCode(address, ret)
	}

	return ret, common.Address{}, err
}

func (cvm *CVM) Config() Config {
	return cvm.config
}

func (cvm *CVM) IntraBlockState() IntraBlockState {
	return cvm.intraBlockState
}

func (cvm *CVM) run(code []byte) ([]byte, error) {
	// TODO:: call grpc cairo
	return code, nil
}
