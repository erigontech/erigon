package vm

import (
	"fmt"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/params"
)

const CairoNotImplemented = "the method is currently not implemented for cvm: %s"

type CVMAdapter struct {
	Cvm *CVM
}

func (c *CVMAdapter) Reset(txCtx evmtypes.TxContext, ibs evmtypes.IntraBlockState) {
	c.Cvm.intraBlockState = ibs
}

func (c *CVMAdapter) Create(caller ContractRef, code []byte, gas uint64, value *uint256.Int) (ret []byte, contractAddr libcommon.Address, leftOverGas uint64, err error) {
	leftOverGas = 0

	ret, contractAddr, err = c.Cvm.Create(caller, code)

	return ret, contractAddr, leftOverGas, err
}

func (cvm *CVMAdapter) Call(caller ContractRef, addr libcommon.Address, input []byte, gas uint64, value *uint256.Int, bailout bool) (ret []byte, leftOverGas uint64, err error) {
	return nil, 0, fmt.Errorf(CairoNotImplemented, "Call")
}

func (cvm *CVMAdapter) Config() Config {
	return cvm.Cvm.Config()
}

func (cvm *CVMAdapter) ChainConfig() *chain.Config {
	return params.AllProtocolChanges
}

func (cvm *CVMAdapter) ChainRules() *chain.Rules {
	return &chain.Rules{}
}

func (cvm *CVMAdapter) Context() evmtypes.BlockContext {
	return evmtypes.BlockContext{}
}

func (cvm *CVMAdapter) IntraBlockState() evmtypes.IntraBlockState {
	return cvm.Cvm.IntraBlockState()
}

func (cvm *CVMAdapter) TxContext() evmtypes.TxContext {
	return evmtypes.TxContext{}
}
