package parlia

import (
	"math/big"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/common/u256"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/systemcontracts"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/log/v3"
)

func (p *Parlia) getCurrentValidatorsBeforeBoneh(header *types.Header, ibs *state.IntraBlockState, blockHash common.Hash, blockNumber *big.Int) ([]common.Address, error) {

	// prepare different method
	method := "getValidators"
	if p.chainConfig.IsEuler(blockNumber) {
		method = "getMiningValidators"
	}

	data, err := p.validatorSetABIBeforeBoneh.Pack(method)
	if err != nil {
		log.Error("Unable to pack tx for getValidators", "error", err)
		return nil, err
	}
	// do smart contract call
	msgData := (hexutil.Bytes)(data)
	_, returnData, err := p.systemCall(header.Coinbase, systemcontracts.ValidatorContract, msgData[:], ibs, header, u256.Num0)
	if err != nil {
		return nil, err
	}

	var valSet []common.Address
	err = p.validatorSetABIBeforeBoneh.UnpackIntoInterface(&valSet, method, returnData)
	return valSet, err
}
