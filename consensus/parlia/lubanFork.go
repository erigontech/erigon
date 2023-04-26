package parlia

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/common/u256"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/systemcontracts"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/log/v3"
)

type Bytes []byte

func (p *Parlia) getCurrentValidatorsBeforeLuban(header *types.Header, ibs *state.IntraBlockState) ([]libcommon.Address, error) {

	// prepare different method
	method := "getValidators"
	if p.chainConfig.IsEuler(header.Number) {
		method = "getMiningValidators"
	}

	data, err := p.validatorSetABIBeforeLuban.Pack(method)
	if err != nil {
		log.Error("Unable to pack tx for getValidators", "error", err)
		return nil, err
	}
	// do smart contract call
	msgData := Bytes(data)
	_, returnData, err := p.systemCall(header.Coinbase, systemcontracts.ValidatorContract, msgData[:], ibs, header, u256.Num0)
	if err != nil {
		return nil, err
	}

	var valSet []libcommon.Address
	err = p.validatorSetABIBeforeLuban.UnpackIntoInterface(&valSet, method, returnData)
	return valSet, err
}
