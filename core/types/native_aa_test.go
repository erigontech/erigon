package types

import (
	"bytes"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/rlp"
)

func TestAccountAbstractionTransaction_DecodeRLP(t *testing.T) {
	from := common.HexToAddress("0x0")
	//to := common.HexToAddress("0x1")

	tx := &AccountAbstractionTransaction{
		Nonce:                       10,
		ChainID:                     uint256.NewInt(1),
		Tip:                         uint256.NewInt(200000),
		FeeCap:                      nil,
		Gas:                         0,
		AccessList:                  []AccessTuple{},
		SenderAddress:               &from,
		AuthorizationData:           nil,
		ExecutionData:               nil,
		Paymaster:                   nil,
		PaymasterData:               nil,
		Deployer:                    nil,
		DeployerData:                nil,
		BuilderFee:                  nil,
		ValidationGasLimit:          0,
		PaymasterValidationGasLimit: 0,
		PostOpGasLimit:              0,
		NonceKey:                    nil,
	}

	buf := bytes.NewBuffer(nil)
	err := tx.EncodeRLP(buf)
	assert.NoError(t, err)

	var txDecode AccountAbstractionTransaction
	stream := rlp.NewStream(buf, 0)
	err = txDecode.DecodeRLP(stream)
	assert.NoError(t, err)

	t.Log(txDecode.Nonce)
}
