package vtree

import (
	"math/big"
	"testing"

	"github.com/ledgerwatch/erigon/common"
	"github.com/stretchr/testify/assert"
)

func TestVerkleAccount(t *testing.T) {
	acc := VerkleAccount{
		Balance:  big.NewInt(2935584985),
		Nonce:    100000,
		CodeHash: common.HexToHash("0xfffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
		CodeSize: 300,
	}
	encodingSize := acc.GetVerkleAccountSizeForStorage()
	enc := make([]byte, encodingSize)
	acc.EncodeVerkleAccountForStorage(enc)
	newAcc := DecodeVerkleAccountForStorage(enc)
	assert.Equal(t, acc.CodeSize, newAcc.CodeSize)
	assert.Equal(t, acc.Nonce, newAcc.Nonce)
	assert.Equal(t, acc.CodeHash, newAcc.CodeHash)
	assert.Equal(t, acc.Balance.String(), newAcc.Balance.String())
}

func TestVerkleAccountEmptyCodeHash(t *testing.T) {
	acc := VerkleAccount{
		Balance:  big.NewInt(2935584985),
		Nonce:    100000,
		CodeHash: emptyCodeHash,
	}
	encodingSize := acc.GetVerkleAccountSizeForStorage()
	assert.Equal(t, encodingSize, 10)
	enc := make([]byte, encodingSize)
	acc.EncodeVerkleAccountForStorage(enc)
	newAcc := DecodeVerkleAccountForStorage(enc)
	assert.Equal(t, acc.Nonce, newAcc.Nonce)
	assert.Equal(t, acc.CodeHash, emptyCodeHash)
	assert.Equal(t, acc.Balance.String(), newAcc.Balance.String())
}
