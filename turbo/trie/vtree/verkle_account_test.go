package vtree

import (
	"fmt"
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
	}
	encodingSize := acc.GetVerkleAccountSizeForStorage()
	enc := make([]byte, encodingSize)
	acc.EncodeVerkleAccountForStorage(enc)
	fmt.Println(common.Bytes2Hex(enc))
	newAcc := DecodeVerkleAccountForStorage(enc)
	assert.Equal(t, acc.Nonce, newAcc.Nonce)
	assert.True(t, acc.CodeHash == newAcc.CodeHash)
	assert.Equal(t, acc.Balance.String(), newAcc.Balance.String())
}
