package types

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/u256"
)

func TestWithdrawalsHash(t *testing.T) {
	w := &Withdrawal{
		Index:     0,
		Validator: 0,
		Address:   common.HexToAddress("0x6295ee1b4f6dd65047762f924ecd367c17eabf8f"),
		Amount:    *u256.Num1,
	}
	withdrawals := Withdrawals([]*Withdrawal{w})
	hash := DeriveSha(withdrawals)
	// The only trie node is short (its RLP < 32 bytes).
	// Its Keccak should be returned, not the node itself.
	assert.Equal(t, common.HexToHash("82cc6fbe74c41496b382fcdf25216c5af7bdbb5a3929e8f2e61bd6445ab66436"), hash)
}
