package types

import (
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/stretchr/testify/assert"
)

func TestWithdrawalsHash(t *testing.T) {
	w := &Withdrawal{
		Index:     0,
		Validator: 0,
		Address:   libcommon.HexToAddress("0x6295ee1b4f6dd65047762f924ecd367c17eabf8f"),
		Amount:    1,
	}
	withdrawals := Withdrawals([]*Withdrawal{w})
	hash := DeriveSha(withdrawals)
	// The only trie node is short (its RLP < 32 bytes).
	// Its Keccak should be returned, not the node itself.
	assert.Equal(t, libcommon.HexToHash("82cc6fbe74c41496b382fcdf25216c5af7bdbb5a3929e8f2e61bd6445ab66436"), hash)
}
