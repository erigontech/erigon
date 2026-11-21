package types

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/race"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/execution/rlp"
)

// A block read with hashes reuses the collection buffers of earlier blocks: what is left per
// block is the cached hash of each txn.
func TestTxnEncodingsReuseBuffers(t *testing.T) {
	to := common.HexToAddress("0x0000000000000000000000000000000000000001")
	const n = 64
	stored := make([][]byte, n)
	txs := make([]Transaction, n)
	for i := range n {
		txn := &DynamicFeeTransaction{
			CommonTx: CommonTx{Nonce: uint64(i), To: &to, GasLimit: 21000, Data: make([]byte, 50+i*10)},
			ChainID:  u256.Num1,
			TipCap:   *uint256.NewInt(1),
			FeeCap:   *uint256.NewInt(2),
		}
		var err error
		stored[i], err = rlp.EncodeToBytes(txn)
		require.NoError(t, err)
		txs[i], err = DecodeTransaction(stored[i])
		require.NoError(t, err)
	}
	readBlock := func() {
		e := NewTxnEncodings()
		defer e.Release()
		for _, s := range stored {
			if err := e.Add(s); err != nil {
				t.Fatal(err)
			}
		}
		e.CacheHashes(txs)
	}
	readBlock()
	allocs := testing.AllocsPerRun(20, readBlock)
	if !race.Enabled { // the race detector drops sync.Pool puts
		require.LessOrEqual(t, allocs, float64(n), "one cached hash per txn")
	}
}
