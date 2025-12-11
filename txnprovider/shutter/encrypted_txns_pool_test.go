package shutter

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/testlog"
	"github.com/erigontech/erigon/txnprovider/shutter/shuttercfg"
)

func TestEncryptedTxnsPoolReturnsCorrectTxnWithAReplaySequence(t *testing.T) {
	t.Parallel()
	// had this sequence of additions happen on chiado and caused an incorrect result
	logger := testlog.Logger(t, log.LvlTrace)
	p := NewEncryptedTxnsPool(logger, shuttercfg.Config{MaxPooledEncryptedTxns: 10_000}, nil, nil)
	p.addSubmission(EncryptedTxnSubmission{
		EonIndex: 4,
		TxnIndex: 38_273,
		GasLimit: big.NewInt(21_000),
	})
	p.addSubmission(EncryptedTxnSubmission{
		EonIndex: 4,
		TxnIndex: 38_275,
		GasLimit: big.NewInt(21_000),
	})
	// gap detected between 38_273 and 38_275, so-refetch happened and fetched 38_274 and 38_275
	p.addSubmission(EncryptedTxnSubmission{
		EonIndex: 4,
		TxnIndex: 38_274,
		GasLimit: big.NewInt(21_000),
	})
	p.addSubmission(EncryptedTxnSubmission{
		EonIndex: 4,
		TxnIndex: 38_275,
		GasLimit: big.NewInt(21_000),
	})
	// then 38_274 event came in (after an unwind)
	p.addSubmission(EncryptedTxnSubmission{
		EonIndex: 4,
		TxnIndex: 38_274,
		GasLimit: big.NewInt(21_000),
	})
	txns, err := p.Txns(4, 38_275, 38_276, 1_000_000)
	require.NoError(t, err)
	require.Len(t, txns, 1)
	require.Equal(t, TxnIndex(38_275), txns[0].TxnIndex)
}
