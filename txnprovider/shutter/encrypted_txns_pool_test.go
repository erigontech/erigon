package shutter

import (
	"context"
	"errors"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc/contracts"
	"github.com/erigontech/erigon/rpc/jsonrpc"
	shuttercontracts "github.com/erigontech/erigon/txnprovider/shutter/internal/contracts"
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

// The sequencer contract numbers txn indexes per eon, restarting at 0: the first
// submission of a new eon must not be dropped as "behind" the previous eon's max.
func TestEncryptedTxnsPoolAcceptsFirstSubmissionOfNewEon(t *testing.T) {
	t.Parallel()
	logger := testlog.Logger(t, log.LvlTrace)
	p := NewEncryptedTxnsPool(logger, shuttercfg.Config{MaxPooledEncryptedTxns: 10_000}, nil, nil)
	p.addSubmission(EncryptedTxnSubmission{
		EonIndex: 0,
		TxnIndex: 4,
		GasLimit: big.NewInt(21_000),
	})
	err := p.handleEncryptedTxnSubmissionEvent(context.Background(), &shuttercontracts.SequencerTransactionSubmitted{
		Eon:      1,
		TxIndex:  0,
		GasLimit: big.NewInt(21_000),
	})
	require.NoError(t, err)
	txns, err := p.Txns(1, 0, 1, 1_000_000)
	require.NoError(t, err)
	require.Len(t, txns, 1)
	require.Equal(t, TxnIndex(0), txns[0].TxnIndex)
}

// A new eon's key is broadcast before its keyper set activates, so submissions to
// the old and the new eon can interleave: an old-eon submission arriving after the
// new eon's first one is still decryptable and must not be dropped.
func TestEncryptedTxnsPoolAcceptsOldEonSubmissionAfterNewEonStarted(t *testing.T) {
	t.Parallel()
	logger := testlog.Logger(t, log.LvlTrace)
	p := NewEncryptedTxnsPool(logger, shuttercfg.Config{MaxPooledEncryptedTxns: 10_000}, nil, nil)
	p.addSubmission(EncryptedTxnSubmission{
		EonIndex: 0,
		TxnIndex: 4,
		GasLimit: big.NewInt(21_000),
	})
	p.addSubmission(EncryptedTxnSubmission{
		EonIndex: 1,
		TxnIndex: 0,
		GasLimit: big.NewInt(21_000),
	})
	err := p.handleEncryptedTxnSubmissionEvent(context.Background(), &shuttercontracts.SequencerTransactionSubmitted{
		Eon:      0,
		TxIndex:  5,
		GasLimit: big.NewInt(21_000),
	})
	require.NoError(t, err)
	txns, err := p.Txns(0, 5, 6, 1_000_000)
	require.NoError(t, err)
	require.Len(t, txns, 1)
	require.Equal(t, TxnIndex(5), txns[0].TxnIndex)
}

func newTestEncryptedTxnsPool(t *testing.T, backend *contracts.MockBackend) *EncryptedTxnsPool {
	config := shuttercfg.Config{
		SequencerContractAddress: "0x0000000000000000000000000000000000000bad",
		MaxPooledEncryptedTxns:   1000,
	}
	return NewEncryptedTxnsPool(testlog.Logger(t, log.LvlCrit), config, backend, nil)
}

// The block-event stream can report a new head before the eth_getLogs path
// (a fresh RO tx) sees it, so the initial submissions load races ahead of the
// chain head and the RPC rejects it with jsonrpc.ErrBlockRangeIntoFuture. The pool must
// wait for the head to catch up and retry rather than treating it as fatal.
func TestEncryptedTxnsPoolLoadSubmissionsRetriesWhenChainHeadIsBehind(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	backend := contracts.NewMockBackend(ctrl)
	headBehindErr := errors.New("rpc error: " + jsonrpc.ErrBlockRangeIntoFuture)
	gomock.InOrder(
		backend.EXPECT().FilterLogs(gomock.Any(), gomock.Any()).Return(nil, headBehindErr),
		backend.EXPECT().FilterLogs(gomock.Any(), gomock.Any()).Return([]types.Log{}, nil),
	)

	p := newTestEncryptedTxnsPool(t, backend)
	err := p.loadSubmissions(context.Background(), 0, 100)
	require.NoError(t, err)
}

func TestEncryptedTxnsPoolLoadSubmissionsDoesNotRetryOtherErrors(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	backend := contracts.NewMockBackend(ctrl)
	backend.EXPECT().FilterLogs(gomock.Any(), gomock.Any()).Return(nil, errors.New("boom")).Times(1)

	p := newTestEncryptedTxnsPool(t, backend)
	err := p.loadSubmissions(context.Background(), 0, 100)
	require.ErrorContains(t, err, "boom")
}

func TestEncryptedTxnsPoolLoadSubmissionsSurfacesIteratorError(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	backend := contracts.NewMockBackend(ctrl)
	// a log that cannot be unpacked as a TransactionSubmitted event makes the
	// iterator fail mid-iteration - the error must not be silently dropped.
	malformed := types.Log{
		Address: common.HexToAddress("0x0000000000000000000000000000000000000bad"),
		Topics:  []common.Hash{common.HexToHash("0x01")},
		Data:    []byte{0x01},
	}
	backend.EXPECT().FilterLogs(gomock.Any(), gomock.Any()).Return([]types.Log{malformed}, nil)

	p := newTestEncryptedTxnsPool(t, backend)
	err := p.loadSubmissions(context.Background(), 0, 100)
	require.Error(t, err)
}
