package blockinfo

import (
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/smt/pkg/smt"
	"math/big"
	"testing"
	"github.com/stretchr/testify/require"
)

func TestBlockInfoTree(t *testing.T) {
	smt := smt.NewSMT(nil)

	sequencerAddress := new(big.Int)
	sequencerAddress.SetString("a9127a157cee3cd2452a194e4efc2f8a5612cfc36c66e768700727ede4d0e2e6", 16)
	newBlockNumber := big.NewInt(1)
	blockGasLimit := big.NewInt(1)
	finalTimestamp := big.NewInt(1)
	finalGER := big.NewInt(1)
	finalBlockHash := new(big.Int)
	finalBlockHash.SetString("a9127a157cee3cd2452a194e4efc2f8a5612cfc36c66e768700727ede4d0e2e6", 16)
	oldBlockHash := new(big.Int)
	oldBlockHash.SetString("a9127a157cee3cd2452a194e4efc2f8a5612cfc36c66e768700727ede4d0e2e6", 16)

	root, err := BuildBlockInfoTree(smt, oldBlockHash, sequencerAddress, newBlockNumber, blockGasLimit, finalTimestamp, finalGER, finalBlockHash)
	require.NoError(t, err, "Building block info tree should not produce an error")

	expectedRoot := new(big.Int)
	expectedRoot.SetString("208579169e61f707c35ab2e4e6c37179eb016ff39e6ab5d1f1389ca85d135d58", 16)

	require.Equal(t, expectedRoot, root, "The calculated root hash should match the expected root hash")
}

func TestReceiptTree(t *testing.T) {
	smt := smt.NewSMT(nil)

	logs := []*types.Log{
		{
			Address: common.HexToAddress("0x0000000000000000000000000000000000000000"),
			Topics: []common.Hash{
				common.HexToHash("0000000000000000000000000000000000000000000000000000000000000001"),
			},
			Data: common.HexToHash("0000000000000000000000000000000000000000000000000000000000000000").Bytes(),
		},
		{
			Address: common.HexToAddress("0x0000000000000000000000000000000000000000"),
			Topics: []common.Hash{
				common.HexToHash("0000000000000000000000000000000000000000000000000000000000000004"),
				common.HexToHash("0000000000000000000000000000000000000000000000000000000000000005"),
				common.HexToHash("0000000000000000000000000000000000000000000000000000000000000006"),
			},
			Data: common.HexToHash("0000000000000000000000000000000000000000000000000000000000000000").Bytes(),
		},
		{
			Address: common.HexToAddress("0x0000000000000000000000000000000000000000"),
			Topics: []common.Hash{
				common.HexToHash("0000000000000000000000000000000000000000000000000000000000000001"),
				common.HexToHash("0000000000000000000000000000000000000000000000000000000000000002"),
			},
			Data: common.HexToHash("0000000000000000000000000000000000000000000000000000000000000000").Bytes(),
		},
	}

	root, err := BuildReceiptTree(
		smt,
		big.NewInt(1),
		logs,
		big.NewInt(1),
		big.NewInt(1),
		big.NewInt(1),
		big.NewInt(1),
		big.NewInt(1),
	)

	if err != nil {
		t.Fatal(err)
	}

	// root taken from JS implementation
	expectedRoot := "5d072bff5949879c2ae676f175123401f287dee3daa63d81c09c15cd34d47ec7"
	actualRoot := root.Text(16)

	if actualRoot != expectedRoot {
		t.Fatalf("expected root %s, got %s", expectedRoot, actualRoot)
	}
}

/*
Contrived tests of the SMT inserts (compared with test cases from the JS implementation)
*/

func TestSetL2TxHash(t *testing.T) {
	smt := smt.NewSMT(nil)
	txIndex := big.NewInt(1)
	l2TxHash := common.HexToHash("0x000000000000000000000000000000005Ca1aB1E").Big()

	root, err := setL2TxHash(smt, txIndex, l2TxHash)
	if err != nil {
		t.Fatal(err)
	}

	// root taken from JS implementation
	expectedRoot := "a9127a157cee3cd2452a194e4efc2f8a5612cfc36c66e768700727ede4d0e2e6"
	actualRoot := root.Text(16)

	if actualRoot != expectedRoot {
		t.Fatalf("expected root %s, got %s", expectedRoot, actualRoot)
	}
}

func TestSetTxStatus(t *testing.T) {
	smt := smt.NewSMT(nil)
	txIndex := big.NewInt(1)
	status := common.HexToHash("0x000000000000000000000000000000005Ca1aB1E").Big()

	root, err := setTxStatus(smt, txIndex, status)
	if err != nil {
		t.Fatal(err)
	}

	// root taken from JS implementation
	expectedRoot := "7cb6a0928f5165a422cfbe5f93d1cc9eda3f686715639823f6087818465fcbb8"
	actualRoot := root.Text(16)

	if actualRoot != expectedRoot {
		t.Fatalf("expected root %s, got %s", expectedRoot, actualRoot)
	}
}

func TestSetCumulativeGasUsed(t *testing.T) {
	smt := smt.NewSMT(nil)
	txIndex := big.NewInt(1)
	cgu := common.HexToHash("0x000000000000000000000000000000005Ca1aB1E").Big()

	root, err := setCumulativeGasUsed(smt, txIndex, cgu)
	if err != nil {
		t.Fatal(err)
	}

	// root taken from JS implementation
	expectedRoot := "c07ff46f07be5b81465c30848202acc4bf82805961d8a9f9ffe74e820e4bca68"
	actualRoot := root.Text(16)

	if actualRoot != expectedRoot {
		t.Fatalf("expected root %s, got %s", expectedRoot, actualRoot)
	}
}

func TestSetTxEffectivePercentage(t *testing.T) {
	smt := smt.NewSMT(nil)
	txIndex := big.NewInt(1)
	egp := common.HexToHash("0x000000000000000000000000000000005Ca1aB1E").Big()

	root, err := setTxEffectivePercentage(smt, txIndex, egp)
	if err != nil {
		t.Fatal(err)
	}

	// root taken from JS implementation
	expectedRoot := "f6b3130ecdd23bd9e47c4dda0fdde6bd0e0446c6d6927778e57e80016fa9fa23"
	actualRoot := root.Text(16)

	if actualRoot != expectedRoot {
		t.Fatalf("expected root %s, got %s", expectedRoot, actualRoot)
	}
}

func TestSetTxLogs(t *testing.T) {
	smt := smt.NewSMT(nil)
	txIndex := big.NewInt(1)
	logIndex := big.NewInt(1)
	log := common.HexToHash("0x000000000000000000000000000000005Ca1aB1E").Big()

	root, err := setTxLog(smt, txIndex, logIndex, log)
	if err != nil {
		t.Fatal(err)
	}

	// root taken from JS implementation
	expectedRoot := "aff38141ae4538baf61f08efe3019ef2d219f30b98b1d40a9813d502f6bacb12"
	actualRoot := root.Text(16)

	if actualRoot != expectedRoot {
		t.Fatalf("expected root %s, got %s", expectedRoot, actualRoot)
	}
}
