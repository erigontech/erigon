package types

import (
	"bytes"
	"math/big"
	"reflect"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

// allArbTxTypes returns one instance of each Arbitrum transaction type.
// If a new Arb tx type is added, it MUST be added here.
func allArbTxTypes() []Transaction {
	to := common.HexToAddress("0xdeadbeef")
	retryTo := &to

	return []Transaction{
		&ArbitrumDepositTx{
			ChainId:     big.NewInt(412346),
			L1RequestId: common.HexToHash("0x01"),
			From:        common.HexToAddress("0x1111"),
			To:          common.HexToAddress("0x2222"),
			Value:       big.NewInt(1000),
		},
		&ArbitrumInternalTx{
			ChainId: uint256.NewInt(412346),
			Data:    []byte("internal"),
		},
		&ArbitrumUnsignedTx{
			ChainId:   big.NewInt(412346),
			From:      common.HexToAddress("0x3333"),
			Nonce:     1,
			GasFeeCap: big.NewInt(1000000000),
			Gas:       21000,
			To:        &to,
			Value:     big.NewInt(100),
		},
		&ArbitrumContractTx{
			ChainId:   big.NewInt(412346),
			RequestId: common.HexToHash("0x02"),
			From:      common.HexToAddress("0x4444"),
			GasFeeCap: big.NewInt(1000000000),
			Gas:       100000,
			To:        &to,
			Value:     big.NewInt(0),
			Data:      []byte("contract"),
		},
		&ArbitrumRetryTx{
			ChainId:             big.NewInt(412346),
			From:                common.HexToAddress("0x5555"),
			Nonce:               0,
			GasFeeCap:           big.NewInt(1000000000),
			Gas:                 60000,
			To:                  &to,
			Value:               big.NewInt(100),
			TicketId:            common.HexToHash("0x03"),
			RefundTo:            common.HexToAddress("0x6666"),
			MaxRefund:           big.NewInt(500),
			SubmissionFeeRefund: big.NewInt(7),
		},
		&ArbitrumSubmitRetryableTx{
			ChainId:          big.NewInt(412346),
			RequestId:        common.HexToHash("0x04"),
			From:             common.HexToAddress("0x7777"),
			L1BaseFee:        big.NewInt(30000000000),
			DepositValue:     big.NewInt(10000),
			GasFeeCap:        big.NewInt(1000000000),
			Gas:              100000,
			RetryTo:          retryTo,
			RetryValue:       big.NewInt(100),
			Beneficiary:      common.HexToAddress("0x8888"),
			MaxSubmissionFee: big.NewInt(7),
			FeeRefundAddr:    common.HexToAddress("0x9999"),
			RetryData:        []byte("retry"),
		},
	}
}

func testArbSigner() ArbitrumSigner {
	return NewArbitrumSigner(*LatestSignerForChainID(big.NewInt(412346)))
}

// TestArbTxAsMessage_TxRunContext_NotNil guards against the bug where
// AsMessage sets TxRunMode but not TxRunContext, causing nil panic in
// MsgIsNonMutating(). Every Arb tx AsMessage must set TxRunContext.
func TestArbTxAsMessage_TxRunContext_NotNil(t *testing.T) {
	signer := testArbSigner()

	for _, tx := range allArbTxTypes() {
		t.Run(reflect.TypeOf(tx).Elem().Name(), func(t *testing.T) {
			msg, err := tx.AsMessage(signer.Signer, big.NewInt(1000000000), nil)
			require.NoError(t, err)
			require.NotNil(t, msg.TxRunContext,
				"AsMessage must set TxRunContext — nil causes panic in MsgIsNonMutating()")
		})
	}
}

// TestArbTxAsMessage_MsgTx_Set guards against the bug where msg.Tx is nil
// after AsMessage, causing PosterDataCost to fall back to fake estimation.
func TestArbTxAsMessage_MsgTx_Set(t *testing.T) {
	signer := testArbSigner()

	for _, tx := range allArbTxTypes() {
		t.Run(reflect.TypeOf(tx).Elem().Name(), func(t *testing.T) {
			msg, err := tx.AsMessage(signer.Signer, big.NewInt(1000000000), nil)
			require.NoError(t, err)
			require.NotNil(t, msg.Tx,
				"AsMessage must set msg.Tx — nil causes PosterDataCost to use fake DynamicFeeTransaction estimation")
		})
	}
}

// TestArbitrumSigner_SenderPerType verifies that ArbitrumSigner.Sender
// returns the correct address for each Arbitrum tx type. If a new type
// is added but not handled in the switch, Sender falls through to
// cryptographic recovery which panics on unsigned txs.
func TestArbitrumSigner_SenderPerType(t *testing.T) {
	signer := testArbSigner()

	tests := []struct {
		name     string
		tx       Transaction
		expected common.Address
	}{
		{"ArbitrumDepositTx", &ArbitrumDepositTx{From: common.HexToAddress("0x1111")}, common.HexToAddress("0x1111")},
		{"ArbitrumInternalTx", &ArbitrumInternalTx{}, ArbosAddress},
		{"ArbitrumUnsignedTx", &ArbitrumUnsignedTx{From: common.HexToAddress("0x3333")}, common.HexToAddress("0x3333")},
		{"ArbitrumContractTx", &ArbitrumContractTx{From: common.HexToAddress("0x4444")}, common.HexToAddress("0x4444")},
		{"ArbitrumRetryTx", &ArbitrumRetryTx{From: common.HexToAddress("0x5555")}, common.HexToAddress("0x5555")},
		{"ArbitrumSubmitRetryableTx", &ArbitrumSubmitRetryableTx{From: common.HexToAddress("0x7777")}, common.HexToAddress("0x7777")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sender, err := signer.Sender(tt.tx)
			require.NoError(t, err)
			require.Equal(t, accounts.InternAddress(tt.expected), sender)
		})
	}
}

// TestArbTxRLPRoundTrip_AllTypes encodes and decodes each Arbitrum tx type
// and verifies the type byte is in the Arbitrum range. Catches broken
// DecodeTyped dispatch when types are added or type bytes change.
func TestArbTxRLPRoundTrip_AllTypes(t *testing.T) {
	for _, tx := range allArbTxTypes() {
		name := reflect.TypeOf(tx).Elem().Name()
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			err := tx.MarshalBinary(&buf)
			require.NoError(t, err)

			encoded := buf.Bytes()
			require.True(t, len(encoded) > 0)

			typeByte := encoded[0]
			require.True(t, typeByte >= 0x5f && typeByte <= 0x6a,
				"%s: type byte 0x%02x outside Arbitrum range [0x5f, 0x6a]", name, typeByte)
		})
	}
}

// TestArbTxAsMessage_IsFree verifies that only DepositTx and InternalTx
// are free (bypass fee validation). If a non-free type is marked free,
// anyone can execute without paying. If a free type isn't marked,
// system bridge deposits revert on zero-balance sender.
func TestArbTxAsMessage_IsFree(t *testing.T) {
	signer := testArbSigner()

	expectedFree := map[string]bool{
		"ArbitrumDepositTx":         true,
		"ArbitrumInternalTx":        true,
		"ArbitrumUnsignedTx":        false,
		"ArbitrumContractTx":        false,
		"ArbitrumRetryTx":           false,
		"ArbitrumSubmitRetryableTx": false,
	}

	for _, tx := range allArbTxTypes() {
		name := reflect.TypeOf(tx).Elem().Name()
		t.Run(name, func(t *testing.T) {
			msg, err := tx.AsMessage(signer.Signer, big.NewInt(1000000000), nil)
			require.NoError(t, err)
			require.Equal(t, expectedFree[name], msg.IsFree(),
				"%s: isFree mismatch — only Deposit and Internal txs should be free", name)
		})
	}
}

// TestArbTxAsMessage_CheckNonce_MatchesSkipAccountChecks verifies that
// AsMessage's checkNonce flag is consistent with skipAccountChecks.
func TestArbTxAsMessage_CheckNonce_MatchesSkipAccountChecks(t *testing.T) {
	signer := testArbSigner()

	for _, tx := range allArbTxTypes() {
		name := reflect.TypeOf(tx).Elem().Name()
		t.Run(name, func(t *testing.T) {
			msg, err := tx.AsMessage(signer.Signer, big.NewInt(1000000000), nil)
			require.NoError(t, err)
			shouldSkip := skipAccountChecks[tx.Type()]
			require.Equal(t, !shouldSkip, msg.CheckNonce(),
				"%s: checkNonce must be inverse of skipAccountChecks", name)
		})
	}
}

// TestSkipAccountChecks_Correctness verifies the skipAccountChecks array.
// System txs (deposit, internal, contract, retry, retryable) must skip.
// User-facing txs (unsigned, legacy) must NOT skip.
func TestSkipAccountChecks_Correctness(t *testing.T) {
	expected := map[byte]bool{
		ArbitrumDepositTxType:         true,
		ArbitrumRetryTxType:           true,
		ArbitrumSubmitRetryableTxType: true,
		ArbitrumInternalTxType:        true,
		ArbitrumContractTxType:        true,
		ArbitrumUnsignedTxType:        false,
		ArbitrumLegacyTxType:          false,
	}

	for txType, shouldSkip := range expected {
		require.Equal(t, shouldSkip, skipAccountChecks[txType],
			"skipAccountChecks[0x%02x] wrong", txType)
	}
}
