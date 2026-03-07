package types

import (
	"bytes"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

func TestArbSepoliaReceiptHash(t *testing.T) {
	// Block 241934433 (0xe6ba061) on arb-sepolia
	// Expected receipt hash from reference node
	expectedHash := common.HexToHash("0xcff129e7e29449de7b5c58e99f987104e7026c201cd27c6f894f5df23d79730e")

	// tx0: type 0x6a (ArbitrumInternalTx), gasUsed=0, status=success, no logs
	r0 := &Receipt{
		Type:              ArbitrumInternalTxType,
		Status:            ReceiptStatusSuccessful,
		CumulativeGasUsed: 0,
		Logs:              []*Log{},
	}

	// tx1: type 0x69 (ArbitrumSubmitRetryableTx), gasUsed=27511, status=success, 2 logs
	r1 := &Receipt{
		Type:              ArbitrumSubmitRetryableTxType,
		Status:            ReceiptStatusSuccessful,
		CumulativeGasUsed: 27511,
		Logs: []*Log{
			{
				Address: common.HexToAddress("0x000000000000000000000000000000000000006e"),
				Topics: []common.Hash{
					common.HexToHash("0x7c793cced5743dc5f531bbe2bfb5a9fa3f40adef29231e6ab165c08a29e3dd89"),
					common.HexToHash("0x9d2a65b8f9f1fe255bc865bbe2ea6be6a4cb9da4d19f2aec6d33a0a17c2a5a47"),
				},
				Data: []byte{},
			},
			{
				Address: common.HexToAddress("0x000000000000000000000000000000000000006e"),
				Topics: []common.Hash{
					common.HexToHash("0x5ccd009502509cf28762c67858994d85b163bb6e451f5e9df7c5e18c9c2e123e"),
					common.HexToHash("0x9d2a65b8f9f1fe255bc865bbe2ea6be6a4cb9da4d19f2aec6d33a0a17c2a5a47"),
					common.HexToHash("0x6f78774cff7b565a4c4d32dcf9d6ea3af0abb2780fa2783ce6f0fc46af927d48"),
					common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000"),
				},
				Data: common.FromHex("0x0000000000000000000000000000000000000000000000000000000000006b77000000000000000000000000230e1ca90ff285b6eb37794ad781629fc77c9842000000000000000000000000000000000000000000000000000006afe1816bd00000000000000000000000000000000000000000000000000000062fab99f350"),
			},
		},
	}

	// tx2: type 0x68 (ArbitrumRetryTx), gasUsed=21000, status=success, no logs
	r2 := &Receipt{
		Type:              ArbitrumRetryTxType,
		Status:            ReceiptStatusSuccessful,
		CumulativeGasUsed: 48511,
		Logs:              []*Log{},
	}

	receipts := Receipts{r0, r1, r2}

	// Set blooms like BlockPostValidation does
	for _, r := range receipts {
		r.Bloom = CreateBloom(Receipts{r})
	}

	receiptHash := DeriveSha(receipts)

	t.Logf("Computed receipt hash: %s", receiptHash.Hex())
	t.Logf("Expected receipt hash: %s", expectedHash.Hex())

	if receiptHash != expectedHash {
		// Try with LegacyTxType encoding (no type prefix)
		r0copy := *r0
		r1copy := *r1
		r2copy := *r2
		r0copy.Type = LegacyTxType
		r1copy.Type = LegacyTxType
		r2copy.Type = LegacyTxType
		legacyReceipts := Receipts{&r0copy, &r1copy, &r2copy}
		for _, r := range legacyReceipts {
			r.Bloom = CreateBloom(Receipts{r})
		}
		legacyHash := DeriveSha(legacyReceipts)
		t.Logf("Legacy (no type prefix) hash: %s", legacyHash.Hex())

		if legacyHash == expectedHash {
			t.Fatal("Receipt hash matches with LegacyTxType! Arbitrum receipts should NOT use type prefix in EncodeIndex.")
		}
	}

	require.Equal(t, expectedHash, receiptHash,
		"Receipt hash mismatch for arb-sepolia block 241934433. "+
			"Check EncodeIndex encoding for Arbitrum receipt types.")
}

func TestArbReceiptEncodeIndex(t *testing.T) {
	// Verify that Arbitrum receipt types use type-prefixed encoding in EncodeIndex
	r := &Receipt{
		Type:              ArbitrumInternalTxType,
		Status:            ReceiptStatusSuccessful,
		CumulativeGasUsed: 0,
		Bloom:             Bloom{},
		Logs:              []*Log{},
	}

	receipts := Receipts{r}
	var buf1 bytes.Buffer
	receipts.EncodeIndex(0, &buf1)
	encoded := buf1.Bytes()

	t.Logf("Encoded receipt (type 0x%02x): %x", ArbitrumInternalTxType, encoded[:min(20, len(encoded))])
	t.Logf("First byte: 0x%02x", encoded[0])

	// Check what the first byte is — it tells us whether type prefix is used
	if encoded[0] == ArbitrumInternalTxType {
		t.Log("Using type-prefixed encoding (EIP-2718 style)")
	} else if encoded[0] >= 0xc0 {
		t.Log("Using legacy (no type prefix) encoding")
	}
}
