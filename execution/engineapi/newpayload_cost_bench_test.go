package engineapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/protocol/rules/merge"
	"github.com/erigontech/erigon/execution/types"
)

// benchTxs builds n signed dynamic-fee transactions each carrying dataLen bytes
// of calldata, returned in canonical EIP-2718 binary form (what the CL sends).
func benchTxs(tb testing.TB, n, dataLen int) [][]byte {
	tb.Helper()
	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	if err != nil {
		tb.Fatal(err)
	}
	signer := types.LatestSignerForChainID(uint256.NewInt(1))
	to := common.HexToAddress("0x1111111111111111111111111111111111111111")
	out := make([][]byte, n)
	var buf bytes.Buffer
	for i := range n {
		data := make([]byte, dataLen)
		for j := range data {
			data[j] = byte(i + j)
		}
		txn := &types.DynamicFeeTransaction{
			CommonTx: types.CommonTx{
				Nonce:    uint64(i),
				GasLimit: 100_000,
				To:       &to,
				Value:    *uint256.NewInt(1),
				Data:     data,
			},
			ChainID: *uint256.NewInt(1),
			TipCap:  *uint256.NewInt(1_000_000_000),
			FeeCap:  *uint256.NewInt(20_000_000_000),
		}
		signed := types.MustSignNewTx(key, *signer, txn)
		buf.Reset()
		if err := signed.MarshalBinary(&buf); err != nil {
			tb.Fatal(err)
		}
		out[i] = bytes.Clone(buf.Bytes())
	}
	return out
}

func benchHeader(txs [][]byte) *types.Header {
	h := &types.Header{
		ParentHash:  common.Hash{0x01},
		Coinbase:    common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Root:        common.Hash{0x03},
		BaseFee:     uint256.NewInt(1_000_000_000),
		Extra:       []byte("erigon"),
		GasUsed:     30_000_000,
		GasLimit:    45_000_000,
		Time:        1_700_000_000,
		UncleHash:   empty.UncleHash,
		Difficulty:  *merge.ProofOfStakeDifficulty,
		Nonce:       merge.ProofOfStakeNonce,
		ReceiptHash: common.Hash{0x04},
		TxHash:      types.DeriveSha(types.BinaryTransactions(txs)),
	}
	h.Number.SetUint64(21_000_000)
	return h
}

// benchSizes brackets real mainnet block shapes so the cost curve against
// payload size is visible.
var benchSizes = []struct {
	name    string
	txs     int
	dataLen int
}{
	{"txs50/data100", 50, 100},
	{"txs200/data300", 200, 300},
	{"txs400/data500", 400, 500},
}

func BenchmarkNPDeriveShaTxs(b *testing.B) {
	for _, s := range benchSizes {
		txs := benchTxs(b, s.txs, s.dataLen)
		b.Run(s.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = types.DeriveSha(types.BinaryTransactions(txs))
			}
		})
	}
}

func BenchmarkNPDecodeTransactions(b *testing.B) {
	for _, s := range benchSizes {
		txs := benchTxs(b, s.txs, s.dataLen)
		b.Run(s.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := types.DecodeTransactions(txs); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkNPTypedRlpStringScan(b *testing.B) {
	for _, s := range benchSizes {
		txs := benchTxs(b, s.txs, s.dataLen)
		b.Run(s.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				for _, txn := range txs {
					if types.TypedTransactionMarshalledAsRlpString(txn) {
						b.Fatal("unexpected")
					}
				}
			}
		})
	}
}

// BenchmarkNPHeaderHash builds a fresh header every iteration: newPayload hashes
// a header it has just assembled, so the atomic hash cache is always cold.
func BenchmarkNPHeaderHash(b *testing.B) {
	txs := benchTxs(b, 200, 300)
	txHash := types.DeriveSha(types.BinaryTransactions(txs))
	b.ReportAllocs()
	for b.Loop() {
		h := types.Header{
			ParentHash:  common.Hash{0x01},
			Coinbase:    common.HexToAddress("0x2222222222222222222222222222222222222222"),
			Root:        common.Hash{0x03},
			BaseFee:     uint256.NewInt(1_000_000_000),
			Extra:       []byte("erigon"),
			GasUsed:     30_000_000,
			GasLimit:    45_000_000,
			Time:        1_700_000_000,
			UncleHash:   empty.UncleHash,
			Difficulty:  *merge.ProofOfStakeDifficulty,
			Nonce:       merge.ProofOfStakeNonce,
			ReceiptHash: common.Hash{0x04},
			TxHash:      txHash,
		}
		h.Number.SetUint64(21_000_000)
		_ = h.Hash()
	}
}

// BenchmarkNPBlockAndRawBody covers what newPayload + InsertBlocks do with the
// decoded txs: wrap them into a Block and re-derive the RawBody written to the
// overlay.
func BenchmarkNPBlockAndRawBody(b *testing.B) {
	for _, s := range benchSizes {
		txs := benchTxs(b, s.txs, s.dataLen)
		decoded, err := types.DecodeTransactions(txs)
		if err != nil {
			b.Fatal(err)
		}
		h := benchHeader(txs)
		hash := h.Hash()
		b.Run(s.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				blk := types.NewBlockFromStorageWithBinaryTxs(hash, h, decoded, txs, nil, nil, nil)
				_ = blk.RawBody()
			}
		})
	}
}

// BenchmarkNPJSONDecode measures the RPC-layer cost of turning the
// engine_newPayload JSON params into an ExecutionPayload. This happens before
// newPayload() is entered, so it is outside engineNewPayloadDuration.
func BenchmarkNPJSONDecode(b *testing.B) {
	for _, s := range benchSizes {
		txs := benchTxs(b, s.txs, s.dataLen)
		hexTxs := make([]hexutil.Bytes, len(txs))
		for i := range txs {
			hexTxs[i] = txs[i]
		}
		h := benchHeader(txs)
		bloom := h.Bloom
		ep := &engine_types.ExecutionPayload{
			ParentHash:    h.ParentHash,
			FeeRecipient:  h.Coinbase,
			StateRoot:     h.Root,
			ReceiptsRoot:  h.ReceiptHash,
			LogsBloom:     bloom[:],
			PrevRandao:    h.MixDigest,
			BlockNumber:   hexutil.Uint64(h.Number.Uint64()),
			GasLimit:      hexutil.Uint64(h.GasLimit),
			GasUsed:       hexutil.Uint64(h.GasUsed),
			Timestamp:     hexutil.Uint64(h.Time),
			ExtraData:     h.Extra,
			BaseFeePerGas: (*hexutil.Big)(h.BaseFee.ToBig()),
			BlockHash:     h.Hash(),
			Transactions:  hexTxs,
		}
		raw, err := json.Marshal(ep)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(fmt.Sprintf("%s/json%dKB", s.name, len(raw)/1024), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				var got engine_types.ExecutionPayload
				if err := json.Unmarshal(raw, &got); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
