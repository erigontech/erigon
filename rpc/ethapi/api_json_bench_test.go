package ethapi

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func benchHeader(prague bool) *types.Header {
	head := &types.Header{
		ParentHash:  common.HexToHash("0x1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f"),
		UncleHash:   empty.UncleHash,
		Coinbase:    common.HexToAddress("0x95222290dd7278aa3ddd389cc1e1d165cc4bafe5"),
		Root:        common.HexToHash("0x2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f2f"),
		TxHash:      common.HexToHash("0x3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f3f"),
		ReceiptHash: common.HexToHash("0x4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f4f"),
		Number:      *uint256.NewInt(21_000_000),
		GasLimit:    36_000_000,
		GasUsed:     17_123_456,
		Time:        1_750_000_000,
		Extra:       []byte("erigon"),
		MixDigest:   common.HexToHash("0x5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f"),
		BaseFee:     uint256.NewInt(1_234_567_890),
	}
	for i := range head.Bloom {
		head.Bloom[i] = byte(i)
	}
	if prague {
		withdrawals := common.HexToHash("0x6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f")
		beaconRoot := common.HexToHash("0x7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f")
		requests := common.HexToHash("0x8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f8f")
		blobGasUsed, excessBlobGas := uint64(786432), uint64(0)
		head.WithdrawalsHash, head.ParentBeaconBlockRoot, head.RequestsHash = &withdrawals, &beaconRoot, &requests
		head.BlobGasUsed, head.ExcessBlobGas = &blobGasUsed, &excessBlobGas
	}
	head.Hash()
	return head
}

func BenchmarkRPCMarshalHeader(b *testing.B) {
	for _, tc := range []struct {
		name string
		head *types.Header
	}{
		{name: "london", head: benchHeader(false)},
		{name: "prague", head: benchHeader(true)},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				out, err := json.Marshal(rpcMarshalHeader(tc.head, tc.head.Hash()))
				if err != nil || len(out) == 0 {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkRPCMarshalBlock(b *testing.B) {
	for _, txCount := range []int{0, 1, 200} {
		txs := make([]types.Transaction, txCount)
		for i := range txs {
			txs[i] = pinnedLegacyTx()
			txs[i].SetSender(accounts.InternAddress(pinSender))
			txs[i].Hash()
		}
		block := types.NewBlock(benchHeader(true), txs, nil, nil, types.Withdrawals{}, nil)
		block.Hash()

		b.Run(fmt.Sprintf("txs=%d", txCount), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				out, err := json.Marshal(RPCMarshalBlock(block, true, true))
				if err != nil || len(out) == 0 {
					b.Fatal(err)
				}
			}
		})
	}
}
