package ethapi

import (
	"encoding/json"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func BenchmarkNewRPCTransaction(b *testing.B) {
	baseFee := uint256.NewInt(7)

	legacy := &types.LegacyTx{CommonTx: pinCommonTx(5), GasPrice: *uint256.NewInt(100)}
	legacy.CommonTx.V = *uint256.NewInt(38)

	dynamicFee := &types.DynamicFeeTransaction{
		CommonTx: pinCommonTx(9),
		ChainID:  *uint256.NewInt(1),
		TipCap:   *uint256.NewInt(10),
		FeeCap:   *uint256.NewInt(1000),
	}

	blob := &types.BlobTx{
		DynamicFeeTransaction: types.DynamicFeeTransaction{
			CommonTx: pinCommonTx(10),
			ChainID:  *uint256.NewInt(1),
			TipCap:   *uint256.NewInt(10),
			FeeCap:   *uint256.NewInt(1000),
		},
		MaxFeePerBlobGas:    *uint256.NewInt(50),
		BlobVersionedHashes: []common.Hash{common.HexToHash("0x0102")},
	}

	for _, bb := range []struct {
		name string
		txn  types.Transaction
	}{
		{name: "legacy", txn: legacy},
		{name: "dynamicfee", txn: dynamicFee},
		{name: "blob", txn: blob},
	} {
		bb.txn.SetSender(accounts.InternAddress(pinSender))
		bb.txn.Hash()
		b.Run(bb.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				out, err := json.Marshal(NewRPCTransaction(bb.txn, pinBlockHash, 1000, 100, 3, baseFee))
				if err != nil || len(out) == 0 {
					b.Fatal(err)
				}
			}
		})
	}
}
