package ethapi

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/types"
)

// legacyRPCMarshalHeader is the map[string]any builder this package used before
// RPCHeader. The tests keep it as an oracle: the typed output must still encode
// to the same JSON object on every chain.
func legacyRPCMarshalHeader(head *types.Header) map[string]any {
	result := map[string]any{
		"number":           (*hexutil.U256)(&head.Number),
		"hash":             head.Hash(),
		"parentHash":       head.ParentHash,
		"nonce":            head.Nonce,
		"mixHash":          head.MixDigest,
		"sha3Uncles":       head.UncleHash,
		"logsBloom":        head.Bloom,
		"stateRoot":        head.Root,
		"miner":            head.Coinbase,
		"difficulty":       (*hexutil.U256)(&head.Difficulty),
		"extraData":        hexutil.Bytes(head.Extra),
		"size":             hexutil.Uint64(head.Size()),
		"gasLimit":         hexutil.Uint64(head.GasLimit),
		"gasUsed":          hexutil.Uint64(head.GasUsed),
		"timestamp":        hexutil.Uint64(head.Time),
		"transactionsRoot": head.TxHash,
		"receiptsRoot":     head.ReceiptHash,
	}
	if head.BaseFee != nil {
		result["baseFeePerGas"] = (*hexutil.U256)(head.BaseFee)
	}
	if head.WithdrawalsHash != nil {
		result["withdrawalsRoot"] = head.WithdrawalsHash
	}
	if head.BlobGasUsed != nil {
		result["blobGasUsed"] = (*hexutil.Uint64)(head.BlobGasUsed)
	}
	if head.ExcessBlobGas != nil {
		result["excessBlobGas"] = (*hexutil.Uint64)(head.ExcessBlobGas)
	}
	if head.ParentBeaconBlockRoot != nil {
		result["parentBeaconBlockRoot"] = head.ParentBeaconBlockRoot
	}
	if head.RequestsHash != nil {
		result["requestsHash"] = head.RequestsHash
	}
	if head.BlockAccessListHash != nil {
		result["blockAccessListHash"] = head.BlockAccessListHash
	}
	if head.SlotNumber != nil {
		result["slotNumber"] = (*hexutil.Uint64)(head.SlotNumber)
	}
	if head.AuRaSeal != nil {
		result["auraSeal"] = hexutil.Bytes(head.AuRaSeal)
		result["auraStep"] = (hexutil.Uint64)(head.AuRaStep)
	}
	return result
}

func legacyRPCMarshalBlock(block *types.Block, inclTx bool, fullTx bool) map[string]any {
	fields := legacyRPCMarshalHeader(block.Header())
	fields["size"] = hexutil.Uint64(block.Size())
	fields["transactions"] = make([]any, 0)
	if inclTx {
		txs := block.Transactions()
		transactions := make([]any, len(txs))
		for i, txn := range txs {
			if fullTx {
				transactions[i] = newRPCTransactionFromBlockAndTxGivenIndex(block, txn, uint64(i))
			} else {
				transactions[i] = txn.Hash()
			}
		}
		fields["transactions"] = transactions
	}
	uncles := block.Uncles()
	uncleHashes := make([]common.Hash, len(uncles))
	for i, uncle := range uncles {
		uncleHashes[i] = uncle.Hash()
	}
	fields["uncles"] = uncleHashes
	if block.Withdrawals() != nil {
		fields["withdrawals"] = block.Withdrawals()
	}
	return fields
}

func requireSameJSONObject(t *testing.T, name string, want, got any) {
	t.Helper()
	decode := func(v any) map[string]json.RawMessage {
		encoded, err := json.Marshal(v)
		require.NoError(t, err, name)
		var fields map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(encoded, &fields), name)
		return fields
	}
	wantFields, gotFields := decode(want), decode(got)
	for key, wantValue := range wantFields {
		gotValue, ok := gotFields[key]
		require.True(t, ok, "%s: field %q missing", name, key)
		require.JSONEq(t, string(wantValue), string(gotValue), "%s: field %q", name, key)
	}
	for key := range gotFields {
		require.Contains(t, wantFields, key, "%s: unexpected field %q", name, key)
	}
}

func headerVariants() map[string]*types.Header {
	hash := common.HexToHash("0x6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f")
	extras := map[string][]byte{"extraNil": nil, "extraEmpty": {}, "extraSet": []byte("erigon")}
	seals := map[string][]byte{"sealNil": nil, "sealEmpty": {}, "sealSet": {1, 2, 3}}
	numbers := map[string]uint64{"zero": 0, "nonzero": 7}

	out := map[string]*types.Header{}
	for extraName, extra := range extras {
		for sealName, seal := range seals {
			for numName, n := range numbers {
				for optional := range 1 << 8 {
					head := &types.Header{
						Number:     *uint256.NewInt(n),
						Difficulty: *uint256.NewInt(n),
						GasLimit:   n,
						GasUsed:    n,
						Time:       n,
						Extra:      extra,
						AuRaSeal:   seal,
						AuRaStep:   n,
					}
					value := n
					if optional&1 != 0 {
						head.BaseFee = uint256.NewInt(n)
					}
					if optional&2 != 0 {
						head.WithdrawalsHash = &hash
					}
					if optional&4 != 0 {
						head.BlobGasUsed = &value
					}
					if optional&8 != 0 {
						head.ExcessBlobGas = &value
					}
					if optional&16 != 0 {
						head.ParentBeaconBlockRoot = &hash
					}
					if optional&32 != 0 {
						head.RequestsHash = &hash
					}
					if optional&64 != 0 {
						head.BlockAccessListHash = &hash
					}
					if optional&128 != 0 {
						head.SlotNumber = &value
					}
					out[fmt.Sprintf("%s/%s/%s/%d", extraName, sealName, numName, optional)] = head
				}
			}
		}
	}
	return out
}

// TestRPCMarshalHeaderMatchesLegacyJSON sweeps every combination of the optional
// header fields, including the Gnosis AuRa seal and the empty-but-not-nil slices
// that decide whether a field is omitted. A header reaches a response only through
// RPCMarshalBlock, so the comparison runs through it.
func TestRPCMarshalHeaderMatchesLegacyJSON(t *testing.T) {
	for name, head := range headerVariants() {
		block := types.NewBlockWithHeader(head, nil)
		requireSameJSONObject(t, name, legacyRPCMarshalBlock(block, false, false), RPCMarshalBlock(block, false, false))
	}
}

func TestRPCMarshalBlockMatchesLegacyJSON(t *testing.T) {
	withdrawal := &types.Withdrawal{Index: 1, Validator: 2, Address: common.HexToAddress("0x01"), Amount: 3}
	for _, withdrawals := range []struct {
		name  string
		value types.Withdrawals
	}{
		{name: "nil", value: nil},
		{name: "empty", value: types.Withdrawals{}},
		{name: "set", value: types.Withdrawals{withdrawal}},
	} {
		for _, uncleCount := range []int{0, 1} {
			for _, tx := range []struct{ inclTx, fullTx bool }{{false, false}, {true, false}, {true, true}} {
				uncles := make([]*types.Header, uncleCount)
				for i := range uncles {
					uncles[i] = &types.Header{Number: *uint256.NewInt(uint64(i))}
				}
				header := &types.Header{Number: *uint256.NewInt(9), Difficulty: *uint256.NewInt(1), BaseFee: uint256.NewInt(7)}
				txs := []types.Transaction{pinnedLegacyTx()}
				block := types.NewBlock(header, txs, uncles, nil, withdrawals.value, nil)
				name := fmt.Sprintf("withdrawals=%s/uncles=%d/inclTx=%v/fullTx=%v", withdrawals.name, uncleCount, tx.inclTx, tx.fullTx)
				requireSameJSONObject(t, name, legacyRPCMarshalBlock(block, tx.inclTx, tx.fullTx), RPCMarshalBlock(block, tx.inclTx, tx.fullTx))
			}
		}
	}
}

func pinnedLegacyTx() types.Transaction {
	to := common.HexToAddress("0x1234567890123456789012345678901234567890")
	txn := &types.LegacyTx{
		CommonTx: types.CommonTx{Nonce: 1, GasLimit: 21000, To: &to, Value: *uint256.NewInt(1)},
		GasPrice: *uint256.NewInt(100),
	}
	txn.CommonTx.V = *uint256.NewInt(38)
	return txn
}
