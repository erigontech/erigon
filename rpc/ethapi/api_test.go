package ethapi

import (
	"bytes"
	"encoding/json"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/rpc/jsonstream"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/types"
)

func TestNewRPCTransaction_NullSignature(t *testing.T) {
	to := common.HexToAddress("0x1234567890123456789012345678901234567890")

	tx := &types.LegacyTx{
		CommonTx: types.CommonTx{
			Nonce:    1,
			GasLimit: 21000,
			To:       &to,
		},
	}
	result := NewRPCTransaction(tx, common.Hash{}, 0, 0, 0, nil)
	require.Nil(t, result.V)
	require.Nil(t, result.R)
	require.Nil(t, result.S)
}

func TestNewRPCTransaction_SignedLegacy(t *testing.T) {
	to := common.HexToAddress("0x1234567890123456789012345678901234567890")

	tx := &types.LegacyTx{
		CommonTx: types.CommonTx{
			Nonce:    1,
			GasLimit: 21000,
			To:       &to,
			V:        *uint256.NewInt(27),
			R:        *uint256.NewInt(1),
			S:        *uint256.NewInt(2),
		},
	}
	result := NewRPCTransaction(tx, common.Hash{}, 0, 0, 0, nil)
	require.NotNil(t, result.V)
	require.NotNil(t, result.R)
	require.NotNil(t, result.S)
}

func TestNewRPCTransaction_SignedLegacyEIP155(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	from := crypto.PubkeyToAddress(key.PublicKey)
	to := common.HexToAddress("0x1234567890123456789012345678901234567890")
	chainID := uint256.NewInt(1)
	tx, err := types.SignTx(types.NewTransaction(1, to, uint256.NewInt(0), 21000, uint256.NewInt(1), nil), *types.LatestSignerForChainID(chainID), key)
	require.NoError(t, err)
	result := NewRPCTransaction(tx, common.Hash{}, 0, 0, 0, nil)
	// from is recovered with the chain id derived from the EIP-155 v value.
	require.Equal(t, from, result.From)
	require.NotNil(t, result.ChainID)
	require.Equal(t, chainID.ToBig(), result.ChainID.ToInt())
}

func TestNewRPCTransaction_EIP1559_YParityZero(t *testing.T) {
	to := common.HexToAddress("0x1234567890123456789012345678901234567890")
	chainID := uint256.NewInt(1)

	tx := &types.DynamicFeeTransaction{
		CommonTx: types.CommonTx{
			Nonce:    1,
			GasLimit: 21000,
			To:       &to,
			V:        *uint256.NewInt(0), // yParity=0
			R:        *uint256.NewInt(1),
			S:        *uint256.NewInt(2),
		},
		ChainID: *chainID,
	}
	result := NewRPCTransaction(tx, common.Hash{}, 0, 0, 0, nil)
	require.NotNil(t, result.V)
	require.NotNil(t, result.R)
	require.NotNil(t, result.S)
	require.EqualValues(t, 0, result.V.ToInt().Int64())
}

func TestNewRPCTransaction_EIP1559_AllZeroSig(t *testing.T) {
	to := common.HexToAddress("0x1234567890123456789012345678901234567890")
	chainID := uint256.NewInt(1)

	tx := &types.DynamicFeeTransaction{
		CommonTx: types.CommonTx{
			Nonce:    0,
			GasLimit: 21000,
			To:       &to,
		},
		ChainID: *chainID,
	}
	result := NewRPCTransaction(tx, common.Hash{}, 0, 0, 0, nil)
	require.NotNil(t, result.V)
	require.NotNil(t, result.R)
	require.NotNil(t, result.S)
	require.EqualValues(t, 0, result.V.ToInt().Int64())
	require.EqualValues(t, 0, result.R.ToInt().Int64())
	require.EqualValues(t, 0, result.S.ToInt().Int64())
}

func txFields(t *testing.T, r SignTransactionResult) map[string]json.RawMessage {
	t.Helper()
	data, err := json.Marshal(r)
	require.NoError(t, err)
	var outer struct {
		Tx json.RawMessage `json:"tx"`
	}
	require.NoError(t, json.Unmarshal(data, &outer))
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(outer.Tx, &fields))
	return fields
}

func TestSignTransactionResultMarshalJSON_EIP1559(t *testing.T) {
	to := common.HexToAddress("0x1234567890123456789012345678901234567890")
	chainID := uint256.NewInt(1)
	tx := &types.DynamicFeeTransaction{
		CommonTx: types.CommonTx{
			Nonce:    1,
			GasLimit: 21000,
			To:       &to,
		},
		ChainID: *chainID,
		TipCap:  *uint256.NewInt(1e9),
		FeeCap:  *uint256.NewInt(2e9),
	}
	var buf bytes.Buffer
	require.NoError(t, tx.MarshalBinary(&buf))

	fields := txFields(t, SignTransactionResult{Raw: buf.Bytes(), Tx: NewRPCTransaction(tx, common.Hash{}, 0, 0, 0, nil)})

	// EIP-1559 without known baseFee: gasPrice is null (matching Geth).
	require.Equal(t, "null", string(fields["gasPrice"]), "gasPrice must be null when baseFee is unknown")

	// maxFeePerGas and maxPriorityFeePerGas must be set.
	require.NotEqual(t, "null", string(fields["maxFeePerGas"]))
	require.NotEqual(t, "null", string(fields["maxPriorityFeePerGas"]))

	// unsigned tx: v/r/s must be "0x0", not null.
	require.Equal(t, `"0x0"`, string(fields["v"]))
	require.Equal(t, `"0x0"`, string(fields["r"]))
	require.Equal(t, `"0x0"`, string(fields["s"]))

	// block-placement and sender fields must be absent.
	for _, k := range []string{"from", "blockHash", "blockNumber", "blockTimestamp", "transactionIndex"} {
		_, present := fields[k]
		require.False(t, present, "%s must be absent", k)
	}
}

func TestSignTransactionResultMarshalJSON_Legacy(t *testing.T) {
	to := common.HexToAddress("0x1234567890123456789012345678901234567890")
	tx := &types.LegacyTx{
		CommonTx: types.CommonTx{
			Nonce:    1,
			GasLimit: 21000,
			To:       &to,
		},
		GasPrice: *uint256.NewInt(1e9),
	}
	var buf bytes.Buffer
	require.NoError(t, tx.MarshalBinary(&buf))

	fields := txFields(t, SignTransactionResult{Raw: buf.Bytes(), Tx: NewRPCTransaction(tx, common.Hash{}, 0, 0, 0, nil)})

	// Legacy: gasPrice must be set.
	gasPrice, ok := fields["gasPrice"]
	require.True(t, ok, "gasPrice must be present")
	require.NotEqual(t, "null", string(gasPrice))

	// Legacy: maxFeePerGas and maxPriorityFeePerGas are inapplicable and must be null (matching Geth).
	require.Equal(t, "null", string(fields["maxFeePerGas"]), "maxFeePerGas must be null for legacy tx")
	require.Equal(t, "null", string(fields["maxPriorityFeePerGas"]), "maxPriorityFeePerGas must be null for legacy tx")

	// unsigned tx: v/r/s must be "0x0", not null.
	require.Equal(t, `"0x0"`, string(fields["v"]))
	require.Equal(t, `"0x0"`, string(fields["r"]))
	require.Equal(t, `"0x0"`, string(fields["s"]))
}

func TestSignTransactionResultMarshalJSON_NilTx(t *testing.T) {
	_, err := json.Marshal(SignTransactionResult{Tx: nil})
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil transaction")
}

func TestNewRPCTransaction_AccessList_AllZeroSig(t *testing.T) {
	to := common.HexToAddress("0x1234567890123456789012345678901234567890")
	chainID := uint256.NewInt(1)

	tx := &types.AccessListTx{
		LegacyTx: types.LegacyTx{
			CommonTx: types.CommonTx{
				Nonce:    0,
				GasLimit: 21000,
				To:       &to,
			},
		},
		ChainID: *chainID,
	}
	result := NewRPCTransaction(tx, common.Hash{}, 0, 0, 0, nil)
	require.NotNil(t, result.V)
	require.NotNil(t, result.R)
	require.NotNil(t, result.S)
	require.EqualValues(t, 0, result.V.ToInt().Int64())
	require.EqualValues(t, 0, result.R.ToInt().Int64())
	require.EqualValues(t, 0, result.S.ToInt().Int64())
}

// A blob call with no maxFeePerBlobGas (debug_traceCall accepts one) must
// default the cap to zero rather than dereference the missing field.
func TestToTransactionBlobWithoutMaxFeePerBlobGas(t *testing.T) {
	to := common.HexToAddress("0x1234567890123456789012345678901234567890")
	args := CallArgs{
		To:                  &to,
		BlobVersionedHashes: []common.Hash{{1}},
	}

	txn, err := args.ToTransaction(1_000_000, uint256.NewInt(7))
	require.NoError(t, err)
	blobTx, ok := txn.(*types.BlobTx)
	require.True(t, ok)
	require.True(t, blobTx.MaxFeePerBlobGas.IsZero())
}

// RPCMarshalHeader aliases the header it is given: the U256 quantities, the
// extraData slice and the fields callers may null out all point straight at it.
// RPCMarshalBlock must therefore hand it a copy — the result is mutable and
// exported, and the block keeps its memoized hash, so a caller writing through
// the result would otherwise leave the block describing itself wrongly.
func TestRPCMarshalBlockDoesNotAliasBlockHeader(t *testing.T) {
	header := &types.Header{
		Number:     *uint256.NewInt(7),
		Difficulty: *uint256.NewInt(11),
		BaseFee:    uint256.NewInt(13),
		Extra:      []byte{1, 2, 3},
		Coinbase:   common.HexToAddress("0x1234567890123456789012345678901234567890"),
	}
	block := types.NewBlock(header, nil, nil, nil, nil, nil)
	wantHash := block.Hash()
	wantMiner := block.Coinbase()

	fields := RPCMarshalBlock(block, false, false)

	(*uint256.Int)(fields.Number).SetUint64(99)
	(*uint256.Int)(fields.Difficulty).SetUint64(99)
	(*uint256.Int)(fields.BaseFeePerGas).SetUint64(99)
	fields.ExtraData[0] = 0xff
	fields.Miner[0] = 0xff
	fields.Nonce[0] = 0xff
	fields.LogsBloom[0] = 0xff

	require.Equal(t, uint64(7), block.NumberU64())
	require.Equal(t, uint64(11), block.HeaderNoCopy().Difficulty.Uint64())
	require.Equal(t, uint64(13), block.BaseFee().Uint64())
	require.Equal(t, []byte{1, 2, 3}, block.Extra())
	require.Equal(t, wantMiner, block.Coinbase())
	require.Equal(t, types.BlockNonce{}, block.HeaderNoCopy().Nonce)
	require.Equal(t, types.Bloom{}, block.HeaderNoCopy().Bloom)
	require.Equal(t, wantHash, block.Hash())
}

type jsonSink []byte

func (s *jsonSink) Write(p []byte) (int, error) { *s = append(*s, p...); return len(p), nil }

// fastHeaderJSON renders h through MarshalFastJSONTo on a pooled stream, as the server does.
func fastHeaderJSON(t *testing.T, h *RPCHeader) string {
	t.Helper()
	var b jsonSink
	s := jsonstream.Get(&b)
	defer jsonstream.Put(s)
	require.NoError(t, h.MarshalFastJSONTo(s))
	require.NoError(t, s.Flush())
	return string(b)
}

func fullRPCHeader() *RPCHeader {
	num := hexutil.U256(*uint256.NewInt(0x1234))
	diff := hexutil.U256(*uint256.NewInt(0x11))
	baseFee := hexutil.U256(*uint256.NewInt(0x7))
	hash := common.HexToHash("0xaabb")
	miner := common.HexToAddress("0x1234567890123456789012345678901234567890")
	nonce := types.BlockNonce{1, 2, 3, 4, 5, 6, 7, 8}
	bloom := types.Bloom{9, 8, 7}
	quantity := hexutil.Uint64(0x20)
	root := common.HexToHash("0xccdd")
	seal := hexutil.Bytes{0xbe, 0xef}
	return &RPCHeader{
		Number: &num, Hash: &hash, ParentHash: common.HexToHash("0x01"),
		Nonce: &nonce, MixHash: common.HexToHash("0x02"), Sha3Uncles: common.HexToHash("0x03"),
		LogsBloom: &bloom, StateRoot: common.HexToHash("0x04"), Miner: &miner, Difficulty: &diff,
		ExtraData: hexutil.Bytes{0xde, 0xad}, GasLimit: 0x5208, GasUsed: 0x5207, Timestamp: 0x64,
		TransactionsRoot: common.HexToHash("0x05"), ReceiptsRoot: common.HexToHash("0x06"),
		BaseFeePerGas: &baseFee, WithdrawalsRoot: &root, BlobGasUsed: &quantity, ExcessBlobGas: &quantity,
		ParentBeaconBlockRoot: &root, RequestsHash: &root, BlockAccessListHash: &root, SlotNumber: &quantity,
		AuraSeal: &seal, AuraStep: &quantity,
	}
}

// TestRPCHeaderMarshalFastJSONTo requires the streamed encoding to be byte-identical to
// the reflection one; that equality is the only thing that makes it safe to swap in.
func TestRPCHeaderMarshalFastJSONTo(t *testing.T) {
	bare := fullRPCHeader()
	bare.BaseFeePerGas, bare.WithdrawalsRoot, bare.BlobGasUsed, bare.ExcessBlobGas = nil, nil, nil, nil
	bare.ParentBeaconBlockRoot, bare.RequestsHash, bare.BlockAccessListHash, bare.SlotNumber = nil, nil, nil, nil
	bare.AuraSeal, bare.AuraStep = nil, nil

	pending := fullRPCHeader()
	pending.Hash, pending.Nonce, pending.Miner = nil, nil, nil

	emptyExtra := fullRPCHeader()
	emptyExtra.ExtraData = hexutil.Bytes{}

	nilExtra := fullRPCHeader()
	nilExtra.ExtraData = nil

	for _, tc := range []struct {
		name string
		h    *RPCHeader
	}{
		{"all fields set", fullRPCHeader()},
		{"optionals absent", bare},
		{"pending drops hash, nonce and miner", pending},
		{"empty extra data", emptyExtra},
		{"nil extra data", nilExtra},
		{"zero value", &RPCHeader{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want, err := json.Marshal(tc.h)
			require.NoError(t, err)
			require.Equal(t, string(want), fastHeaderJSON(t, tc.h))
		})
	}
}

func fastBlockJSON(t *testing.T, b *RPCBlock) string {
	t.Helper()
	var buf jsonSink
	s := jsonstream.Get(&buf)
	defer jsonstream.Put(s)
	require.NoError(t, b.MarshalFastJSONTo(s))
	require.NoError(t, s.Flush())
	return string(buf)
}

// TestRPCBlockMarshalFastJSONTo guards the whole block, not just the header: RPCBlock
// embeds RPCHeader, so a missing block marshaller would promote the header's and emit a
// block with no transactions at all.
func TestRPCBlockMarshalFastJSONTo(t *testing.T) {
	to := common.HexToAddress("0x1234567890123456789012345678901234567890")
	signer := types.LatestSignerForChainID(nil)
	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	txn, err := types.SignTx(&types.LegacyTx{
		CommonTx: types.CommonTx{Nonce: 1, GasLimit: 21000, To: &to, Value: *uint256.NewInt(5)},
		GasPrice: *uint256.NewInt(1),
	}, *signer, key)
	require.NoError(t, err)

	header := &types.Header{Number: *uint256.NewInt(7), Difficulty: *uint256.NewInt(11)}
	withTx := types.NewBlock(header, []types.Transaction{txn}, nil, nil, types.Withdrawals{}, nil)
	empty := types.NewBlock(header, nil, nil, nil, nil, nil)

	count := 1
	for _, tc := range []struct {
		name string
		b    *RPCBlock
	}{
		{"hashes", RPCMarshalBlock(withTx, true, false)},
		{"full transactions", RPCMarshalBlock(withTx, true, true)},
		{"no transactions", RPCMarshalBlock(empty, true, false)},
		{"uncle form", RPCMarshalBlock(empty, false, false)},
		{"pending", func() *RPCBlock { b := RPCMarshalBlock(withTx, true, false); b.MarkPending(); return b }()},
		{"typed nil hash slice", func() *RPCBlock {
			b := RPCMarshalBlock(withTx, true, false)
			var none []common.Hash
			b.Transactions = none // typed nil: a non-nil any that json renders as null
			return b
		}()},
		{"nil uncles", func() *RPCBlock {
			b := RPCMarshalBlock(withTx, true, false)
			b.Uncles = nil
			return b
		}()},
		{"otterscan shape", func() *RPCBlock {
			b := RPCMarshalBlock(withTx, true, false)
			b.TransactionCount = count
			b.LogsBloom = nil
			return b
		}()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want, err := json.Marshal(tc.b)
			require.NoError(t, err)
			require.Equal(t, string(want), fastBlockJSON(t, tc.b))
		})
	}
}
