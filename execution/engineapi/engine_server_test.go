// Copyright 2025 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package engineapi

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	goethkzg "github.com/crate-crypto/go-eth-kzg"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcservices"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/engineapi/engine_block_downloader"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/jsonrpc"
	"github.com/erigontech/erigon/rpc/jsonstream"
	"github.com/erigontech/erigon/rpc/rpccfg"
	"github.com/erigontech/erigon/rpc/rpchelper"
	"github.com/erigontech/erigon/txnprovider/txpool"
)

func TestEngineRejectsMissingRequiredObjects(t *testing.T) {
	t.Parallel()

	srv := NewEngineServer(log.New(), &chain.Config{}, nil, nil, false, false, false, true, nil, nil, 0, 0)
	client := newEngineInProcClient(t, srv)

	for _, test := range []struct {
		method string
		params []any
	}{
		{"engine_newPayloadV1", []any{nil}},
		{"engine_newPayloadV2", []any{nil}},
		{"engine_newPayloadV3", []any{nil, []common.Hash{}, common.Hash{}}},
		{"engine_newPayloadV4", []any{nil, []common.Hash{}, common.Hash{}, []hexutil.Bytes{}}},
		{"engine_newPayloadV5", []any{nil, []common.Hash{}, common.Hash{}, []hexutil.Bytes{}}},
		{"engine_forkchoiceUpdatedV1", []any{nil, nil}},
		{"engine_forkchoiceUpdatedV2", []any{nil, nil}},
		{"engine_forkchoiceUpdatedV3", []any{nil, nil}},
		{"engine_forkchoiceUpdatedV4", []any{nil, nil, nil}},
	} {
		t.Run(test.method, func(t *testing.T) {
			for _, params := range []struct {
				name string
				args []any
			}{
				{"null", test.params},
				{"empty", []any{}},
				{"omitted", nil},
			} {
				t.Run(params.name, func(t *testing.T) {
					err := client.CallContext(t.Context(), new(any), test.method, params.args...)
					var rpcErr rpc.Error
					require.ErrorAs(t, err, &rpcErr)
					require.Equal(t, -32602, rpcErr.ErrorCode())
				})
			}
		})
	}
}

func TestNewPayloadV5BoundsBALAllocations(t *testing.T) {
	reads := make([]uint64, 100_000)
	for i := range reads {
		reads[i] = uint64(i)
	}
	raw, err := rlp.EncodeToBytes([]any{[]any{
		common.Address{}, []any{}, reads, []any{}, []any{}, []any{},
	}})
	require.NoError(t, err)

	header := makeParentHeader(1)
	header.UncleHash = empty.UncleHash
	header.TxHash = empty.RootHash
	header.GasLimit = types.BalItemCost
	withdrawals := types.Withdrawals{}
	withdrawalsHash := types.DeriveSha(withdrawals)
	header.WithdrawalsHash = &withdrawalsHash
	header.BlobGasUsed = new(uint64)
	header.ExcessBlobGas = new(uint64)
	header.ParentBeaconBlockRoot = &common.Hash{}
	header.RequestsHash = (types.FlatRequests{}).Hash()
	balHash := crypto.Keccak256Hash(raw)
	header.BlockAccessListHash = &balHash
	header.SlotNumber = new(uint64)
	block := types.NewBlockFromStorage(header.Hash(), header, nil, nil, withdrawals, nil)
	payload, err := engine_types.ExecutionPayloadFromBlock(block)
	require.NoError(t, err)
	balBytes := hexutil.Bytes(raw)
	payload.BlockAccessList = &balBytes
	srv := NewEngineServer(log.New(), allForksChainConfig(), &stubExecutionModule{}, nil, false, false, false, true, nil, nil, 0, 0)

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	status, err := srv.NewPayloadV5(t.Context(), payload, []common.Hash{}, header.ParentBeaconBlockRoot, []hexutil.Bytes{})
	runtime.ReadMemStats(&after)
	require.NoError(t, err)
	require.NotNil(t, status)
	require.Equal(t, engine_types.InvalidStatus, status.Status)
	require.NotNil(t, status.ValidationError)
	require.ErrorContains(t, status.ValidationError.Error(), "block access list too large")
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(1<<20))
}

func TestNewPayloadBoundsDerivedBlobHashAllocations(t *testing.T) {
	const hashCount = 128 * 1024
	hashes := make([]common.Hash, hashCount)
	for i := range hashes {
		hashes[i][0] = kzg.BlobCommitmentVersionKZG
	}
	tx := &types.BlobTx{
		DynamicFeeTransaction: types.DynamicFeeTransaction{
			// Excess transaction gas prevents asynchronous sender recovery during measurement.
			CommonTx: types.CommonTx{GasLimit: 21_001, To: &common.Address{0x01}},
		},
		BlobVersionedHashes: hashes,
	}
	var encoded bytes.Buffer
	require.NoError(t, tx.MarshalBinary(&encoded))
	payload := &engine_types.ExecutionPayload{
		LogsBloom:     make(hexutil.Bytes, types.BloomByteLength),
		Timestamp:     1,
		GasLimit:      21_000,
		Transactions:  []hexutil.Bytes{encoded.Bytes()},
		Withdrawals:   types.Withdrawals{},
		BlobGasUsed:   new(hexutil.Uint64),
		ExcessBlobGas: new(hexutil.Uint64),
	}
	srv := newTestEngineServer(prePragueChainConfig(), true)
	ctx := t.Context()
	measure := func(derive bool) uint64 {
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		status, err := srv.newPayload(ctx, payload, []common.Hash{}, &common.Hash{}, nil, clparams.DenebVersion, derive, nil)
		runtime.ReadMemStats(&after)
		require.NoError(t, err)
		require.Equal(t, engine_types.InvalidStatus, status.Status)
		require.EqualError(t, status.ValidationError.Error(), "invalid block hash")
		return after.TotalAlloc - before.TotalAlloc
	}
	measure(false)
	withoutDerivation, withDerivation := measure(false), measure(true)
	// Decoding and hashing allocate in both paths; derivation must not add another full hash list.
	require.Less(t, withDerivation, withoutDerivation+uint64(hashCount*len(common.Hash{})/2))
}

func TestNewPayloadV2WithdrawalsValidation(t *testing.T) {
	t.Parallel()

	const withdrawal = `{"index":"0x0","validatorIndex":"0x1","address":"0x1111111111111111111111111111111111111111","amount":"0x1"}`
	for _, test := range []struct {
		name        string
		withdrawals string
		wantError   string
	}{
		{"null entry", `[null]`, "null withdrawal at index 0"},
		{"null after valid entry", `[` + withdrawal + `,null]`, "null withdrawal at index 1"},
		{"missing list", `null`, "missing withdrawals list"},
		{"empty list", `[]`, ""},
		{"valid entry", `[` + withdrawal + `]`, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			var decoded engine_types.ExecutionPayload
			require.NoError(t, json.Unmarshal([]byte(`{"withdrawals":`+test.withdrawals+`}`), &decoded))
			header := makeParentHeader(1001)
			header.ParentHash = common.Hash{0x1}
			header.UncleHash = empty.UncleHash
			header.TxHash = empty.RootHash
			if decoded.Withdrawals != nil {
				root := types.DeriveSha(types.Withdrawals(decoded.Withdrawals))
				header.WithdrawalsHash = &root
			}
			block := types.NewBlockFromStorage(header.Hash(), header, nil, nil, decoded.Withdrawals, nil)
			payload, err := engine_types.ExecutionPayloadFromBlock(block)
			require.NoError(t, err)

			cfg := preCancunChainConfig()
			stub := &stubExecutionModule{}
			downloader := engine_block_downloader.NewEngineBlockDownloader(t.Context(), log.New(), stub, nil, nil, cfg, ethconfig.Sync{}, nil)
			srv := NewEngineServer(log.New(), cfg, stub, downloader, false, false, false, true, nil, nil, 0, 0)
			srv.test = true

			status, err := srv.NewPayloadV2(t.Context(), payload)
			if test.wantError != "" {
				require.Nil(t, status)
				var invalidParams *rpc.InvalidParamsError
				require.ErrorAs(t, err, &invalidParams)
				require.Equal(t, -32602, invalidParams.ErrorCode())
				require.Equal(t, test.wantError, invalidParams.Message)
				return
			}
			require.NoError(t, err)
			require.Equal(t, engine_types.SyncingStatus, status.Status)
		})
	}
}

// newValidPayloadServer returns a server whose stub module accepts the returned Capella payload as VALID.
func newValidPayloadServer(t *testing.T) (*EngineServer, *stubExecutionModule, *engine_types.ExecutionPayload) {
	parent := makeParentHeader(1000)
	parent.Number.SetUint64(99)
	header := makeParentHeader(1001)
	header.ParentHash = parent.Hash()
	header.UncleHash = empty.UncleHash
	header.TxHash = empty.RootHash
	withdrawalsHash := types.DeriveSha(types.Withdrawals{})
	header.WithdrawalsHash = &withdrawalsHash
	block := types.NewBlockFromStorage(header.Hash(), header, nil, nil, types.Withdrawals{}, nil)
	payload, err := engine_types.ExecutionPayloadFromBlock(block)
	require.NoError(t, err)

	cfg := preCancunChainConfig()
	stub := &stubExecutionModule{
		getHeaderFunc:     getHeaderReturning(parent.Hash(), parent),
		currentHeaderFunc: func(context.Context) (*types.Header, error) { return parent, nil },
	}
	downloader := engine_block_downloader.NewEngineBlockDownloader(t.Context(), log.New(), stub, nil, nil, cfg, ethconfig.Sync{}, nil)
	srv := NewEngineServer(log.New(), cfg, stub, downloader, false, false, false, true, nil, nil, 0, 64)
	return srv, stub, payload
}

// onValid reads the validated state in the engine-lock section of the validation, so no forkchoice update or other
// payload replaces or commits that state first.
func TestNewPayloadRunsOnValidUnderTheEngineLock(t *testing.T) {
	srv, _, payload := newValidPayloadServer(t)

	var validHash common.Hash
	var lockHeld bool
	status, err := srv.newPayload(t.Context(), payload, nil, nil, nil, clparams.CapellaVersion, false, func(blockHash common.Hash) {
		validHash = blockHash
		lockHeld = !srv.lock.TryLock()
		if !lockHeld {
			srv.lock.Unlock()
		}
	})
	require.NoError(t, err)
	require.Equal(t, engine_types.ValidStatus, status.Status)
	require.Equal(t, payload.BlockHash, validHash)
	require.True(t, lockHeld)
}

// The readiness wait of the witness route runs before the engine lock is taken, so a busy module does not stall a
// forkchoice update.
func TestNewPayloadWaitsForReadinessWithoutTheEngineLock(t *testing.T) {
	srv, stub, payload := newValidPayloadServer(t)
	var lockHeldInReady bool
	stub.readyFunc = func(context.Context) (bool, error) {
		if srv.lock.TryLock() {
			srv.lock.Unlock()
		} else {
			lockHeldInReady = true
		}
		return true, nil
	}

	status, err := srv.newPayload(t.Context(), payload, nil, nil, nil, clparams.CapellaVersion, false, func(common.Hash) {})
	require.NoError(t, err)
	require.Equal(t, engine_types.ValidStatus, status.Status)
	require.False(t, lockHeldInReady)
}

func TestNewPayloadRejectsMoreTransactionsThanGasLimitAllows(t *testing.T) {
	t.Parallel()

	const gasLimit = 4 * 16_800 // the per-transaction minimum after London
	for _, test := range []struct {
		name          string
		txCount       int
		txGasLimit    uint64
		cfg           *chain.Config
		frame         bool
		noWithdrawals bool
		nilBlobHashes bool
		prague        bool
		amsterdam     bool
		wantStatus    engine_types.EngineStatus
		wantErr       string
		wantErrCode   int
	}{
		{name: "4 txs", txCount: 4, cfg: preCancunChainConfig(), wantStatus: engine_types.SyncingStatus},
		{name: "5 txs", txCount: 5, cfg: preCancunChainConfig(), wantStatus: engine_types.InvalidStatus, wantErr: "gas limit reached: 5 transactions use at least 16800 gas each"},
		{name: "1 tx above the gas limit", txCount: 1, txGasLimit: gasLimit + 1, cfg: preCancunChainConfig(), wantStatus: engine_types.InvalidStatus, wantErr: "gas limit reached"},
		{name: "5 frame txs on Bogota", txCount: 5, cfg: allForksChainConfig(), frame: true, amsterdam: true, wantStatus: engine_types.SyncingStatus},
		{name: "6 frame txs on Bogota", txCount: 6, cfg: allForksChainConfig(), frame: true, amsterdam: true, wantStatus: engine_types.InvalidStatus, wantErr: "gas limit reached: 6 transactions use at least 12000 gas each"},
		{name: "4 txs on Prague", txCount: 4, cfg: preOsakaChainConfig(), prague: true, wantStatus: engine_types.InvalidStatus, wantErr: "gas limit reached: 4 transactions use at least 21000 gas each"},
		{name: "5 txs on Amsterdam", txCount: 5, cfg: allForksChainConfig(), amsterdam: true, wantStatus: engine_types.SyncingStatus},
		{name: "5 txs without withdrawals", txCount: 5, cfg: preCancunChainConfig(), noWithdrawals: true, wantErrCode: -32602},
		{name: "5 txs after Cancun", txCount: 5, cfg: prePragueChainConfig(), wantErrCode: -38005},
		{name: "5 txs with null blob hashes", txCount: 5, cfg: prePragueChainConfig(), nilBlobHashes: true, wantErrCode: -32602},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if test.frame {
				test.cfg.BogotaTime = common.NewUint64(0)
			}
			to := common.Address{0x1}
			txs := make(types.Transactions, test.txCount)
			for i := range txs {
				txs[i] = &types.LegacyTx{CommonTx: types.CommonTx{Nonce: uint64(i), To: &to, GasLimit: cmp.Or(test.txGasLimit, 21_000)}}
				if test.frame {
					txs[i] = &types.FrameTransaction{
						ChainID: *test.cfg.ChainID, NonceKeys: []uint256.Int{{}}, Nonce: uint64(i), SenderAddress: to,
						Frames: []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000, State: gasLimit}}},
					}
				}
			}
			header := makeParentHeader(1001)
			header.ParentHash = common.Hash{0x1}
			header.UncleHash = empty.UncleHash
			header.GasLimit = gasLimit
			header.TxHash = types.DeriveSha(txs)
			withdrawals := types.Withdrawals{}
			withdrawalsHash := types.DeriveSha(withdrawals)
			header.WithdrawalsHash = &withdrawalsHash
			if test.nilBlobHashes || test.prague || test.amsterdam {
				header.BlobGasUsed = new(uint64)
				header.ExcessBlobGas = new(uint64)
				header.ParentBeaconBlockRoot = &common.Hash{}
			}
			if test.prague || test.amsterdam {
				header.RequestsHash = (types.FlatRequests{}).Hash()
			}
			if test.amsterdam {
				balHash := empty.BlockAccessListHash
				header.BlockAccessListHash = &balHash
				header.SlotNumber = new(uint64)
			}
			block := types.NewBlockFromStorage(header.Hash(), header, txs, nil, withdrawals, nil)
			payload, err := engine_types.ExecutionPayloadFromBlock(block)
			require.NoError(t, err)
			if test.noWithdrawals {
				payload.Withdrawals = nil
			}

			stub := &stubExecutionModule{}
			downloader := engine_block_downloader.NewEngineBlockDownloader(t.Context(), log.New(), stub, nil, nil, test.cfg, ethconfig.Sync{}, nil)
			srv := NewEngineServer(log.New(), test.cfg, stub, downloader, false, false, false, true, nil, nil, 0, 0)
			srv.test = true

			var status *engine_types.PayloadStatus
			switch {
			case test.amsterdam:
				payload.BlockAccessList = &hexutil.Bytes{0xc0}
				status, err = srv.NewPayloadV5(t.Context(), payload, []common.Hash{}, header.ParentBeaconBlockRoot, []hexutil.Bytes{})
			case test.prague:
				status, err = srv.NewPayloadV4(t.Context(), payload, []common.Hash{}, header.ParentBeaconBlockRoot, []hexutil.Bytes{})
			case test.nilBlobHashes:
				status, err = srv.NewPayloadV3(t.Context(), payload, nil, header.ParentBeaconBlockRoot)
			default:
				status, err = srv.NewPayloadV2(t.Context(), payload)
			}
			if test.wantErrCode != 0 {
				var rpcErr rpc.Error
				require.ErrorAs(t, err, &rpcErr)
				require.Equal(t, test.wantErrCode, rpcErr.ErrorCode())
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.wantStatus, status.Status)
			if test.wantErr != "" {
				require.EqualError(t, status.ValidationError.Error(), test.wantErr)
			}
		})
	}
}

func TestNewPayloadOverBlobLimitOnBadParentWithUnknownAncestor(t *testing.T) {
	t.Parallel()

	cfg := prePragueChainConfig()
	header := makeParentHeader(1001)
	header.ParentHash = common.Hash{0x1}
	blobHashes := make([]common.Hash, cfg.GetMaxBlobsPerBlock(header.Time)+1)
	for i := range blobHashes {
		blobHashes[i] = common.Hash{kzg.BlobCommitmentVersionKZG, byte(i)}
	}
	txs := types.Transactions{&types.BlobTx{
		DynamicFeeTransaction: types.DynamicFeeTransaction{
			CommonTx: types.CommonTx{GasLimit: 21_000, To: &common.Address{0x1}},
			ChainID:  *uint256.NewInt(1),
			TipCap:   *uint256.NewInt(1),
			FeeCap:   *uint256.NewInt(1),
		},
		MaxFeePerBlobGas:    *uint256.NewInt(1),
		BlobVersionedHashes: blobHashes,
	}}
	header.UncleHash = empty.UncleHash
	header.TxHash = types.DeriveSha(txs)
	withdrawals := types.Withdrawals{}
	withdrawalsHash := types.DeriveSha(withdrawals)
	header.WithdrawalsHash = &withdrawalsHash
	header.BlobGasUsed = new(uint64)
	header.ExcessBlobGas = new(uint64)
	header.ParentBeaconBlockRoot = &common.Hash{}
	block := types.NewBlockFromStorage(header.Hash(), header, txs, nil, withdrawals, nil)
	payload, err := engine_types.ExecutionPayloadFromBlock(block)
	require.NoError(t, err)

	stub := &stubExecutionModule{}
	downloader := engine_block_downloader.NewEngineBlockDownloader(t.Context(), log.New(), stub, nil, nil, cfg, ethconfig.Sync{}, nil)
	downloader.ReportBadHeader(header.ParentHash, common.Hash{}, "invalid block")
	srv := NewEngineServer(log.New(), cfg, stub, downloader, false, false, false, true, nil, nil, 0, 0)
	srv.test = true

	status, err := srv.NewPayloadV3(t.Context(), payload, blobHashes, header.ParentBeaconBlockRoot)
	require.NoError(t, err)
	require.Equal(t, engine_types.InvalidStatus, status.Status)
	require.Nil(t, status.LatestValidHash)
}

func TestForkchoiceUpdatedV2RejectsNullWithdrawalWhenSyncing(t *testing.T) {
	t.Parallel()

	var attributes engine_types.PayloadAttributes
	require.NoError(t, json.Unmarshal([]byte(`{"timestamp":"0x3e9","withdrawals":[null]}`), &attributes))
	cfg := preCancunChainConfig()
	stub := &stubExecutionModule{}
	downloader := engine_block_downloader.NewEngineBlockDownloader(t.Context(), log.New(), stub, nil, nil, cfg, ethconfig.Sync{}, nil)
	srv := NewEngineServer(log.New(), cfg, stub, downloader, false, false, false, true, nil, nil, 0, 0)
	srv.test = true

	response, err := srv.ForkchoiceUpdatedV2(t.Context(), &engine_types.ForkChoiceState{HeadHash: common.Hash{0x1}}, &attributes)
	require.Nil(t, response)
	var rpcErr rpc.Error
	require.ErrorAs(t, err, &rpcErr)
	require.Equal(t, -38003, rpcErr.ErrorCode())
}

// Do 1 step to start txPool
func oneBlockSteps(m *execmoduletester.ExecModuleTester, require *require.Assertions, blocks int) {
	chain, err := m.GenerateChain(blocks, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{1})
	})
	require.NoError(err)
	err = m.InsertChain(chain)
	require.NoError(err)
}

// Do 1 step to start txPool
func oneBlockStep(mockSentry *execmoduletester.ExecModuleTester, require *require.Assertions) {
	oneBlockSteps(mockSentry, require, 1)
}

func newBaseApiForTest(m *execmoduletester.ExecModuleTester) *jsonrpc.BaseAPI {
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	return jsonrpc.NewBaseApi(nil, stateCache, m.BlockReader, m.Engine, &rpccfg.BaseApiConfig{Dirs: m.Dirs})
}

func newEthApiForTest(base *jsonrpc.BaseAPI, db kv.TemporalRoDB, txPool txpoolproto.TxpoolClient) *jsonrpc.APIImpl {
	cfg := &rpccfg.EthApiConfig{
		GasCap:                      5000000,
		FeeCap:                      ethconfig.Defaults.RPCTxFeeCap,
		ReturnDataLimit:             100_000,
		AllowUnprotectedTxs:         false,
		MaxGetProofRewindBlockCount: 100_000,
		SubscribeLogsChannelSize:    128,
		RpcTxSyncDefaultTimeout:     20 * time.Second,
		RpcTxSyncMaxTimeout:         1 * time.Minute,
	}
	return jsonrpc.NewEthAPI(base, db, nil, txPool, nil, cfg, log.New())
}

func TestGetBlobsV1(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	buf := bytes.NewBuffer(nil)
	funds := big.NewInt(1 * common.Ether)
	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	address := crypto.PubkeyToAddress(key.PublicKey)

	chainConfig := chain.AllProtocolChanges.Copy()
	chainConfig.PragueTime = nil
	chainConfig.OsakaTime = nil
	chainConfig.AmsterdamTime = nil
	gspec := &types.Genesis{
		Config: chainConfig,
		Alloc: types.GenesisAlloc{
			address: {Balance: funds},
		},
	}
	mockSentry := execmoduletester.New(
		t,
		execmoduletester.WithGenesisSpec(gspec),
		execmoduletester.WithKey(key),
		execmoduletester.WithTxPool(),
	)
	require := require.New(t)
	oneBlockStep(mockSentry, require)

	wrappedTxn := types.MakeWrappedBlobTxn(mockSentry.ChainConfig.ChainID)
	txn, err := types.SignTx(wrappedTxn, *types.LatestSignerForChainID(mockSentry.ChainConfig.ChainID), mockSentry.Key)
	require.NoError(err)
	dt := &wrappedTxn.Tx.DynamicFeeTransaction
	v, r, s := txn.RawSignatureValues()
	dt.V.Set(v)
	dt.R.Set(r)
	dt.S.Set(s)

	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, mockSentry)
	txPool := direct.NewTxPoolClient(mockSentry.TxPoolGrpcServer)

	ff := rpchelper.New(ctx, rpchelper.DefaultFiltersConfig, nil, txPool, txpoolproto.NewMiningClient(conn), func() {}, mockSentry.Log, nil)
	api := newEthApiForTest(newBaseApiForTest(mockSentry), mockSentry.DB, txPool)

	executionRpc := mockSentry.ExecModule
	eth := rpcservices.NewRemoteBackend(nil, mockSentry.DB, mockSentry.BlockReader)
	fcuTimeout := ethconfig.Defaults.FcuTimeout
	maxReorgDepth := ethconfig.Defaults.MaxReorgDepth
	engineServer := NewEngineServer(mockSentry.Log, mockSentry.ChainConfig, executionRpc, nil, false, false, false, true, txPool, mockSentry.TxPool, fcuTimeout, maxReorgDepth)
	ctx, cancel := context.WithCancel(ctx)
	var eg errgroup.Group
	t.Cleanup(func() {
		err := eg.Wait() // wait for clean exit
		require.ErrorIs(err, context.Canceled)
	})
	t.Cleanup(cancel)
	eg.Go(func() error {
		return engineServer.Start(ctx, &httpcfg.HttpCfg{}, mockSentry.DB, mockSentry.BlockReader, ff, nil, mockSentry.Engine, eth, nil, nil)
	})

	err = wrappedTxn.MarshalBinaryWrapped(buf)
	require.NoError(err)
	_, err = api.SendRawTransaction(ctx, buf.Bytes())
	require.NoError(err)

	blobHashes := append([]common.Hash{{}}, wrappedTxn.Tx.BlobVersionedHashes...)
	blobsResp, err := engineServer.GetBlobsV1(ctx, blobHashes)
	require.NoError(err)
	require.True(blobsResp[0] == nil)
	require.Equal(blobsResp[1].Blob, hexutil.Bytes(wrappedTxn.Blobs[0][:]))
	require.Equal(blobsResp[2].Blob, hexutil.Bytes(wrappedTxn.Blobs[1][:]))
	require.Equal(blobsResp[1].Proof, hexutil.Bytes(wrappedTxn.Proofs[0][:]))
	require.Equal(blobsResp[2].Proof, hexutil.Bytes(wrappedTxn.Proofs[1][:]))
}

func TestGetBlobsV2(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	buf := bytes.NewBuffer(nil)
	mockSentry := execmoduletester.New(t, execmoduletester.WithTxPool(), execmoduletester.WithChainConfig(chain.AllProtocolChanges))
	require := require.New(t)
	oneBlockStep(mockSentry, require)

	wrappedTxn := types.MakeV1WrappedBlobTxn(mockSentry.ChainConfig.ChainID)
	txn, err := types.SignTx(wrappedTxn, *types.LatestSignerForChainID(mockSentry.ChainConfig.ChainID), mockSentry.Key)
	require.NoError(err)
	dt := &wrappedTxn.Tx.DynamicFeeTransaction
	v, r, s := txn.RawSignatureValues()
	dt.V.Set(v)
	dt.R.Set(r)
	dt.S.Set(s)

	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, mockSentry)
	txPool := direct.NewTxPoolClient(mockSentry.TxPoolGrpcServer)

	ff := rpchelper.New(ctx, rpchelper.DefaultFiltersConfig, nil, txPool, txpoolproto.NewMiningClient(conn), func() {}, mockSentry.Log, nil)
	api := newEthApiForTest(newBaseApiForTest(mockSentry), mockSentry.DB, txPool)

	executionRpc := mockSentry.ExecModule
	eth := rpcservices.NewRemoteBackend(nil, mockSentry.DB, mockSentry.BlockReader)
	fcuTimeout := ethconfig.Defaults.FcuTimeout
	maxReorgDepth := ethconfig.Defaults.MaxReorgDepth
	engineServer := NewEngineServer(mockSentry.Log, mockSentry.ChainConfig, executionRpc, nil, false, false, false, true, txPool, mockSentry.TxPool, fcuTimeout, maxReorgDepth)
	ctx, cancel := context.WithCancel(ctx)
	var eg errgroup.Group
	t.Cleanup(func() {
		err := eg.Wait() // wait for clean exit
		require.ErrorIs(err, context.Canceled)
	})
	t.Cleanup(cancel)
	eg.Go(func() error {
		return engineServer.Start(ctx, &httpcfg.HttpCfg{}, mockSentry.DB, mockSentry.BlockReader, ff, nil, mockSentry.Engine, eth, nil, nil)
	})

	err = wrappedTxn.MarshalBinaryWrapped(buf)
	require.NoError(err)
	hh, err := api.SendRawTransaction(ctx, buf.Bytes())
	require.NoError(err)
	require.NotEmpty(hh)

	blobHashes := append([]common.Hash{{}}, wrappedTxn.Tx.BlobVersionedHashes...)
	blobsResp, err := engineServer.GetBlobsV2(ctx, blobHashes)
	require.NoError(err)
	require.Nil(blobsResp) // Any one blob not found makes the whole response nil

	blobHashes = blobHashes[1:]
	blobsResp, err = engineServer.GetBlobsV2(ctx, blobHashes)
	require.NoError(err)
	require.Len(blobsResp, 2)
	require.Equal(blobsResp[0].Blob, hexutil.Bytes(wrappedTxn.Blobs[0][:]))
	require.Equal(blobsResp[1].Blob, hexutil.Bytes(wrappedTxn.Blobs[1][:]))

	for i := range 128 {
		require.Equal(blobsResp[0].CellProofs[i], hexutil.Bytes(wrappedTxn.Proofs[i][:]))
		require.Equal(blobsResp[1].CellProofs[i], hexutil.Bytes(wrappedTxn.Proofs[i+128][:]))
	}
}

func newGetBlobsTxPoolFixture(t *testing.T) (context.Context, *EngineServer, *types.BlobTxWrapper) {
	t.Helper()
	buf := bytes.NewBuffer(nil)
	mockSentry := execmoduletester.New(t, execmoduletester.WithTxPool(), execmoduletester.WithChainConfig(chain.AllProtocolChanges))
	require := require.New(t)
	oneBlockStep(mockSentry, require)

	wrappedTxn := types.MakeV1WrappedBlobTxn(mockSentry.ChainConfig.ChainID)
	txn, err := types.SignTx(wrappedTxn, *types.LatestSignerForChainID(mockSentry.ChainConfig.ChainID), mockSentry.Key)
	require.NoError(err)
	dt := &wrappedTxn.Tx.DynamicFeeTransaction
	v, r, s := txn.RawSignatureValues()
	dt.V.Set(v)
	dt.R.Set(r)
	dt.S.Set(s)

	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, mockSentry)
	txPool := direct.NewTxPoolClient(mockSentry.TxPoolGrpcServer)

	ff := rpchelper.New(ctx, rpchelper.DefaultFiltersConfig, nil, txPool, txpoolproto.NewMiningClient(conn), func() {}, mockSentry.Log, nil)
	api := newEthApiForTest(newBaseApiForTest(mockSentry), mockSentry.DB, txPool)

	executionRpc := mockSentry.ExecModule
	eth := rpcservices.NewRemoteBackend(nil, mockSentry.DB, mockSentry.BlockReader)
	fcuTimeout := ethconfig.Defaults.FcuTimeout
	maxReorgDepth := ethconfig.Defaults.MaxReorgDepth
	engineServer := NewEngineServer(mockSentry.Log, mockSentry.ChainConfig, executionRpc, nil, false, false, false, true, txPool, mockSentry.TxPool, fcuTimeout, maxReorgDepth)
	ctx, cancel := context.WithCancel(ctx)
	var eg errgroup.Group
	t.Cleanup(func() {
		err := eg.Wait() // wait for clean exit
		require.ErrorIs(err, context.Canceled)
	})
	t.Cleanup(cancel)
	eg.Go(func() error {
		return engineServer.Start(ctx, &httpcfg.HttpCfg{}, mockSentry.DB, mockSentry.BlockReader, ff, nil, mockSentry.Engine, eth, nil, nil)
	})

	err = wrappedTxn.MarshalBinaryWrapped(buf)
	require.NoError(err)
	hh, err := api.SendRawTransaction(ctx, buf.Bytes())
	require.NoError(err)
	require.NotEmpty(hh)

	return ctx, engineServer, wrappedTxn
}

func TestGetBlobsV3(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	ctx, engineServer, wrappedTxn := newGetBlobsTxPoolFixture(t)
	require := require.New(t)

	blobHashes := append([]common.Hash{{}}, wrappedTxn.Tx.BlobVersionedHashes...)
	blobsResp, err := engineServer.GetBlobsV3(ctx, blobHashes)
	require.NoError(err)
	require.Len(blobsResp, 3) // Unlike GetBlobsV2, only the missing blob should be nil
	require.Nil(blobsResp[0])
	require.Equal(blobsResp[1].Blob, hexutil.Bytes(wrappedTxn.Blobs[0][:]))
	require.Equal(blobsResp[2].Blob, hexutil.Bytes(wrappedTxn.Blobs[1][:]))

	blobHashes = blobHashes[1:]
	blobsResp, err = engineServer.GetBlobsV3(ctx, blobHashes)
	require.NoError(err)
	require.Len(blobsResp, 2)
	require.Equal(blobsResp[0].Blob, hexutil.Bytes(wrappedTxn.Blobs[0][:]))
	require.Equal(blobsResp[1].Blob, hexutil.Bytes(wrappedTxn.Blobs[1][:]))

	for i := range 128 {
		require.Equal(blobsResp[0].CellProofs[i], hexutil.Bytes(wrappedTxn.Proofs[i][:]))
		require.Equal(blobsResp[1].CellProofs[i], hexutil.Bytes(wrappedTxn.Proofs[i+128][:]))
	}
}

func TestGetBlobsV4WithTxPool(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	ctx, engineServer, wrappedTxn := newGetBlobsTxPoolFixture(t)
	require := require.New(t)

	blobHashes := append([]common.Hash{{}}, wrappedTxn.Tx.BlobVersionedHashes...)
	indices := []uint64{0, 64, 127}
	mask := hexutil.MustDecodeHex("0x01000000000000000100000000000080")
	cellBundles, err := engineServer.GetBlobsV4(ctx, blobHashes, mask)
	require.NoError(err)
	require.Len(cellBundles, 3)
	require.Nil(cellBundles[0])
	for i, bundle := range cellBundles[1:] {
		require.NotNil(bundle)
		require.Len(bundle.BlobCells, len(indices))
		require.Len(bundle.Proofs, len(indices))
		cells := make([]*goethkzg.Cell, len(indices))
		proofs := make([]goethkzg.KZGProof, len(indices))
		commitments := make([]goethkzg.KZGCommitment, len(indices))
		for j := range indices {
			require.NotNil(bundle.BlobCells[j])
			require.NotNil(bundle.Proofs[j])
			require.Len(*bundle.BlobCells[j], len(goethkzg.Cell{}))
			require.Len(*bundle.Proofs[j], len(goethkzg.KZGProof{}))
			cells[j] = (*goethkzg.Cell)(*bundle.BlobCells[j])
			proofs[j] = goethkzg.KZGProof(*bundle.Proofs[j])
			commitments[j] = goethkzg.KZGCommitment(wrappedTxn.Commitments[i])
		}
		require.NoError(kzg.Ctx().VerifyCellKZGProofBatch(commitments, indices, cells, proofs))
	}
}

type blobGetterFunc func([]common.Hash) []txpool.PoolBlobBundle

func (f blobGetterFunc) GetBlobs(hashes []common.Hash) []txpool.PoolBlobBundle {
	return f(hashes)
}

type blobGetterMap map[common.Hash]txpool.PoolBlobBundle

func (m blobGetterMap) GetBlobs(hashes []common.Hash) []txpool.PoolBlobBundle {
	bundles := make([]txpool.PoolBlobBundle, len(hashes))
	for i, hash := range hashes {
		bundles[i] = m[hash]
	}
	return bundles
}

func getBlobsV4Fixture(t *testing.T, value byte) (common.Hash, txpool.PoolBlobBundle, []*goethkzg.Cell) {
	t.Helper()
	var blob goethkzg.Blob
	blob[31] = value
	cells, proofs, err := kzg.Ctx().ComputeCellsAndKZGProofs(&blob, 2)
	require.NoError(t, err)
	commitment, err := kzg.Ctx().BlobToKZGCommitment(&blob, 2)
	require.NoError(t, err)
	return common.Hash(kzg.KZGToVersionedHash(commitment)), txpool.PoolBlobBundle{
		Commitment: commitment,
		Blob:       blob[:],
		Proofs:     proofs[:],
	}, cells[:]
}

func newGetBlobsV4Client(t *testing.T, getter txpool.BlobGetter) *rpc.Client {
	t.Helper()
	return newEngineInProcClient(t, &EngineServer{logger: log.New(), blobGetter: getter})
}

func TestGetBlobsV4(t *testing.T) {
	hash, bundle, cells := getBlobsV4Fixture(t, 1)
	client := newGetBlobsV4Client(t, blobGetterMap{hash: bundle})
	allIndices := make([]int, goethkzg.CellsPerExtBlob)
	for i := range allIndices {
		allIndices[i] = i
	}
	for _, tc := range []struct {
		name    string
		mask    hexutil.Bytes
		indices []int
	}{
		{"selected_cells", hexutil.MustDecodeHex("0x81010000000000800100000000000080"), []int{0, 7, 8, 63, 64, 127}},
		{"all_cells", bytes.Repeat([]byte{0xff}, 16), allIndices},
		{"no_cells", make(hexutil.Bytes, 16), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var result []struct {
				BlobCells []hexutil.Bytes `json:"blob_cells"`
				Proofs    []hexutil.Bytes `json:"proofs"`
			}
			require.NoError(t, client.CallContext(t.Context(), &result, "engine_getBlobsV4", []common.Hash{hash}, tc.mask))
			require.Len(t, result, 1)
			require.NotNil(t, result[0].BlobCells)
			require.NotNil(t, result[0].Proofs)
			require.Len(t, result[0].BlobCells, len(tc.indices))
			require.Len(t, result[0].Proofs, len(tc.indices))
			for i, index := range tc.indices {
				require.Equal(t, hexutil.Bytes(cells[index][:]), result[0].BlobCells[i])
				require.Equal(t, hexutil.Bytes(bundle.Proofs[index][:]), result[0].Proofs[i])
			}
		})
	}
}

func TestGetBlobsV4UsesPoolCells(t *testing.T) {
	hash, bundle, _ := getBlobsV4Fixture(t, 1)
	_, _, poolCells := getBlobsV4Fixture(t, 2)
	bundle.Cells = (*[goethkzg.CellsPerExtBlob]*goethkzg.Cell)(poolCells)
	client := newGetBlobsV4Client(t, blobGetterMap{hash: bundle})
	var result []struct {
		BlobCells []hexutil.Bytes `json:"blob_cells"`
	}
	require.NoError(t, client.CallContext(t.Context(), &result, "engine_getBlobsV4", []common.Hash{hash}, hexutil.Bytes(hexutil.MustDecodeHex("0x01000000000000000000000000000000"))))
	require.Len(t, result, 1)
	require.Len(t, result[0].BlobCells, 1)
	require.Equal(t, hexutil.Bytes(poolCells[0][:]), result[0].BlobCells[0], "served cell 0 must be the pool's, not recomputed from the blob")
}

func TestGetBlobsV4FastJSON(t *testing.T) {
	hash, bundle, _ := getBlobsV4Fixture(t, 1)
	server := &EngineServer{logger: log.New(), blobGetter: blobGetterMap{hash: bundle}}
	result, err := server.GetBlobsV4(t.Context(), []common.Hash{hash, {}}, hexutil.MustDecodeHex("0x01000000000000000100000000000080"))
	require.NoError(t, err)
	marshaler, ok := any(result).(interface {
		MarshalFastJSONTo(*jsonstream.Stream) error
	})
	require.True(t, ok, "GetBlobsV4 must return a fast JSON result")
	want, err := json.Marshal(result)
	require.NoError(t, err)
	got, err := jsonstream.Marshal(marshaler)
	require.NoError(t, err)
	require.Equal(t, string(want), string(got))
}

func TestGetBlobsV4PartialResponse(t *testing.T) {
	hashA, bundleA, cellsA := getBlobsV4Fixture(t, 1)
	hashB, bundleB, cellsB := getBlobsV4Fixture(t, 2)
	pool := blobGetterMap{hashA: bundleA, hashB: bundleB}
	cells := map[common.Hash][]*goethkzg.Cell{hashA: cellsA, hashB: cellsB}
	client := newGetBlobsV4Client(t, pool)
	hashes := []common.Hash{hashB, {}, hashA, hashB}
	mask := hexutil.Bytes(hexutil.MustDecodeHex("0x80010000000000000000000000000080"))
	var result []*engine_types.BlobCellsAndProofsV1
	require.NoError(t, client.CallContext(t.Context(), &result, "engine_getBlobsV4", hashes, mask))
	require.Len(t, result, len(hashes))
	for i, hash := range hashes {
		bundle, available := pool[hash]
		if !available {
			require.Nil(t, result[i])
			continue
		}
		require.NotNil(t, result[i])
		require.Len(t, result[i].BlobCells, 3)
		require.Len(t, result[i].Proofs, 3)
		for j, index := range []int{7, 8, 127} {
			require.NotNil(t, result[i].BlobCells[j])
			require.NotNil(t, result[i].Proofs[j])
			require.Equal(t, hexutil.Bytes(cells[hash][index][:]), *result[i].BlobCells[j])
			require.Equal(t, hexutil.Bytes(bundle.Proofs[index][:]), *result[i].Proofs[j])
		}
	}
}

func TestGetBlobsV4InvalidMask(t *testing.T) {
	client := newGetBlobsV4Client(t, blobGetterFunc(func(hashes []common.Hash) []txpool.PoolBlobBundle {
		return make([]txpool.PoolBlobBundle, len(hashes))
	}))
	for _, tc := range []struct {
		name string
		mask any
	}{
		{"long", "0x" + strings.Repeat("00", 17)},
		{"short", "0x" + strings.Repeat("00", 15)},
		{"empty", "0x"},
		{"null", nil},
		{"missing_prefix", strings.Repeat("00", 16)},
		{"invalid_hex", "0x" + strings.Repeat("gg", 16)},
		{"wrong_type", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var result any
			err := client.CallContext(t.Context(), &result, "engine_getBlobsV4", []common.Hash{}, tc.mask)
			var rpcErr rpc.Error
			require.ErrorAs(t, err, &rpcErr)
			require.Equal(t, -32602, rpcErr.ErrorCode())
		})
	}
}

func TestGetBlobsV4RequestLimit(t *testing.T) {
	client := newGetBlobsV4Client(t, blobGetterFunc(func(hashes []common.Hash) []txpool.PoolBlobBundle {
		return make([]txpool.PoolBlobBundle, len(hashes))
	}))
	for _, count := range []int{0, 128, 129} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			var result []*engine_types.BlobCellsAndProofsV1
			err := client.CallContext(t.Context(), &result, "engine_getBlobsV4", make([]common.Hash, count), make(hexutil.Bytes, 16))
			if count > 128 {
				var rpcErr rpc.Error
				require.ErrorAs(t, err, &rpcErr)
				require.Equal(t, -38004, rpcErr.ErrorCode())
				return
			}
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, make([]*engine_types.BlobCellsAndProofsV1, count), result)
		})
	}
}

func TestGetBlobsV4PoolDisabled(t *testing.T) {
	client := newGetBlobsV4Client(t, nil)
	var result json.RawMessage
	err := client.CallContext(t.Context(), &result, "engine_getBlobsV4", []common.Hash{{1}}, make(hexutil.Bytes, 16))
	require.EqualError(t, err, txpool.ErrPoolDisabled.Error())
}

func TestGetBlobsV4Unavailable(t *testing.T) {
	for _, tc := range []struct {
		name   string
		getter txpool.BlobGetter
	}{
		{"unavailable", blobGetterFunc(func([]common.Hash) []txpool.PoolBlobBundle { return nil })},
		{"invalid_response_length", blobGetterFunc(func([]common.Hash) []txpool.PoolBlobBundle {
			return make([]txpool.PoolBlobBundle, 2)
		})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := newGetBlobsV4Client(t, tc.getter)
			var result json.RawMessage
			err := client.CallContext(t.Context(), &result, "engine_getBlobsV4", []common.Hash{{1}}, make(hexutil.Bytes, 16))
			require.NoError(t, err)
			require.JSONEq(t, "null", string(result))
		})
	}
}

func TestGetBlobsV4JSONRPCClient(t *testing.T) {
	client := &JsonRpcClient{rpcClient: newGetBlobsV4Client(t, blobGetterFunc(func(hashes []common.Hash) []txpool.PoolBlobBundle {
		return make([]txpool.PoolBlobBundle, len(hashes))
	}))}
	result, err := client.GetBlobsV4(t.Context(), []common.Hash{{1}, {2}}, make(hexutil.Bytes, 16))
	require.NoError(t, err)
	require.Equal(t, []*engine_types.BlobCellsAndProofsV1{nil, nil}, result)
}

func TestGetBlobsV4InvalidBlobLength(t *testing.T) {
	for _, size := range []int{1, len(goethkzg.Blob{}) - 1, len(goethkzg.Blob{}) + 1} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			hash := common.Hash{1}
			client := newGetBlobsV4Client(t, blobGetterMap{hash: {
				Blob:   make([]byte, size),
				Proofs: make([]goethkzg.KZGProof, goethkzg.CellsPerExtBlob),
			}})
			var result []*engine_types.BlobCellsAndProofsV1
			err := client.CallContext(t.Context(), &result, "engine_getBlobsV4", []common.Hash{hash}, make(hexutil.Bytes, 16))
			require.NoError(t, err)
			require.Equal(t, []*engine_types.BlobCellsAndProofsV1{nil}, result)
		})
	}
}

func TestGetBlobsV4Canceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	server := &EngineServer{logger: log.New(), blobGetter: blobGetterMap{}}
	result, err := server.GetBlobsV4(ctx, []common.Hash{{1}}, make(hexutil.Bytes, 16))
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result)
}

func canonicalHashAt(t *testing.T, db kv.TemporalRoDB, blockNum uint64) common.Hash {
	t.Helper()
	var hash common.Hash
	err := db.View(context.Background(), func(tx kv.Tx) error {
		var err error
		hash, err = rawdb.ReadCanonicalHash(tx, blockNum)
		return err
	})
	require.NoError(t, err)
	return hash
}

func writeBlockAccessListBytes(t *testing.T, db kv.TemporalRwDB, blockHash common.Hash, blockNum uint64, balBytes []byte) {
	t.Helper()
	err := db.Update(context.Background(), func(tx kv.RwTx) error {
		return rawdb.WriteBlockAccessListBytes(tx, blockHash, blockNum, balBytes)
	})
	require.NoError(t, err)
}

func TestGetPayloadBodiesByHashV2(t *testing.T) {
	mockSentry := execmoduletester.New(t, execmoduletester.WithTxPool(), execmoduletester.WithChainConfig(chain.AllProtocolChanges))
	req := require.New(t)
	oneBlockStep(mockSentry, req)

	executionRpc := mockSentry.ExecModule
	maxReorgDepth := ethconfig.Defaults.MaxReorgDepth
	downloader := engine_block_downloader.NewEngineBlockDownloader(t.Context(), mockSentry.Log, executionRpc, nil, nil, mockSentry.ChainConfig, ethconfig.Sync{}, nil)
	engineServer := NewEngineServer(mockSentry.Log, mockSentry.ChainConfig, executionRpc, downloader, false, false, false, true, nil, nil, ethconfig.Defaults.FcuTimeout, maxReorgDepth)

	const blockNum = 1
	blockHash := canonicalHashAt(t, mockSentry.DB, blockNum)
	req.NotEqual(common.Hash{}, blockHash)

	ctx := context.Background()

	// Amsterdam-enabled chains always have a BAL written by GenerateChain
	bodies, err := engineServer.GetPayloadBodiesByHashV2(ctx, []common.Hash{blockHash})
	req.NoError(err)
	req.Len(bodies, 1)
	req.NotNil(bodies[0])
	req.NotNil(bodies[0].BlockAccessList)
	req.NotEmpty(*bodies[0].BlockAccessList)

	// Overwrite with a non-empty BAL and verify it's returned
	balBytes := []byte{0x01, 0x02, 0x03}
	writeBlockAccessListBytes(t, mockSentry.DB, blockHash, blockNum, balBytes)

	bodies, err = engineServer.GetPayloadBodiesByHashV2(ctx, []common.Hash{blockHash})
	req.NoError(err)
	req.Len(bodies, 1)
	req.NotNil(bodies[0])
	req.NotNil(bodies[0].BlockAccessList)
	req.Equal(hexutil.Bytes(balBytes), *bodies[0].BlockAccessList)
}

func TestGetPayloadBodiesByRangeV2(t *testing.T) {
	mockSentry := execmoduletester.New(t, execmoduletester.WithTxPool(), execmoduletester.WithChainConfig(chain.AllProtocolChanges))
	req := require.New(t)
	oneBlockSteps(mockSentry, req, 2)

	executionRpc := mockSentry.ExecModule
	maxReorgDepth := ethconfig.Defaults.MaxReorgDepth
	engineServer := NewEngineServer(mockSentry.Log, mockSentry.ChainConfig, executionRpc, nil, false, false, false, true, nil, nil, ethconfig.Defaults.FcuTimeout, maxReorgDepth)

	const (
		start = 1
		count = 2
	)
	blockHash1 := canonicalHashAt(t, mockSentry.DB, start)
	blockHash2 := canonicalHashAt(t, mockSentry.DB, start+1)
	req.NotEqual(common.Hash{}, blockHash1)
	req.NotEqual(common.Hash{}, blockHash2)

	ctx := context.Background()

	// Amsterdam-enabled chains always have a BAL written by GenerateChain
	bodies, err := engineServer.GetPayloadBodiesByRangeV2(ctx, start, count)
	req.NoError(err)
	req.Len(bodies, 2)
	req.NotNil(bodies[0])
	req.NotNil(bodies[1])
	req.NotNil(bodies[0].BlockAccessList)
	req.NotNil(bodies[1].BlockAccessList)
	req.NotEmpty(*bodies[0].BlockAccessList)
	req.NotEmpty(*bodies[1].BlockAccessList)

	// Overwrite with non-empty BALs and verify they're returned
	balBytes1 := []byte{0x01, 0x02, 0x03}
	balBytes2 := []byte{0x04, 0x05, 0x06}
	writeBlockAccessListBytes(t, mockSentry.DB, blockHash1, start, balBytes1)
	writeBlockAccessListBytes(t, mockSentry.DB, blockHash2, start+1, balBytes2)

	bodies, err = engineServer.GetPayloadBodiesByRangeV2(ctx, start, count)
	req.NoError(err)
	req.Len(bodies, 2)
	req.NotNil(bodies[0])
	req.NotNil(bodies[1])
	req.NotNil(bodies[0].BlockAccessList)
	req.NotNil(bodies[1].BlockAccessList)
	req.Equal(hexutil.Bytes(balBytes1), *bodies[0].BlockAccessList)
	req.Equal(hexutil.Bytes(balBytes2), *bodies[1].BlockAccessList)
}

// TestQuickStatusHeaderLookup pins how the known-block check looks up its block. A new payload
// passes its number: a hash-only lookup of a block that is not stored yet searches every header
// snapshot file before it misses. A forkchoice update does not know its head's number (it passes
// 0), so it must look the head up by hash only.
func TestQuickStatusHeaderLookup(t *testing.T) {
	t.Parallel()

	parent := makeParentHeader(1000)
	parentHash := parent.Hash()
	blockHash := common.Hash{0x42}
	payloadNumber := parent.Number.Uint64() + 1

	for _, test := range []struct {
		name       string
		newPayload bool
		number     uint64
		forkchoice *engine_types.ForkChoiceState
		wantNumber *uint64
	}{
		{name: "newPayload", newPayload: true, number: payloadNumber, wantNumber: &payloadNumber},
		{name: "forkchoiceUpdated", forkchoice: &engine_types.ForkChoiceState{HeadHash: blockHash}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			var lookups int
			stub := &stubExecutionModule{
				getHeaderFunc: func(_ context.Context, hash *common.Hash, number *uint64) (*types.Header, error) {
					if hash != nil && *hash == blockHash {
						lookups++
						require.Equal(t, test.wantNumber, number)
						return nil, nil
					}
					return getHeaderReturning(parentHash, parent)(context.Background(), hash, number)
				},
			}

			cfg := preCancunChainConfig()
			ctx := context.Background()
			downloader := engine_block_downloader.NewEngineBlockDownloader(ctx, log.New(), stub, nil, nil, cfg, ethconfig.Sync{}, nil)
			srv := NewEngineServer(log.New(), cfg, stub, downloader, false, false, true, true, nil, nil, 0, 0)

			_, err := srv.getQuickPayloadStatusIfPossible(ctx, blockHash, test.number, parentHash, test.forkchoice, test.newPayload, defaultReadinessWait)
			require.NoError(t, err)
			require.Equal(t, 1, lookups)
		})
	}
}

func TestValidatedPayloadStatusAnswersSyncingWhenTooFarAway(t *testing.T) {
	var e EngineServer
	resp := e.validatedPayloadStatus(types.NewBlockWithHeader(&types.Header{}, nil), execmodule.ExecutionStatusTooFarAway, nil, common.Hash{})
	require.Equal(t, engine_types.SyncingStatus, resp.Status)
	require.Nil(t, resp.LatestValidHash)
	require.Nil(t, resp.ValidationError)
}

func newEngineServerWithReorgDepth(t *testing.T, m *execmoduletester.ExecModuleTester, module execmodule.ExecutionModule, maxReorgDepth uint64) *EngineServer {
	downloader := engine_block_downloader.NewEngineBlockDownloader(t.Context(), m.Log, m.ExecModule, nil, nil, m.ChainConfig, ethconfig.Sync{}, nil)
	srv := NewEngineServer(m.Log, m.ChainConfig, module, downloader, false, false, false, true, nil, nil, 0, maxReorgDepth)
	srv.test = true
	return srv
}

func newPayloadV2(t *testing.T, srv *EngineServer, block *types.Block) *engine_types.PayloadStatus {
	t.Helper()
	payload, err := engine_types.ExecutionPayloadFromBlock(block)
	require.NoError(t, err)
	status, err := srv.NewPayloadV2(t.Context(), payload)
	require.NoError(t, err)
	return status
}

// forkchoiceV2AndWait also waits for the background commit, after which the execution module is
// idle and validates the next payload.
func forkchoiceV2AndWait(t *testing.T, srv *EngineServer, head common.Hash) *engine_types.PayloadStatus {
	t.Helper()
	r, err := srv.ForkchoiceUpdatedV2(t.Context(), &engine_types.ForkChoiceState{HeadHash: head}, nil)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		ready, err := srv.executionService.Ready(t.Context())
		return err == nil && ready
	}, time.Minute, time.Millisecond)
	return r.PayloadStatus
}

func payloadStatusJSON(t *testing.T, status *engine_types.PayloadStatus) string {
	t.Helper()
	encoded, err := json.Marshal(status)
	require.NoError(t, err)
	return string(encoded)
}

func bodiesServedByHash(t *testing.T, srv *EngineServer, hashes ...common.Hash) (v1, v2 []bool) {
	t.Helper()
	bodiesV1, err := srv.GetPayloadBodiesByHashV1(t.Context(), hashes)
	require.NoError(t, err)
	bodiesV2, err := srv.GetPayloadBodiesByHashV2(t.Context(), hashes)
	require.NoError(t, err)
	for i := range hashes {
		v1 = append(v1, bodiesV1[i] != nil)
		v2 = append(v2, bodiesV2[i] != nil)
	}
	return v1, v2
}

// storingAfterValidation stores the block again right after the first validation of its hash, as
// a concurrent insertion of the same block can.
type storingAfterValidation struct {
	execmodule.ExecutionModule
	t      *testing.T
	block  *types.Block
	stored atomic.Bool
}

func (m *storingAfterValidation) ValidateChain(ctx context.Context, block *types.Block) (execmodule.ValidationResult, error) {
	result, err := m.ExecutionModule.ValidateChain(ctx, block)
	m.storeAfterFirstValidation(ctx, block)
	return result, err
}

func (m *storingAfterValidation) ValidateChainAndView(ctx context.Context, block *types.Block, wait time.Duration, fn execmodule.BlockStateView) (execmodule.ValidationResult, error) {
	result, err := m.ExecutionModule.ValidateChainAndView(ctx, block, wait, fn)
	m.storeAfterFirstValidation(ctx, block)
	return result, err
}

func (m *storingAfterValidation) storeAfterFirstValidation(ctx context.Context, block *types.Block) {
	if block.Hash() == m.block.Hash() && m.stored.CompareAndSwap(false, true) {
		_, insertErr := m.ExecutionModule.InsertBlocks(ctx, []*types.Block{m.block})
		require.NoError(m.t, insertErr)
	}
}

func withHeaderChange(block *types.Block, change func(*types.Header)) *types.Block {
	header := types.CopyHeader(block.HeaderNoCopy())
	change(header)
	return types.NewBlockFromStorage(header.Hash(), header, block.Transactions(), block.Uncles(), block.Withdrawals(), nil)
}

func withInvalidGasLimit(block *types.Block) *types.Block {
	return withHeaderChange(block, func(header *types.Header) { header.GasLimit *= 2 })
}

func TestGetPayloadBodiesByHashOmitsAnInvalidBlockStoredAfterItsValidation(t *testing.T) {
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(&types.Genesis{Config: preCancunChainConfig()}))
	chain, err := m.GenerateChain(2, func(int, *blockgen.BlockGen) {})
	require.NoError(t, err)
	block1, block2 := chain.Blocks[0], withInvalidGasLimit(chain.Blocks[1])
	srv := newEngineServerWithReorgDepth(t, m, &storingAfterValidation{ExecutionModule: m.ExecModule, t: t, block: block2}, 2)

	require.Equal(t, engine_types.ValidStatus, newPayloadV2(t, srv, block1).Status)
	require.Equal(t, engine_types.ValidStatus, forkchoiceV2AndWait(t, srv, block1.Hash()).Status)
	for _, request := range []string{"first", "repeated"} {
		require.Equal(t, engine_types.InvalidStatus, newPayloadV2(t, srv, block2).Status, request)
		require.NotNil(t, srv.chainRW.GetHeader(t.Context(), block2.Hash(), block2.NumberU64()), "the test needs the invalid block stored")

		v1, v2 := bodiesServedByHash(t, srv, block1.Hash(), block2.Hash())
		require.Equal(t, []bool{true, false}, v1, "%s request: V1", request)
		require.Equal(t, []bool{true, false}, v2, "%s request: V2", request)
	}
}

// witnessChain is a committed genesis with a four-block chain of transfers and a three-block side fork on it, none of
// them sent to the server yet.
type witnessChain struct {
	t      *testing.T
	srv    *EngineServer
	blocks []*types.Block
	side   []*types.Block
}

func newWitnessChain(t *testing.T, wrap func(*execmoduletester.ExecModuleTester, []*types.Block) execmodule.ExecutionModule, opts ...execmoduletester.Option) *witnessChain {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	genesis := &types.Genesis{Config: preCancunChainConfig(), Alloc: types.GenesisAlloc{sender: {Balance: big.NewInt(1e18)}}}
	m := execmoduletester.New(t, append([]execmoduletester.Option{execmoduletester.WithGenesisSpec(genesis), execmoduletester.WithKey(key)}, opts...)...)
	transfer := func(to common.Address) func(int, *blockgen.BlockGen) {
		return func(i int, b *blockgen.BlockGen) {
			txn, err := types.SignTx(types.NewTransaction(uint64(i), to, uint256.NewInt(uint64(i+1)), 21_000, uint256.NewInt(m.Genesis.BaseFee().Uint64()), nil), *types.LatestSignerForChainID(nil), key)
			require.NoError(t, err)
			b.AddTx(txn)
		}
	}
	chain, err := m.GenerateChain(4, transfer(common.Address{0x01}))
	require.NoError(t, err)
	side, err := m.GenerateChain(3, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{0x99})
		transfer(common.Address{0x02})(i, b)
	})
	require.NoError(t, err)
	var module execmodule.ExecutionModule = m.ExecModule
	if wrap != nil {
		module = wrap(m, chain.Blocks)
	}
	srv := newEngineServerWithReorgDepth(t, m, module, 16)
	srv.witnessBuilder = jsonrpc.NewPrivateDebugAPI(jsonrpc.NewBaseApi(nil, m.StateCache, m.BlockReader, m.Engine, &rpccfg.BaseApiConfig{Dirs: m.Dirs}), m.DB, nil, &rpccfg.DebugApiConfig{})
	return &witnessChain{t: t, srv: srv, blocks: chain.Blocks, side: side.Blocks}
}

// witness sends block as the witness route does.
func (c *witnessChain) witness(block *types.Block) (*engine_types.PayloadStatus, *jsonrpc.ExecutionWitnessResult, error) {
	payload, err := engine_types.ExecutionPayloadFromBlock(block)
	require.NoError(c.t, err)
	var witness *jsonrpc.ExecutionWitnessResult
	var witnessErr error
	status, err := c.srv.newPayload(c.t.Context(), payload, nil, nil, nil, clparams.CapellaVersion, false, func(blockHash common.Hash) {
		witness, witnessErr = c.srv.payloadWitness(c.t.Context(), blockHash)
	})
	require.NoError(c.t, err)
	return status, witness, witnessErr
}

func (c *witnessChain) requireWitness(block *types.Block) *jsonrpc.ExecutionWitnessResult {
	c.t.Helper()
	status, witness, err := c.witness(block)
	require.Equal(c.t, engine_types.ValidStatus, status.Status)
	require.NoError(c.t, err)
	require.NotEmpty(c.t, witness.State)
	return witness
}

func (c *witnessChain) dropCachedWitnesses() {
	c.srv.payloadWitnesses = jsonrpc.NewWitnessResultCache(payloadWitnessCacheSize, payloadWitnessCacheBytes)
}

// requireCommittedWitnesses checks each witness against the one rebuilt from the committed changesets of its block.
func (c *witnessChain) requireCommittedWitnesses(blocks []*types.Block, witnesses []*jsonrpc.ExecutionWitnessResult) {
	c.t.Helper()
	c.dropCachedWitnesses()
	for i, witness := range witnesses {
		committed := c.requireWitness(blocks[i])
		require.Equal(c.t, committed.State, witness.State, "block %d", blocks[i].NumberU64())
		require.Equal(c.t, committed.Codes, witness.Codes, "block %d", blocks[i].NumberU64())
		require.Equal(c.t, committed.Headers, witness.Headers, "block %d", blocks[i].NumberU64())
	}
}

// commitWithWitnesses commits blocks one by one through the witness route.
func (c *witnessChain) commitWithWitnesses(blocks []*types.Block) []*jsonrpc.ExecutionWitnessResult {
	c.t.Helper()
	var witnesses []*jsonrpc.ExecutionWitnessResult
	for _, block := range blocks {
		witnesses = append(witnesses, c.requireWitness(block))
		require.Equal(c.t, engine_types.ValidStatus, forkchoiceV2AndWait(c.t, c.srv, block.Hash()).Status)
	}
	return witnesses
}

func storingFirstBlockAfterValidation(t *testing.T) func(*execmoduletester.ExecModuleTester, []*types.Block) execmodule.ExecutionModule {
	return func(m *execmoduletester.ExecModuleTester, blocks []*types.Block) execmodule.ExecutionModule {
		return &storingAfterValidation{ExecutionModule: m.ExecModule, t: t, block: blocks[0]}
	}
}

// A block insertion that takes the execution module as soon as the validation releases it, as the background block
// downloader can, drops the validated state; the payload witness must still be built from that state.
func TestNewPayloadWitnessSurvivesAnInsertionAfterValidation(t *testing.T) {
	c := newWitnessChain(t, storingFirstBlockAfterValidation(t))

	c.requireWitness(c.blocks[0])
}

// A payload validated by a plain newPayload whose retained state a block insertion drops is still a VALID payload with
// a witness.
func TestNewPayloadWitnessForAValidPayloadWhoseStateWasDropped(t *testing.T) {
	c := newWitnessChain(t, storingFirstBlockAfterValidation(t))
	require.Equal(t, engine_types.ValidStatus, newPayloadV2(t, c.srv, c.blocks[0]).Status)

	c.requireWitness(c.blocks[0])
}

func TestNewPayloadWitnessForAValidPayloadThatIsNoLongerTheRetainedTip(t *testing.T) {
	c := newWitnessChain(t, nil)
	witnesses := []*jsonrpc.ExecutionWitnessResult{c.requireWitness(c.blocks[0]), c.requireWitness(c.blocks[1])}
	c.dropCachedWitnesses()

	for i, witness := range witnesses {
		require.Equal(t, witness, c.requireWitness(c.blocks[i]), "block %d", c.blocks[i].NumberU64())
	}
}

func TestNewPayloadWitnessForChildrenOfUncommittedPayloads(t *testing.T) {
	c := newWitnessChain(t, nil)
	c.requireWitness(c.blocks[0])
	forkchoiceV2AndWait(t, c.srv, c.blocks[0].Hash())

	uncommitted := []*jsonrpc.ExecutionWitnessResult{c.requireWitness(c.blocks[1]), c.requireWitness(c.blocks[2]), c.requireWitness(c.blocks[3])}

	require.Equal(t, engine_types.ValidStatus, forkchoiceV2AndWait(t, c.srv, c.blocks[3].Hash()).Status)
	c.requireCommittedWitnesses(c.blocks[1:], uncommitted)
}

func TestNewPayloadWitnessForASideFork(t *testing.T) {
	c := newWitnessChain(t, nil)
	c.commitWithWitnesses(c.blocks[:2])

	side := []*jsonrpc.ExecutionWitnessResult{c.requireWitness(c.side[0]), c.requireWitness(c.side[1]), c.requireWitness(c.side[2])}

	require.Equal(t, engine_types.ValidStatus, forkchoiceV2AndWait(t, c.srv, c.side[2].Hash()).Status)
	c.requireCommittedWitnesses(c.side, side)
}

func TestNewPayloadWitnessForAReorgedOutPayload(t *testing.T) {
	c := newWitnessChain(t, nil)
	canonical := c.commitWithWitnesses(c.blocks[:2])
	for _, block := range c.side {
		require.Equal(t, engine_types.ValidStatus, newPayloadV2(t, c.srv, block).Status)
	}
	require.Equal(t, engine_types.ValidStatus, forkchoiceV2AndWait(t, c.srv, c.side[2].Hash()).Status)
	c.dropCachedWitnesses()

	for i, witness := range canonical {
		require.Equal(t, witness, c.requireWitness(c.blocks[i]), "block %d", c.blocks[i].NumberU64())
	}
}

// The background commit of a forkchoice update holds the execution module after the update is answered; a witness
// request for the new head right after it must not wait for the module.
func TestNewPayloadWitnessForACommittedPayloadWhileTheModuleIsBusy(t *testing.T) {
	var hold atomic.Bool
	committed, release := make(chan struct{}), make(chan struct{})
	observer := func(ctx context.Context, p execmodule.StateTransitionPoint) {
		if p == execmodule.StateTransitionCommitComplete && hold.CompareAndSwap(true, false) {
			close(committed)
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
	}
	c := newWitnessChain(t, nil, execmoduletester.WithParallelStateFlushing(true), execmoduletester.WithStateTransitionObserver(observer))
	t.Cleanup(func() { close(release) })
	require.Equal(t, engine_types.ValidStatus, newPayloadV2(t, c.srv, c.blocks[0]).Status)
	hold.Store(true)
	r, err := c.srv.ForkchoiceUpdatedV2(t.Context(), &engine_types.ForkChoiceState{HeadHash: c.blocks[0].Hash()}, nil)
	require.NoError(t, err)
	require.Equal(t, engine_types.ValidStatus, r.PayloadStatus.Status)
	<-committed

	c.requireWitness(c.blocks[0])
}

// A known VALID payload whose state must be rebuilt gets SYNCING while the module is busy, as /payloads answers.
func TestNewPayloadWitnessAnswersSyncingWhenTheModuleIsBusy(t *testing.T) {
	srv, stub, payload := newValidPayloadServer(t)
	parentHeader := stub.getHeaderFunc
	stub.getHeaderFunc = func(ctx context.Context, blockHash *common.Hash, blockNumber *uint64) (*types.Header, error) {
		if blockHash != nil && *blockHash == payload.BlockHash {
			return &types.Header{Number: *uint256.NewInt(uint64(payload.BlockNumber))}, nil
		}
		return parentHeader(ctx, blockHash, blockNumber)
	}
	stub.isValidatedFunc = func(hash common.Hash) bool { return hash == payload.BlockHash }
	stub.validateChainBlockFunc = func(context.Context, *types.Block) (execmodule.ValidationResult, error) {
		return execmodule.ValidationResult{ValidationStatus: execmodule.ExecutionStatusBusy}, nil
	}

	onValidCalled := false
	status, err := srv.newPayload(t.Context(), payload, nil, nil, nil, clparams.CapellaVersion, false, func(common.Hash) { onValidCalled = true })
	require.NoError(t, err)
	require.Equal(t, engine_types.SyncingStatus, status.Status)
	require.False(t, onValidCalled)
}

func TestGetPayloadBodiesByHashOmitsABlockRejectedByAForkchoiceUpdate(t *testing.T) {
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(&types.Genesis{Config: preCancunChainConfig()}))
	chain, err := m.GenerateChain(1, func(int, *blockgen.BlockGen) {})
	require.NoError(t, err)
	block := withHeaderChange(chain.Blocks[0], func(header *types.Header) { header.Root = common.Hash{0x01} })
	srv := newEngineServerWithReorgDepth(t, m, m.ExecModule, 1)

	require.Equal(t, engine_types.AcceptedStatus, newPayloadV2(t, srv, block).Status, "the block must be stored without a validation")
	require.Equal(t, engine_types.InvalidStatus, forkchoiceV2AndWait(t, srv, block.Hash()).Status)
	require.NotNil(t, srv.chainRW.GetHeader(t.Context(), block.Hash(), block.NumberU64()), "the test needs the rejected block stored")

	v1, v2 := bodiesServedByHash(t, srv, m.Genesis.Hash(), block.Hash())
	require.Equal(t, []bool{true, false}, v1, "V1")
	require.Equal(t, []bool{true, false}, v2, "V2")
}

func TestGetPayloadBodiesByHashOmitsABlockRejectedForABadAncestor(t *testing.T) {
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(&types.Genesis{Config: preCancunChainConfig()}))
	chain, err := m.GenerateChain(5, func(int, *blockgen.BlockGen) {})
	require.NoError(t, err)
	block1, bad := chain.Blocks[0], withInvalidGasLimit(chain.Blocks[1])
	child := withHeaderChange(chain.Blocks[2], func(header *types.Header) { header.ParentHash = bad.Hash() })
	head := withHeaderChange(chain.Blocks[3], func(header *types.Header) { header.ParentHash = child.Hash() })
	headChild := withHeaderChange(chain.Blocks[4], func(header *types.Header) { header.ParentHash = head.Hash() })
	srv := newEngineServerWithReorgDepth(t, m, m.ExecModule, 2)

	require.Equal(t, engine_types.ValidStatus, newPayloadV2(t, srv, block1).Status)
	for _, block := range []*types.Block{bad, child, head} {
		require.Equal(t, engine_types.AcceptedStatus, newPayloadV2(t, srv, block).Status, "the block must be stored without a validation")
	}
	require.Equal(t, engine_types.ValidStatus, forkchoiceV2AndWait(t, srv, block1.Hash()).Status)
	badStatus := newPayloadV2(t, srv, bad)
	require.Equal(t, engine_types.InvalidStatus, badStatus.Status)
	require.Nil(t, srv.chainRW.GetHeader(t.Context(), bad.Hash(), bad.NumberU64()), "the test needs the bad ancestor removed")

	rejected := forkchoiceV2AndWait(t, srv, head.Hash())
	require.Equal(t, engine_types.InvalidStatus, rejected.Status)
	require.Equal(t, block1.Hash(), *rejected.LatestValidHash)
	require.EqualError(t, rejected.ValidationError.Error(), fmt.Sprintf("ancestor %s rejected: %s", bad.Hash(), badStatus.ValidationError.Error()))
	require.NotNil(t, srv.chainRW.GetHeader(t.Context(), head.Hash(), head.NumberU64()), "the test needs the rejected block stored")
	v1, v2 := bodiesServedByHash(t, srv, block1.Hash(), head.Hash())
	require.Equal(t, []bool{true, false}, v1, "V1")
	require.Equal(t, []bool{true, false}, v2, "V2")

	require.JSONEq(t, payloadStatusJSON(t, rejected), payloadStatusJSON(t, forkchoiceV2AndWait(t, srv, head.Hash())), "a repeated forkchoice update")
	require.JSONEq(t, payloadStatusJSON(t, rejected), payloadStatusJSON(t, newPayloadV2(t, srv, head)), "a later newPayload")

	headChildStatus := newPayloadV2(t, srv, headChild)
	require.Equal(t, engine_types.InvalidStatus, headChildStatus.Status)
	require.Equal(t, block1.Hash(), *headChildStatus.LatestValidHash)
	require.EqualError(t, headChildStatus.ValidationError.Error(), fmt.Sprintf("ancestor %s rejected: %s", head.Hash(), rejected.ValidationError.Error()))
}
