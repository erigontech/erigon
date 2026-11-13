// Copyright 2026 The Erigon Authors
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

package execmoduletester

import (
	"bytes"
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/kv/remotedbserver"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/notifications"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/node/privateapi"
	"github.com/erigontech/erigon/txnprovider/txpool"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

// Every block above genesis calls logEmitter once, so each block has exactly one log.
var logEmitter = common.Address{0xbb}

type reorgSubscriptions struct {
	headers      [][]byte
	logs         []*notifications.LogNotification
	receipts     []*notifications.ReceiptNotification
	stateChanges *remoteproto.StateChangeBatch
}

func collectReorgSubscriptions(headers <-chan [][]byte, logs <-chan []*notifications.LogNotification, receipts <-chan []*notifications.ReceiptNotification) *reorgSubscriptions {
	got := &reorgSubscriptions{}
	for {
		select {
		case h := <-headers:
			got.headers = append(got.headers, h...)
		case l := <-logs:
			got.logs = append(got.logs, l...)
		case r := <-receipts:
			got.receipts = append(got.receipts, r...)
		default:
			return got
		}
	}
}

func (s *reorgSubscriptions) headerHashes(t *testing.T) []common.Hash {
	hashes := make([]common.Hash, 0, len(s.headers))
	for _, enc := range s.headers {
		var h types.Header
		require.NoError(t, rlp.DecodeBytes(enc, &h))
		hashes = append(hashes, h.Hash())
	}
	return hashes
}

func (s *reorgSubscriptions) logCount(blockHash common.Hash, removed bool) int {
	var n int
	for _, l := range s.logs {
		if l.BlockHash == blockHash && l.Removed == removed {
			n++
		}
	}
	return n
}

func (s *reorgSubscriptions) receiptCount(blockHash, txHash common.Hash) int {
	var n int
	for _, receipt := range s.receipts {
		if receipt.Receipt.BlockHash == blockHash && receipt.Receipt.TxHash == txHash {
			n++
		}
	}
	return n
}

type reorgCase struct {
	name       string
	forkLen    int
	newPayload bool
}

var reorgCases = []reorgCase{
	{name: "SameHeightViaNewPayload", forkLen: 1, newPayload: true},
	{name: "SameHeightViaForkchoiceOnly", forkLen: 1, newPayload: false},
	{name: "HigherHeadViaNewPayload", forkLen: 2, newPayload: true},
	{name: "HigherHeadViaForkchoiceOnly", forkLen: 2, newPayload: false},
	{name: "EightBlockForkViaNewPayload", forkLen: 8, newPayload: true},
	{name: "EightBlockForkViaForkchoiceOnly", forkLen: 8, newPayload: false},
	{name: "NineBlockForkViaNewPayload", forkLen: 9, newPayload: true},
	{name: "NineBlockForkViaForkchoiceOnly", forkLen: 9, newPayload: false},
}

func newReorgChains(t *testing.T, canonicalLen, sideLen int, opts ...Option) (*ExecModuleTester, *blockgen.ChainPack, *blockgen.ChainPack) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	m := New(t, append([]Option{WithGenesisSpec(&types.Genesis{
		Config: chain.AllProtocolChanges,
		Alloc: types.GenesisAlloc{
			sender:     {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)},
			logEmitter: {Code: []byte{0x60, 0x00, 0x60, 0x00, 0xa0, 0x00}}, // PUSH1 0 PUSH1 0 LOG0 STOP
		},
	}), WithKey(key)}, opts...)...)
	signer := *types.LatestSignerForChainID(m.ChainConfig.ChainID)
	gasPrice := uint256.NewInt(m.Genesis.BaseFee().Uint64() * 2)
	sign := func(txn types.Transaction) types.Transaction {
		signed, err := types.SignTx(txn, signer, key)
		require.NoError(t, err)
		return signed
	}
	genBlocks := func(isCanonical bool) func(int, *blockgen.BlockGen) {
		return func(i int, b *blockgen.BlockGen) {
			nonce := uint64(i)
			if isCanonical && i > 1 {
				nonce++
			}
			b.AddTx(sign(types.NewTransaction(nonce, logEmitter, uint256.NewInt(0), 100_000, gasPrice, nil)))
			if isCanonical && i == 1 {
				b.AddTx(sign(types.NewTransaction(uint64(i)+1, common.Address{0x01}, uint256.NewInt(1), 21_000, gasPrice, nil)))
			}
		}
	}
	canonical, err := m.GenerateChain(canonicalLen, genBlocks(true))
	require.NoError(t, err)
	side, err := m.GenerateChain(sideLen, genBlocks(false))
	require.NoError(t, err)
	require.Equal(t, canonical.Blocks[0].Hash(), side.Blocks[0].Hash())
	return m, canonical, side
}

func subscribeStateChangeHeads(t *testing.T, m *ExecModuleTester) func(common.Hash) *remoteproto.StateChangeBatch {
	batches := make(chan *remoteproto.StateChangeBatch, 16)
	stream, err := m.stateChangesClient.StateChanges(t.Context(), &remoteproto.StateChangeRequest{}, grpc.WaitForReady(true))
	require.NoError(t, err)
	go func() {
		for {
			batch, err := stream.Recv()
			if err != nil {
				return
			}
			batches <- batch
		}
	}()
	return func(head common.Hash) *remoteproto.StateChangeBatch {
		t.Helper()
		deadline := time.After(10 * time.Second)
		for {
			select {
			case batch := <-batches:
				last := batch.ChangeBatch[len(batch.ChangeBatch)-1]
				if gointerfaces.ConvertH256ToHash(last.BlockHash) == head {
					return batch
				}
			case <-deadline:
				t.Fatalf("no state-change batch announced head %x", head)
			}
		}
	}
}

// runReorg builds genesis -> b1 -> oldHead, makes it canonical, then switches the head to a fork
// b1 -> forkBlocks[0] -> ... and returns what the newHeads, logs, and receipts subscribers received
// during the switch.
func runReorg(t *testing.T, tc reorgCase) (oldHead *types.Block, forkBlocks []*types.Block, got *reorgSubscriptions) {
	m, oldChain, newChain := newReorgChains(t, 2, 1+tc.forkLen)
	oldHead, forkBlocks = oldChain.Blocks[1], newChain.Blocks[1:]
	newHead := forkBlocks[len(forkBlocks)-1]
	// Dispatch sends headers and logs before the state-change batch, so a batch announcing head
	// means every header and log of that forkchoice update is already in the subscription channels.
	waitForBatchWithHead := subscribeStateChangeHeads(t, m)

	require.NoError(t, m.InsertValidateAndUfc1By1(t.Context(), oldChain.Blocks))
	m.ExecModule.WaitIdle(t.Context())
	waitForBatchWithHead(oldHead.Hash())
	// A repeated forkchoice update for the current head takes the short-circuit path.
	repeated, err := m.UpdateForkChoice(t.Context(), oldHead.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, repeated.Status)
	m.ExecModule.WaitIdle(t.Context())

	headers, unsubscribe := m.Notifications.Events.AddHeaderSubscription()
	t.Cleanup(unsubscribe)
	logs, unsubscribe := m.Notifications.Events.AddLogsSubscription()
	t.Cleanup(unsubscribe)
	m.Notifications.Events.EmptyLogSubscription(false)
	receipts, unsubscribe := m.Notifications.Events.AddReceiptsSubscription()
	t.Cleanup(unsubscribe)
	m.Notifications.Events.EmptyReceiptSubscription(false)

	status, err := m.InsertBlocks(t.Context(), forkBlocks)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	if tc.newPayload {
		validation, err := m.ValidateChain(t.Context(), newHead)
		require.NoError(t, err)
		require.Equal(t, execmodule.ExecutionStatusSuccess, validation.ValidationStatus)
		require.Equal(t, newHead.Hash(), m.ExecModule.ForkValidator().ExtendingForkHeadHash())
	} else {
		require.NotEqual(t, newHead.Hash(), m.ExecModule.ForkValidator().ExtendingForkHeadHash())
	}
	fcu, err := m.UpdateForkChoice(t.Context(), newHead.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, fcu.Status)
	m.ExecModule.WaitIdle(t.Context())
	batch := waitForBatchWithHead(newHead.Hash())

	got = collectReorgSubscriptions(headers, logs, receipts)
	got.stateChanges = batch
	return oldHead, forkBlocks, got
}

func TestReorgPreservesUnwindStateChanges(t *testing.T) {
	for _, tc := range reorgCases {
		t.Run(tc.name, func(t *testing.T) {
			oldHead, forkBlocks, got := runReorg(t, tc)
			require.False(t, got.stateChanges.StateChangesIncomplete)
			changes := got.stateChanges.ChangeBatch
			require.Len(t, changes, 1+len(forkBlocks))
			require.Equal(t, remoteproto.Direction_UNWIND, changes[0].Direction)
			require.Equal(t, oldHead.ParentHash(), common.Hash(gointerfaces.ConvertH256ToHash(changes[0].BlockHash)))
			var unwound []common.Hash
			for _, raw := range changes[0].Txs {
				txn, err := types.DecodeTransaction(raw)
				require.NoError(t, err)
				unwound = append(unwound, txn.Hash())
			}
			var want []common.Hash
			for _, txn := range oldHead.Transactions() {
				want = append(want, txn.Hash())
			}
			require.ElementsMatch(t, want, unwound, "only transactions above the common ancestor may be restored")
			for i, block := range forkBlocks {
				require.Equal(t, remoteproto.Direction_FORWARD, changes[i+1].Direction)
				require.Equal(t, block.Hash(), common.Hash(gointerfaces.ConvertH256ToHash(changes[i+1].BlockHash)))
			}
		})
	}
}

func TestReorgAccountNotificationAcrossSteps(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	recipient := common.Address{0xaa}
	m := New(t, WithStepSize(8), WithKey(key), WithGenesisSpec(&types.Genesis{
		Config: chain.AllProtocolChanges,
		Alloc: types.GenesisAlloc{
			sender:    {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)},
			recipient: {Balance: big.NewInt(1)},
		},
	}))
	signer := *types.LatestSignerForChainID(m.ChainConfig.ChainID)
	gasPrice := uint256.NewInt(m.Genesis.BaseFee().Uint64() * 2)
	generate := func(canonical bool) func(int, *blockgen.BlockGen) {
		return func(i int, b *blockgen.BlockGen) {
			to := common.Address{0xbb}
			if canonical && i > 0 {
				to = recipient
			}
			txn, err := types.SignTx(types.NewTransaction(uint64(i), to, uint256.NewInt(1), 21_000, gasPrice, nil), signer, key)
			require.NoError(t, err)
			b.AddTx(txn)
		}
	}
	canonical, err := m.GenerateChain(4, generate(true))
	require.NoError(t, err)
	side, err := m.GenerateChain(4, generate(false))
	require.NoError(t, err)
	require.Equal(t, canonical.Blocks[0].Hash(), side.Blocks[0].Hash())
	waitForBatchWithHead := subscribeStateChangeHeads(t, m)
	require.NoError(t, m.InsertValidateAndUfc1By1(t.Context(), canonical.Blocks))
	m.ExecModule.WaitIdle(t.Context())
	waitForBatchWithHead(canonical.TopBlock.Hash())

	require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
		first, err := m.BlockReader.TxnumReader().Min(t.Context(), tx, 2)
		require.NoError(t, err)
		last, err := m.BlockReader.TxnumReader().Min(t.Context(), tx, 4)
		require.NoError(t, err)
		require.Less(t, first/tx.Debug().StepSize(), last/tx.Debug().StepSize())
		return nil
	}))
	status, err := m.InsertBlocks(t.Context(), side.Blocks[1:])
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	result, err := m.UpdateForkChoice(t.Context(), side.TopBlock.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
	m.ExecModule.WaitIdle(t.Context())
	batch := waitForBatchWithHead(side.TopBlock.Hash())
	require.False(t, batch.StateChangesIncomplete)
	cache := kvcache.NewLatestBatchCache()
	cache.OnNewBlock(batch)
	require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
		want, _, err := tx.GetLatest(kv.AccountsDomain, recipient[:], kv.GetLatestOptions{})
		require.NoError(t, err)
		view, err := cache.View(t.Context(), tx)
		require.NoError(t, err)
		got, err := view.Get(recipient[:])
		require.NoError(t, err)
		require.Equal(t, want, got, "the replacement branch leaves the recipient untouched, so its unwind notification must contain the restored account")
		return nil
	}))
}

func TestReorgCodeRollbackPreservesRecentRoots(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	contract := types.CreateAddress(sender, 1)
	cfg := chain.AllProtocolChanges.Copy()
	cfg.BogotaTime = common.NewUint64(1)
	frameSender := common.Address{0xcc}
	data := make([]byte, 72)
	data[0], data[39], data[71] = 1, 1, 2
	frame := &types.FrameTransaction{
		NonceKeys: nil, ChainID: *cfg.ChainID, SenderAddress: frameSender,
		Frames: []types.Frame{
			{Mode: types.FrameVerify, Target: &types.FrameRecentRootVerifier, Data: data, Limits: mdgas.MdGas{Execution: 20_000}},
			{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}},
		},
		Fees: types.FrameFees{MaxFee: *uint256.NewInt(10_000_000_000)},
	}
	refs := protocol.FrameTransactionRecentRoots(frame)
	require.Len(t, refs, 1)
	m := New(t, WithKey(key), WithGenesisSpec(&types.Genesis{
		Config: cfg, SlotNumber: common.NewUint64(0),
		Alloc: types.GenesisAlloc{
			sender:      {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)},
			contract:    {Balance: big.NewInt(1)},
			frameSender: {Balance: big.NewInt(1_000_000_000_000_000_000), Code: []byte{0x60, 3, 0x5f, 0x5f, 0xaa}},
			types.FrameRecentRootVerifier: {Code: protocol.RecentRootCode, Storage: map[common.Hash]common.Hash{
				refs[0].StorageKey: refs[0].EntryHash,
			}},
		},
	}))
	signer := *types.LatestSignerForChainID(m.ChainConfig.ChainID)
	gasPrice := uint256.NewInt(m.Genesis.BaseFee().Uint64() * 2)
	generate := func(deploy bool) func(int, *blockgen.BlockGen) {
		return func(i int, b *blockgen.BlockGen) {
			var txn types.Transaction = types.NewTransaction(uint64(i), common.Address{0xbb}, uint256.NewInt(0), 21_000, gasPrice, nil)
			if deploy && i == 1 {
				txn = types.NewContractCreation(1, uint256.NewInt(0), 100_000, gasPrice, common.FromHex("60016000f3"))
			}
			txn, err := types.SignTx(txn, signer, key)
			require.NoError(t, err)
			b.AddTx(txn)
		}
	}
	canonical, err := m.GenerateChain(2, generate(true))
	require.NoError(t, err)
	side, err := m.GenerateChain(2, generate(false))
	require.NoError(t, err)
	waitForBatchWithHead := subscribeStateChangeHeads(t, m)
	require.NoError(t, m.InsertValidateAndUfc1By1(t.Context(), canonical.Blocks))
	m.ExecModule.WaitIdle(t.Context())
	initial := waitForBatchWithHead(canonical.TopBlock.Hash())
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	logger := log.New()
	backend := privateapi.NewEthBackendServer(ctx, nil, m.DB, m.Notifications, m.BlockReader, logger, nil, cfg)
	poolDB := mdbxtest.NewTestPoolDB(t)
	startupStateChanges := remotedbserver.NewKvServer(ctx, m.DB, nil, nil, logger)
	startupStateChanges.SendStateChanges(ctx, initial)
	pool, err := txpool.New(ctx, make(chan txpool.Announcements, 1), poolDB, m.DB, txpoolcfg.DefaultConfig,
		kvcache.NewLatestBatchCache(), cfg, nil, direct.NewStateDiffClientDirect(startupStateChanges), func() {}, nil, direct.NewEthBackendClientDirect(backend), logger, txpool.WithFeeCalculator(nil))
	require.NoError(t, err)
	runDone := make(chan error, 1)
	go func() { runDone <- pool.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		require.ErrorIs(t, <-runDone, context.Canceled)
	})
	require.Eventually(t, pool.Started, 5*time.Second, time.Millisecond)
	var encoded bytes.Buffer
	require.NoError(t, frame.MarshalBinary(&encoded))
	server := txpool.NewGrpcServer(ctx, pool, poolDB, nil, *cfg.ChainID, logger)
	reply, err := server.Add(ctx, &txpoolproto.AddRequest{RlpTxs: [][]byte{encoded.Bytes()}})
	require.NoError(t, err)
	require.Equal(t, []txpoolproto.ImportResult{txpoolproto.ImportResult_SUCCESS}, reply.Imported, "%v", reply.Errors)
	require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
		code, _, err := tx.GetLatest(kv.CodeDomain, contract[:], kv.GetLatestOptions{})
		require.NoError(t, err)
		require.Equal(t, []byte{0}, code)
		return nil
	}))
	status, err := m.InsertBlocks(t.Context(), side.Blocks[1:])
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	result, err := m.UpdateForkChoice(t.Context(), side.TopBlock.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
	m.ExecModule.WaitIdle(t.Context())
	batch := waitForBatchWithHead(side.TopBlock.Hash())
	require.NoError(t, pool.OnNewBlock(ctx, batch, txpool.TxnSlots{}, txpool.TxnSlots{}, txpool.TxnSlots{}))
	hash := frame.Hash()
	require.Equal(t, txpoolcfg.AlreadyKnown, pool.KnownTxnReason(hash[:]), "the unrelated deployment rollback must keep the recent-root transaction pooled")
	require.False(t, batch.StateChangesIncomplete, "a streamed code rollback must preserve unrelated frame validations")
	var restored *remoteproto.AccountChange
	for _, change := range batch.ChangeBatch {
		if change.Direction != remoteproto.Direction_UNWIND {
			continue
		}
		for _, account := range change.Changes {
			if gointerfaces.ConvertH160toAddress(account.Address) == contract {
				restored = account
			}
		}
	}
	require.NotNil(t, restored)
	require.Equal(t, remoteproto.Action_UPSERT_CODE, restored.Action)
	require.Empty(t, restored.Code)
	require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
		code, _, err := tx.GetLatest(kv.CodeDomain, contract[:], kv.GetLatestOptions{})
		require.NoError(t, err)
		require.Empty(t, code)
		account, _, err := tx.GetLatest(kv.AccountsDomain, contract[:], kv.GetLatestOptions{})
		require.NoError(t, err)
		require.NotEmpty(t, account, "the funded account survives the code rollback")
		return nil
	}))
}

func TestPayloadValidationDoesNotAnnounceUnwind(t *testing.T) {
	m, canonical, side := newReorgChains(t, 3, 2)
	waitForBatchWithHead := subscribeStateChangeHeads(t, m)
	require.NoError(t, m.InsertValidateAndUfc1By1(t.Context(), canonical.Blocks[:2]))
	m.ExecModule.WaitIdle(t.Context())
	waitForBatchWithHead(canonical.Blocks[1].Hash())
	require.Empty(t, m.Notifications.Accumulator.Changes())

	status, err := m.InsertBlocks(t.Context(), side.Blocks[1:])
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	validation, err := m.ValidateChain(t.Context(), side.Blocks[1])
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, validation.ValidationStatus)
	require.Empty(t, m.Notifications.Accumulator.Changes(), "validation must not announce a canonical unwind")

	status, err = m.InsertBlocks(t.Context(), canonical.Blocks[2:])
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	fcu, err := m.UpdateForkChoice(t.Context(), canonical.Blocks[2].Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, fcu.Status)
	m.ExecModule.WaitIdle(t.Context())
	batch := waitForBatchWithHead(canonical.Blocks[2].Hash())
	require.False(t, batch.StateChangesIncomplete)
	require.Len(t, batch.ChangeBatch, 1)
	require.Equal(t, remoteproto.Direction_FORWARD, batch.ChangeBatch[0].Direction)
}

func TestReorgAnnouncesNewCanonicalHeaders(t *testing.T) {
	for _, tc := range reorgCases {
		t.Run(tc.name, func(t *testing.T) {
			_, forkBlocks, got := runReorg(t, tc)
			want := make([]common.Hash, 0, len(forkBlocks))
			for _, b := range forkBlocks {
				want = append(want, b.Hash())
			}
			require.ElementsMatch(t, want, got.headerHashes(t), "newHeads subscribers were not told about the new canonical blocks")
		})
	}
}

func TestReorgMarksOldHeadLogsRemoved(t *testing.T) {
	for _, tc := range reorgCases {
		t.Run(tc.name, func(t *testing.T) {
			oldHead, forkBlocks, got := runReorg(t, tc)
			require.Equal(t, 1, got.logCount(oldHead.Hash(), true), "old head's log was not reported as removed")
			for _, b := range forkBlocks {
				require.Zero(t, got.logCount(b.Hash(), true), "log of new canonical block %d reported as removed", b.NumberU64())
			}
		})
	}
}

func TestReorgAnnouncesNewCanonicalLogs(t *testing.T) {
	for _, tc := range reorgCases {
		t.Run(tc.name, func(t *testing.T) {
			oldHead, forkBlocks, got := runReorg(t, tc)
			for _, b := range forkBlocks {
				require.Equal(t, 1, got.logCount(b.Hash(), false), "log of new canonical block %d was not reported", b.NumberU64())
			}
			require.Zero(t, got.logCount(oldHead.Hash(), false), "old head's log was reported again as added")
		})
	}
}

func TestReorgAnnouncesTransactionReceipts(t *testing.T) {
	for _, tc := range reorgCases {
		t.Run(tc.name, func(t *testing.T) {
			oldHead, forkBlocks, got := runReorg(t, tc)
			wantCount := 0
			for _, tx := range oldHead.Transactions() {
				require.Zero(t, got.receiptCount(oldHead.Hash(), tx.Hash()))
			}
			for _, block := range forkBlocks {
				wantCount += len(block.Transactions())
				for _, tx := range block.Transactions() {
					require.Equal(t, 1, got.receiptCount(block.Hash(), tx.Hash()))
				}
			}
			require.Len(t, got.receipts, wantCount)
		})
	}
}

func TestReorgToLowerHeadNotifiesRemovedAndAddedLogs(t *testing.T) {
	m, canonical, side := newReorgChains(t, 3, 2)
	a2, a3 := canonical.Blocks[1], canonical.Blocks[2]
	b2 := side.Blocks[1]
	waitForBatchWithHead := subscribeStateChangeHeads(t, m)

	require.NoError(t, m.InsertValidateAndUfc1By1(t.Context(), canonical.Blocks))
	m.ExecModule.WaitIdle(t.Context())
	waitForBatchWithHead(a3.Hash())

	headers, unsubscribe := m.Notifications.Events.AddHeaderSubscription()
	t.Cleanup(unsubscribe)
	logs, unsubscribe := m.Notifications.Events.AddLogsSubscription()
	t.Cleanup(unsubscribe)
	m.Notifications.Events.EmptyLogSubscription(false)
	status, err := m.InsertBlocks(t.Context(), []*types.Block{b2})
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	fcu, err := m.UpdateForkChoice(t.Context(), b2.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, fcu.Status)
	m.ExecModule.WaitIdle(t.Context())
	waitForBatchWithHead(b2.Hash())

	got := collectReorgSubscriptions(headers, logs, nil)
	require.Equal(t, []common.Hash{b2.Hash()}, got.headerHashes(t))
	require.Equal(t, 1, got.logCount(a2.Hash(), true))
	require.Equal(t, 1, got.logCount(a3.Hash(), true))
	require.Equal(t, 1, got.logCount(b2.Hash(), false))
	require.Len(t, got.logs, 3)
}

func TestSideForkValidationPreservesCanonicalRemovedLogs(t *testing.T) {
	m, canonical, side := newReorgChains(t, 3, 3)
	b1, a2, c3 := canonical.Blocks[0], canonical.Blocks[1], canonical.Blocks[2]
	b2, d3 := side.Blocks[1], side.Blocks[2]

	waitForBatchWithHead := subscribeStateChangeHeads(t, m)
	insertAndValidate := func(block *types.Block) {
		t.Helper()
		status, err := m.InsertBlocks(t.Context(), []*types.Block{block})
		require.NoError(t, err)
		require.Equal(t, execmodule.ExecutionStatusSuccess, status)
		validation, err := m.ValidateChain(t.Context(), block)
		require.NoError(t, err)
		require.Equal(t, execmodule.ExecutionStatusSuccess, validation.ValidationStatus)
	}
	updateForkChoice := func(block *types.Block) {
		t.Helper()
		fcu, err := m.UpdateForkChoice(t.Context(), block.Header())
		require.NoError(t, err)
		require.Equal(t, execmodule.ExecutionStatusSuccess, fcu.Status)
		m.ExecModule.WaitIdle(t.Context())
		waitForBatchWithHead(block.Hash())
	}

	require.NoError(t, m.InsertValidateAndUfc1By1(t.Context(), []*types.Block{b1, a2}))
	m.ExecModule.WaitIdle(t.Context())
	waitForBatchWithHead(a2.Hash())
	insertAndValidate(b2)
	insertAndValidate(c3)
	updateForkChoice(c3)
	insertAndValidate(d3)

	headers, unsubscribe := m.Notifications.Events.AddHeaderSubscription()
	t.Cleanup(unsubscribe)
	logs, unsubscribe := m.Notifications.Events.AddLogsSubscription()
	t.Cleanup(unsubscribe)
	m.Notifications.Events.EmptyLogSubscription(false)
	updateForkChoice(d3)

	got := collectReorgSubscriptions(headers, logs, nil)
	require.ElementsMatch(t, []common.Hash{b2.Hash(), d3.Hash()}, got.headerHashes(t))
	require.Equal(t, 1, got.logCount(a2.Hash(), true))
	require.Equal(t, 1, got.logCount(c3.Hash(), true))
	require.Equal(t, 1, got.logCount(b2.Hash(), false))
	require.Equal(t, 1, got.logCount(d3.Hash(), false))
	require.Len(t, got.logs, 4)
}

func TestFailedForkchoiceDoesNotReportNeverCanonicalLogsRemoved(t *testing.T) {
	m, canonical, side := newReorgChains(t, 3, 3)
	b1, a2, a3 := canonical.Blocks[0], canonical.Blocks[1], canonical.Blocks[2]
	b2, b3 := side.Blocks[1], side.Blocks[2]
	badHeader := types.CopyHeader(b3.HeaderNoCopy())
	badHeader.Root = common.HexToHash("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef")
	badB3 := types.NewBlockFromNetwork(badHeader, b3.Body(), b3.BlockAccessListSidecar())
	waitForBatchWithHead := subscribeStateChangeHeads(t, m)

	require.NoError(t, m.InsertValidateAndUfc1By1(t.Context(), []*types.Block{b1, a2}))
	m.ExecModule.WaitIdle(t.Context())
	waitForBatchWithHead(a2.Hash())

	status, err := m.InsertBlocks(t.Context(), []*types.Block{b2, badB3})
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	fcu, err := m.UpdateForkChoice(t.Context(), badB3.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusBadBlock, fcu.Status)

	require.NoError(t, m.InsertValidateAndUfc1By1(t.Context(), []*types.Block{a3}))
	m.ExecModule.WaitIdle(t.Context())
	waitForBatchWithHead(a3.Hash())

	logs, unsubscribe := m.Notifications.Events.AddLogsSubscription()
	t.Cleanup(unsubscribe)
	m.Notifications.Events.EmptyLogSubscription(false)
	status, err = m.InsertBlocks(t.Context(), []*types.Block{b3})
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	fcu, err = m.UpdateForkChoice(t.Context(), b3.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, fcu.Status)
	m.ExecModule.WaitIdle(t.Context())
	waitForBatchWithHead(b3.Hash())

	got := collectReorgSubscriptions(nil, logs, nil)
	require.Zero(t, got.logCount(b2.Hash(), true))
	require.Equal(t, 1, got.logCount(a2.Hash(), true))
	require.Equal(t, 1, got.logCount(a3.Hash(), true))
}

func TestMissingSegmentForkchoicePreservesCanonicalRemovedLogs(t *testing.T) {
	m, canonical, side := newReorgChains(t, 3, 3)
	a3 := canonical.Blocks[2]
	b2, b3 := side.Blocks[1], side.Blocks[2]
	orphanHeader := types.CopyHeader(b3.HeaderNoCopy())
	orphanHeader.ParentHash = common.HexToHash("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef")
	orphan := types.NewBlockFromNetwork(orphanHeader, b3.Body(), b3.BlockAccessListSidecar())
	waitForBatchWithHead := subscribeStateChangeHeads(t, m)

	require.NoError(t, m.InsertValidateAndUfc1By1(t.Context(), canonical.Blocks))
	m.ExecModule.WaitIdle(t.Context())
	waitForBatchWithHead(a3.Hash())

	require.NoError(t, m.DB.Update(t.Context(), func(tx kv.RwTx) error {
		return rawdb.WriteHeader(tx, orphan.Header())
	}))
	fcu, err := m.UpdateForkChoice(t.Context(), orphan.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusMissingSegment, fcu.Status)

	logs, unsubscribe := m.Notifications.Events.AddLogsSubscription()
	t.Cleanup(unsubscribe)
	m.Notifications.Events.EmptyLogSubscription(false)
	status, err := m.InsertBlocks(t.Context(), []*types.Block{b2, b3})
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	fcu, err = m.UpdateForkChoice(t.Context(), b3.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, fcu.Status)
	m.ExecModule.WaitIdle(t.Context())
	waitForBatchWithHead(b3.Hash())

	got := collectReorgSubscriptions(nil, logs, nil)
	require.Equal(t, 1, got.logCount(a3.Hash(), true))
}

func TestRejectedUnwindingForkchoicePreservesCanonicalNotifications(t *testing.T) {
	m, canonical, side := newReorgChains(t, 2, 2)
	b1, a2 := canonical.Blocks[0], canonical.Blocks[1]
	b2 := side.Blocks[1]
	waitForBatchWithHead := subscribeStateChangeHeads(t, m)

	require.NoError(t, m.InsertValidateAndUfc1By1(t.Context(), canonical.Blocks))
	m.ExecModule.WaitIdle(t.Context())
	waitForBatchWithHead(a2.Hash())

	fcu, err := m.UpdateForkChoice(t.Context(), b1.Header(), WithFinalisedHash(a2.Hash()), WithSafeHash(a2.Hash()))
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusInvalidForkchoice, fcu.Status)
	m.ExecModule.WaitIdle(t.Context())

	logs, unsubscribe := m.Notifications.Events.AddLogsSubscription()
	t.Cleanup(unsubscribe)
	m.Notifications.Events.EmptyLogSubscription(false)
	status, err := m.InsertBlocks(t.Context(), []*types.Block{b2})
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	fcu, err = m.UpdateForkChoice(t.Context(), b2.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, fcu.Status)
	m.ExecModule.WaitIdle(t.Context())
	batch := waitForBatchWithHead(b2.Hash())
	require.Equal(t, 2, len(batch.ChangeBatch), "only the accepted reorg's changes must be announced")
	require.Equal(t, remoteproto.Direction_UNWIND, batch.ChangeBatch[0].Direction)
	require.Equal(t, b1.Hash(), common.Hash(gointerfaces.ConvertH256ToHash(batch.ChangeBatch[0].BlockHash)))
	require.Equal(t, remoteproto.Direction_FORWARD, batch.ChangeBatch[1].Direction)
	require.Equal(t, b2.Hash(), common.Hash(gointerfaces.ConvertH256ToHash(batch.ChangeBatch[1].BlockHash)))

	got := collectReorgSubscriptions(nil, logs, nil)
	require.Equal(t, 1, got.logCount(b2.Hash(), false))
	require.Equal(t, 1, got.logCount(a2.Hash(), true), "replaced canonical block's log was not reported as removed")
}

func TestRejectedUnwindingForkchoiceDoesNotAnnounceCanonicalUnwind(t *testing.T) {
	m, canonical, _ := newReorgChains(t, 3, 3)
	b1, a2, a3 := canonical.Blocks[0], canonical.Blocks[1], canonical.Blocks[2]
	waitForBatchWithHead := subscribeStateChangeHeads(t, m)

	require.NoError(t, m.InsertValidateAndUfc1By1(t.Context(), []*types.Block{b1, a2}))
	m.ExecModule.WaitIdle(t.Context())
	waitForBatchWithHead(a2.Hash())

	fcu, err := m.UpdateForkChoice(t.Context(), b1.Header(), WithFinalisedHash(a2.Hash()), WithSafeHash(a2.Hash()))
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusInvalidForkchoice, fcu.Status)
	m.ExecModule.WaitIdle(t.Context())

	logs, unsubscribe := m.Notifications.Events.AddLogsSubscription()
	t.Cleanup(unsubscribe)
	m.Notifications.Events.EmptyLogSubscription(false)
	require.NoError(t, m.InsertValidateAndUfc1By1(t.Context(), []*types.Block{a3}))
	m.ExecModule.WaitIdle(t.Context())
	batch := waitForBatchWithHead(a3.Hash())
	require.Equal(t, 1, len(batch.ChangeBatch), "the rejected forkchoice must not leave changes for the next extension")
	require.Equal(t, remoteproto.Direction_FORWARD, batch.ChangeBatch[0].Direction)

	got := collectReorgSubscriptions(nil, logs, nil)
	require.Equal(t, 1, got.logCount(a3.Hash(), false))
	require.Zero(t, got.logCount(a2.Hash(), true), "canonical block's log was reported as removed")
}

func TestRepeatedForkchoiceDeliversIncompleteChanges(t *testing.T) {
	m, canonical, _ := newReorgChains(t, 2, 2)
	waitForBatchWithHead := subscribeStateChangeHeads(t, m)
	require.NoError(t, m.InsertValidateAndUfc1By1(t.Context(), canonical.Blocks))
	m.ExecModule.WaitIdle(t.Context())
	waitForBatchWithHead(canonical.TopBlock.Hash())

	m.Notifications.Accumulator.MarkIncomplete()
	fcu, err := m.UpdateForkChoice(t.Context(), canonical.TopBlock.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, fcu.Status)
	m.ExecModule.WaitIdle(t.Context())
	batch := waitForBatchWithHead(canonical.TopBlock.Hash())
	require.True(t, batch.StateChangesIncomplete)
	require.Len(t, batch.ChangeBatch, 1)
}

func TestStateChangesResetAfterUnpublishedCommit(t *testing.T) {
	m := New(t, WithAlwaysGenerateChangesets(true))
	blocks, err := m.GenerateChain(2, nil)
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(blocks))
	m.ExecModule.WaitIdle(t.Context())
	require.NoError(t, m.ExecModule.SetHead(t.Context(), 1))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	stream, err := m.stateChangesClient.StateChanges(ctx, &remoteproto.StateChangeRequest{})
	require.NoError(t, err)
	batch, err := stream.Recv()
	require.NoError(t, err)
	require.True(t, batch.StateChangesIncomplete)
	require.Len(t, batch.ChangeBatch, 1)
	require.Equal(t, gointerfaces.ConvertHashToH256(blocks.Blocks[0].Hash()), batch.ChangeBatch[0].BlockHash)
	require.NoError(t, m.DB.View(ctx, func(tx kv.Tx) error {
		version, err := rawdb.GetStateVersion(tx)
		require.NoError(t, err)
		require.Equal(t, version, batch.StateVersionId)
		return nil
	}))
}

func TestStateChangesResetBeforeFirstPublication(t *testing.T) {
	m := New(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	server := remotedbserver.NewKvServer(ctx, m.DB, nil, nil, m.Log)
	dispatcher := execmodule.NewDispatcher(m.ChainConfig, m.BlockReader, nil, nil, nil, m.Log)
	server.SetStateChangesSnapshot(func(ctx context.Context) (*remoteproto.StateChangeBatch, error) {
		return dispatcher.StateChangesSnapshot(ctx, m.DB)
	})
	stream, err := direct.NewStateDiffClientDirect(server).StateChanges(ctx, &remoteproto.StateChangeRequest{})
	require.NoError(t, err)
	batch, err := stream.Recv()
	require.NoError(t, err, "a subscription must not need another block to learn the committed head")
	require.True(t, batch.StateChangesIncomplete)
	require.Len(t, batch.ChangeBatch, 1)
	require.Equal(t, gointerfaces.ConvertHashToH256(m.Genesis.Hash()), batch.ChangeBatch[0].BlockHash)
}

func TestSuccessiveSetHeadAnnouncesDeepUnwindTarget(t *testing.T) {
	m := New(t, WithStepSize(10_000), WithAlwaysGenerateChangesets(true))
	chain, err := m.GenerateChain(1_003, nil)
	require.NoError(t, err)
	waitForBatchWithHead := subscribeStateChangeHeads(t, m)
	status, err := m.InsertBlocks(t.Context(), chain.Blocks)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	result, err := m.UpdateForkChoice(t.Context(), chain.TopBlock.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
	m.ExecModule.WaitIdle(t.Context())
	waitForBatchWithHead(chain.TopBlock.Hash())

	require.NoError(t, m.ExecModule.SetHead(t.Context(), 1_002))
	require.NoError(t, m.ExecModule.SetHead(t.Context(), 1))
	target := chain.Blocks[0].Header()
	require.NoError(t, m.DB.View(t.Context(), func(tx kv.Tx) error {
		require.Equal(t, target.Hash(), rawdb.ReadHeadBlockHash(tx))
		return nil
	}))
	pending := m.Notifications.Accumulator.Changes()
	require.NotEmpty(t, pending)
	require.Equal(t, target.Hash(), pending[len(pending)-1].BlockHash, "an omitted unwind must still record its committed target")

	result, err = m.UpdateForkChoice(t.Context(), target)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
	m.ExecModule.WaitIdle(t.Context())
	batch := waitForBatchWithHead(target.Hash())
	require.True(t, batch.StateChangesIncomplete)
	last := batch.ChangeBatch[len(batch.ChangeBatch)-1]
	require.Equal(t, target.Number.Uint64(), last.BlockHeight)
	require.Equal(t, target.Time, last.BlockTime)
	require.Equal(t, target.SlotNumber, last.SlotNumber)
}

func TestBelowFinalityForkchoiceKeepsPendingNotifications(t *testing.T) {
	m := New(t, WithStepSize(16), WithLoopBlockLimit(8))
	chain, err := m.GenerateChain(27, nil)
	require.NoError(t, err)
	waitForBatchWithHead := subscribeStateChangeHeads(t, m)
	require.NoError(t, m.InsertValidateAndUfc1By1(t.Context(), chain.Blocks[:2]))
	m.ExecModule.WaitIdle(t.Context())
	waitForBatchWithHead(chain.Blocks[1].Hash())
	fcu, err := m.UpdateForkChoice(t.Context(), chain.Blocks[1].Header(), WithFinalisedHash(chain.Blocks[1].Hash()))
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, fcu.Status)
	m.ExecModule.WaitIdle(t.Context())

	header := types.CopyHeader(chain.TopBlock.HeaderNoCopy())
	header.Root[0] ^= 1
	badTip := types.NewBlockFromNetwork(header, chain.TopBlock.Body(), chain.TopBlock.BlockAccessListSidecar())
	blocks := append([]*types.Block(nil), chain.Blocks[2:]...)
	blocks[len(blocks)-1] = badTip
	status, err := m.InsertBlocks(t.Context(), blocks)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	fcu, err = m.UpdateForkChoice(t.Context(), badTip.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusBadBlock, fcu.Status)
	m.ExecModule.WaitIdle(t.Context())

	retained := append([]notifications.StateChange(nil), m.Notifications.Accumulator.Changes()...)
	require.NotEmpty(t, retained)
	var committed uint64
	require.NoError(t, m.DB.View(t.Context(), func(tx kv.Tx) error {
		var err error
		committed, err = stages.GetStageProgress(tx, stages.Finish)
		require.Equal(t, badTip.Hash(), rawdb.ReadCurrentHeader(tx).Hash(), "the rejected catch-up left its requested head marker")
		return err
	}))
	require.Greater(t, committed, uint64(2))
	require.Less(t, committed, badTip.NumberU64())
	fcu, err = m.UpdateForkChoice(t.Context(), chain.Blocks[0].Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, fcu.Status)
	m.ExecModule.WaitIdle(t.Context())
	require.Equal(t, retained, m.Notifications.Accumulator.Changes(), "an ignored head must not dispatch retained changes using the stale head marker")

	accepted := chain.Blocks[committed-1]
	fcu, err = m.UpdateForkChoice(t.Context(), accepted.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, fcu.Status)
	m.ExecModule.WaitIdle(t.Context())
	batch := waitForBatchWithHead(accepted.Hash())
	for _, change := range batch.ChangeBatch {
		require.LessOrEqual(t, change.BlockHeight, committed)
	}
	require.Empty(t, m.Notifications.Accumulator.Changes())
}

func TestRejectedHeadOnlyCatchupRetainsHeaderRange(t *testing.T) {
	m := New(t, WithStepSize(16), WithLoopBlockLimit(8))
	chain, err := m.GenerateChain(100, nil)
	require.NoError(t, err)
	waitForBatchWithHead := subscribeStateChangeHeads(t, m)
	require.NoError(t, m.InsertValidateAndUfc1By1(t.Context(), chain.Blocks[:2]))
	m.ExecModule.WaitIdle(t.Context())
	waitForBatchWithHead(chain.Blocks[1].Hash())

	header := types.CopyHeader(chain.TopBlock.HeaderNoCopy())
	header.Root[0] ^= 1
	badTip := types.NewBlockFromNetwork(header, chain.TopBlock.Body(), chain.TopBlock.BlockAccessListSidecar())
	blocks := append([]*types.Block(nil), chain.Blocks[2:]...)
	blocks[len(blocks)-1] = badTip
	status, err := m.InsertBlocks(t.Context(), blocks)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	fcu, err := m.UpdateForkChoice(t.Context(), badTip.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusBadBlock, fcu.Status)
	m.ExecModule.WaitIdle(t.Context())

	var committed uint64
	require.NoError(t, m.DB.View(t.Context(), func(tx kv.Tx) error {
		var err error
		committed, err = stages.GetStageProgress(tx, stages.Finish)
		return err
	}))
	require.Greater(t, committed, uint64(10))
	require.Greater(t, m.Notifications.Accumulator.Changes()[0].BlockHeight, uint64(3), "catch-up must omit individual block changes")
	headers, unsubscribe := m.Notifications.Events.AddHeaderSubscription()
	t.Cleanup(unsubscribe)
	accepted := chain.Blocks[committed-1]
	fcu, err = m.UpdateForkChoice(t.Context(), accepted.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, fcu.Status)
	m.ExecModule.WaitIdle(t.Context())
	waitForBatchWithHead(accepted.Hash())
	var want []common.Hash
	for _, block := range chain.Blocks[2:committed] {
		want = append(want, block.Hash())
	}
	require.Equal(t, want, collectReorgSubscriptions(headers, nil, nil).headerHashes(t), "all committed catch-up headers must be announced")
}

func TestRejectedCatchupForkchoiceKeepsCommittedNotifications(t *testing.T) {
	for _, name := range []string{"committed head", "new head"} {
		t.Run(name, func(t *testing.T) {
			sameHead := name == "committed head"
			m, canonical, side := newReorgChains(t, 2, 27, WithStepSize(16), WithLoopBlockLimit(8))
			waitForBatchWithHead := subscribeStateChangeHeads(t, m)
			require.NoError(t, m.InsertValidateAndUfc1By1(t.Context(), canonical.Blocks))
			m.ExecModule.WaitIdle(t.Context())
			waitForBatchWithHead(canonical.TopBlock.Hash())

			header := types.CopyHeader(side.TopBlock.HeaderNoCopy())
			header.Root[0] ^= 1
			badTip := types.NewBlockFromNetwork(header, side.TopBlock.Body(), side.TopBlock.BlockAccessListSidecar())
			blocks := append([]*types.Block(nil), side.Blocks[1:]...)
			blocks[len(blocks)-1] = badTip
			status, err := m.InsertBlocks(t.Context(), blocks)
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusSuccess, status)
			fcu, err := m.UpdateForkChoice(t.Context(), badTip.Header())
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusBadBlock, fcu.Status)
			m.ExecModule.WaitIdle(t.Context())

			var committed uint64
			require.NoError(t, m.DB.View(t.Context(), func(tx kv.Tx) error {
				var err error
				committed, err = stages.GetStageProgress(tx, stages.Finish)
				return err
			}))
			require.Greater(t, committed, canonical.TopBlock.NumberU64(), "catch-up must commit before the rejected cycle")
			require.Less(t, committed, side.TopBlock.NumberU64()-1, "the failed cycle must also contain a valid, uncommitted block")
			changes := m.Notifications.Accumulator.Changes()
			require.Equal(t, int(committed), len(changes), "retain the canonical unwind and committed blocks, without the failed cycle")
			require.Equal(t, notifications.DirectionUnwind, changes[0].Direction)
			require.Equal(t, side.Blocks[0].Hash(), changes[0].BlockHash)
			for i, change := range changes[1:] {
				require.Equal(t, notifications.DirectionForward, change.Direction)
				require.Equal(t, side.Blocks[i+1].Hash(), change.BlockHash)
			}

			headers, unsubscribe := m.Notifications.Events.AddHeaderSubscription()
			t.Cleanup(unsubscribe)
			newHead := side.TopBlock
			if sameHead {
				newHead = side.Blocks[committed-1]
			} else {
				status, err = m.InsertBlocks(t.Context(), []*types.Block{newHead})
				require.NoError(t, err)
				require.Equal(t, execmodule.ExecutionStatusSuccess, status)
			}
			fcu, err = m.UpdateForkChoice(t.Context(), newHead.Header())
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusSuccess, fcu.Status)
			m.ExecModule.WaitIdle(t.Context())
			require.Empty(t, m.Notifications.Accumulator.Changes(), "successful forkchoice must dispatch retained changes")
			batch := waitForBatchWithHead(newHead.Hash())
			require.Equal(t, int(newHead.NumberU64()), len(batch.ChangeBatch), "committed and retried changes must be delivered once")
			require.Equal(t, remoteproto.Direction_UNWIND, batch.ChangeBatch[0].Direction)
			for i, change := range batch.ChangeBatch {
				require.Equal(t, side.Blocks[i].Hash(), common.Hash(gointerfaces.ConvertH256ToHash(change.BlockHash)))
				if i > 0 {
					require.Equal(t, remoteproto.Direction_FORWARD, change.Direction)
				}
			}
			var wantHeaders []common.Hash
			for _, block := range side.Blocks[1:newHead.NumberU64()] {
				wantHeaders = append(wantHeaders, block.Hash())
			}
			got := collectReorgSubscriptions(headers, nil, nil)
			require.Equal(t, wantHeaders, got.headerHashes(t), "headers from committed catch-up cycles must also be delivered")

			fcu, err = m.UpdateForkChoice(t.Context(), newHead.Header())
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusSuccess, fcu.Status)
			m.ExecModule.WaitIdle(t.Context())
			require.Empty(t, m.Notifications.Accumulator.Changes())
			require.Empty(t, collectReorgSubscriptions(headers, nil, nil).headers, "repeated forkchoice must not repeat delivered headers")
		})
	}
}
