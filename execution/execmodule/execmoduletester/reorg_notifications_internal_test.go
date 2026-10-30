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
	"math/big"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/notifications"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
)

// Every block above genesis calls logEmitter once, so each block has exactly one log.
var logEmitter = common.Address{0xbb}

type reorgSubscriptions struct {
	headers  [][]byte
	logs     []*notifications.LogNotification
	receipts []*notifications.ReceiptNotification
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

func newReorgChains(t *testing.T, canonicalLen, sideLen int) (*ExecModuleTester, *blockgen.ChainPack, *blockgen.ChainPack) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	m := New(t, WithGenesisSpec(&types.Genesis{
		Config: chain.AllProtocolChanges,
		Alloc: types.GenesisAlloc{
			sender:     {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)},
			logEmitter: {Code: []byte{0x60, 0x00, 0x60, 0x00, 0xa0, 0x00}}, // PUSH1 0 PUSH1 0 LOG0 STOP
		},
	}), WithKey(key))
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

func subscribeStateChangeHeads(t *testing.T, m *ExecModuleTester) func(common.Hash) {
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
	return func(head common.Hash) {
		t.Helper()
		deadline := time.After(10 * time.Second)
		for {
			select {
			case batch := <-batches:
				last := batch.ChangeBatch[len(batch.ChangeBatch)-1]
				if gointerfaces.ConvertH256ToHash(last.BlockHash) == head {
					return
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
		validation, err := m.ValidateChain(t.Context(), newHead.Header())
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
	waitForBatchWithHead(newHead.Hash())

	return oldHead, forkBlocks, collectReorgSubscriptions(headers, logs, receipts)
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
		validation, err := m.ValidateChain(t.Context(), block.Header())
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

func TestRejectedUnwindingForkchoicePreservesCanonicalRemovedLogs(t *testing.T) {
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
	waitForBatchWithHead(b2.Hash())

	got := collectReorgSubscriptions(nil, logs, nil)
	require.Equal(t, 1, got.logCount(b2.Hash(), false))
	require.Equal(t, 1, got.logCount(a2.Hash(), true), "replaced canonical block's log was not reported as removed")
}

func TestRejectedUnwindingForkchoiceDoesNotReportCanonicalLogsRemoved(t *testing.T) {
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
	waitForBatchWithHead(a3.Hash())

	got := collectReorgSubscriptions(nil, logs, nil)
	require.Equal(t, 1, got.logCount(a3.Hash(), false))
	require.Zero(t, got.logCount(a2.Hash(), true), "canonical block's log was reported as removed")
}
