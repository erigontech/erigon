// Copyright 2024 The Erigon Authors
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

package snapshotsync

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/networkname"
)

func beginTestRoTx(t *testing.T) kv.Tx {
	t.Helper()
	tx, err := memdb.NewTestDB(t, dbcfg.ChainDB).BeginRo(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tx.Rollback)
	return tx
}

type frozenBody struct {
	blockNum  uint64
	baseTxNum uint64
	txCount   uint64
}

type fakeSnapshots struct {
	services.BlockSnapshots
	max uint64
}

func (f fakeSnapshots) SegmentsMax() uint64 { return f.max }

// fakeBlockReader stubs the blockReader interface for getMinimumBlocksToDownload,
// implementing only the methods that function touches.
type fakeBlockReader struct {
	blockReader
	frozenMax uint64
	bodies    []frozenBody
}

func (f *fakeBlockReader) Snapshots() services.BlockSnapshots { return fakeSnapshots{max: f.frozenMax} }

func (f *fakeBlockReader) IterateFrozenBodies(_ kv.Getter, fn func(blockNum, baseTxNum, txCount uint64) error) error {
	for _, b := range f.bodies {
		if err := fn(b.blockNum, b.baseTxNum, b.txCount); err != nil {
			return err
		}
	}
	return nil
}

func TestBlackListForPruning(t *testing.T) {
	c, ok := snapcfg.KnownCfg(networkname.Mainnet)
	if !ok {
		t.Fatal("no known cfg")
	}

	preverified := c.Preverified

	maxStep, err := getMaxStepRangeInSnapshots(preverified)
	if err != nil {
		t.Fatal(err)
	}
	// stepPrune is chosen well above the lowest state-history step range so
	// real state history files land in the blacklist. The previous test used
	// stepPrune=64 which left the blacklist empty (mainnet preverified state
	// history files start much higher), so it validated nothing about state
	// blacklisting. blockPrune sits above the 20M mark so tx-segment pruning
	// gets exercised too.
	const stepPrune = 5000
	const minBlockToDownload uint64 = 20_000_000
	const blockPrune uint64 = 25_000_000
	// effectiveCutoff mirrors the internal adjustBlockPrune clamp; without
	// it the assertion accepts segments above the cutoff the function actually used.
	effectiveCutoff := adjustBlockPrune(blockPrune, minBlockToDownload)
	blackList, err := buildBlackListForPruning(prune.MinimalMode, nil, stepPrune, 0, 0, minBlockToDownload, blockPrune, preverified)
	if err != nil {
		t.Fatal(err)
	}

	var sawStateHistory, sawTransactions bool
	for p := range blackList {
		info, _, ok := snaptype.ParseFileName("tmp", p)
		if !ok {
			continue
		}
		switch {
		case strings.Contains(p, "transactions"):
			sawTransactions = true
			if info.To > effectiveCutoff {
				t.Errorf("transaction segment %s should not have been blacklisted (To=%d > effectiveCutoff=%d)", p, info.To, effectiveCutoff)
			}
		case strings.Contains(p, "domain"):
			t.Errorf("domain segment %s should never be blacklisted", p)
		default:
			// State history file (idx/history/accessor).
			sawStateHistory = true
			if info.To > stepPrune {
				t.Errorf("state history %s should not have been blacklisted (To=%d > stepPrune=%d)", p, info.To, stepPrune)
			}
			if info.To == maxStep {
				t.Errorf("freshest state history %s should not have been blacklisted (To==maxStep)", p)
			}
		}
	}
	if !sawStateHistory {
		t.Error("expected at least one state history file to be blacklisted; got none — test no longer exercises the state-history path")
	}
	if !sawTransactions {
		t.Error("expected at least one transaction segment to be blacklisted; got none — test no longer exercises the tx-segment path")
	}
}

// TestBlackListForPruning_BlocksModeKeepsAllTransactions verifies that
// --prune.mode=blocks (Blocks=KeepAllBlocksPruneMode, History finite)
// blacklists state history but never transaction segments. Distance.Enabled()
// returns false for KeepAllBlocksPruneMode, which is the contract this test
// locks down — if .Enabled() ever stopped excluding that sentinel,
// tx segments would start getting pruned by accident.
func TestBlackListForPruning_BlocksModeKeepsAllTransactions(t *testing.T) {
	c, ok := snapcfg.KnownCfg(networkname.Mainnet)
	if !ok {
		t.Fatal("no known cfg")
	}
	preverified := c.Preverified

	// stepPrune is chosen well above the lowest state-history file step
	// range so at least some history files land in the blacklist; the exact
	// number depends on the bundled preverified set.
	const stepPrune = 5000
	blackList, err := buildBlackListForPruning(prune.BlocksMode, nil, stepPrune, 0, 0, 100_000, 0, preverified)
	if err != nil {
		t.Fatal(err)
	}

	sawHistory := false
	for p := range blackList {
		if strings.Contains(p, "transactions") {
			t.Errorf("blocks mode must not blacklist transaction segments, got %s", p)
		}
		if strings.HasPrefix(p, "idx") || strings.HasPrefix(p, "history") || strings.HasPrefix(p, "accessor") {
			sawHistory = true
		}
	}
	if !sawHistory {
		t.Error("expected state history files to be blacklisted in blocks mode; got none")
	}
}

// TestDownloadFilteringApplies covers the predicate that gates the slow
// getMinimumBlocksToDownload + buildBlackListForPruning call. Of particular
// interest is the {KeepPostMergeBlocksPruneMode, KeepPostMergeBlocksPruneMode} hybrid
// produced by `--prune.mode=archive --prune.distance.blocks=18446744073709551615`:
// neither field's Enabled() is true, but the operator opted into
// chain-history-expiry, so filtering must apply when MergeHeight is set.
func TestDownloadFilteringApplies(t *testing.T) {
	mergeHeight := uint64(15_537_394)
	ccMainnet := &chain.Config{MergeHeight: &mergeHeight}
	ccNoMerge := &chain.Config{}

	cases := []struct {
		name string
		mode prune.Mode
		cc   *chain.Config
		want bool
	}{
		{"archive on mainnet", prune.ArchiveMode, ccMainnet, false},
		{"archive on pre-merge chain", prune.ArchiveMode, ccNoMerge, false},
		{"full", prune.FullMode, ccMainnet, true},
		{"minimal", prune.MinimalMode, ccMainnet, true},
		{"blocks", prune.BlocksMode, ccMainnet, true},
		{
			name: "archive+blocks-override chain-history-expiry (mainnet)",
			mode: prune.Mode{Initialised: true, History: prune.KeepPostMergeBlocksPruneMode, Blocks: prune.KeepPostMergeBlocksPruneMode, CommitmentHistory: prune.KeepAllBlocksPruneMode},
			cc:   ccMainnet,
			want: true, // pre-merge tx must still be filtered
		},
		{
			name: "archive+blocks-override chain-history-expiry (no MergeHeight)",
			mode: prune.Mode{Initialised: true, History: prune.KeepPostMergeBlocksPruneMode, Blocks: prune.KeepPostMergeBlocksPruneMode, CommitmentHistory: prune.KeepAllBlocksPruneMode},
			cc:   ccNoMerge,
			want: false, // no MergeHeight → nothing to filter
		},
		{
			name: "legacy full {DefaultBlocks, Distance}",
			mode: prune.Mode{Initialised: true, History: prune.Distance(100_000), Blocks: prune.KeepPostMergeBlocksPruneMode, CommitmentHistory: prune.KeepAllBlocksPruneMode},
			cc:   ccMainnet,
			want: true,
		},
		{
			// Commitment-only config: History/Blocks unlimited but a bounded
			// commitment window. Filtering must still apply (regression guard).
			name: "commitment-history-only bounded",
			mode: prune.Mode{Initialised: true, History: prune.KeepAllBlocksPruneMode, Blocks: prune.KeepAllBlocksPruneMode, CommitmentHistory: prune.Distance(100_000)},
			cc:   ccMainnet,
			want: true,
		},
		{
			// Receipts-only config: History/Blocks unlimited but a bounded
			// receipts window. Filtering must still apply (regression guard).
			name: "receipts-only bounded",
			mode: prune.Mode{Initialised: true, History: prune.KeepAllBlocksPruneMode, Blocks: prune.KeepAllBlocksPruneMode, CommitmentHistory: prune.KeepAllBlocksPruneMode, Receipts: prune.Distance(100_000)},
			cc:   ccMainnet,
			want: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := downloadFilteringApplies(tc.mode, tc.cc); got != tc.want {
				t.Errorf("downloadFilteringApplies(%s) = %v, want %v", tc.mode.String(), got, tc.want)
			}
		})
	}
}

// TestBlackListForPruning_ChainHistoryExpiry covers the case absorbed from
// the former isTransactionsSegmentExpired: when Blocks=KeepPostMergeBlocksPruneMode
// and the chain has a MergeHeight, pre-merge transaction segments must be
// blacklisted at download time while post-merge segments stay downloadable.
func TestBlackListForPruning_ChainHistoryExpiry(t *testing.T) {
	c, ok := snapcfg.KnownCfg(networkname.Mainnet)
	if !ok {
		t.Fatal("no known cfg")
	}
	preverified := c.Preverified

	mergeHeight := uint64(15_537_394) // mainnet merge block
	cc := &chain.Config{MergeHeight: &mergeHeight}

	// Legacy full-mode shape: History finite, Blocks at the chain-history-expiry sentinel.
	legacyFull := prune.Mode{
		Initialised: true,
		History:     prune.Distance(100_000),
		Blocks:      prune.KeepPostMergeBlocksPruneMode,
	}

	blackList, err := buildBlackListForPruning(legacyFull, cc, 64, 0, 0, 100_000, 0, preverified)
	if err != nil {
		t.Fatal(err)
	}

	sawPreMergeTx := false
	for p := range blackList {
		if !strings.Contains(p, "transactions") {
			continue
		}
		info, _, ok := snaptype.ParseFileName("tmp", p)
		if !ok {
			continue
		}
		if info.From >= mergeHeight {
			t.Errorf("post-merge tx segment unexpectedly blacklisted: %s (From=%d)", p, info.From)
		} else {
			sawPreMergeTx = true
		}
	}
	if !sawPreMergeTx {
		t.Error("expected at least one pre-merge tx segment to be blacklisted; got none")
	}
}

// TestBuildBlackListForPruning_CommitmentHistory locks down the commitment
// history filter folded into buildBlackListForPruning: commitment-domain
// state-history files (idx/history/accessor) with To <= minCommitmentHistoryStep
// are blacklisted, while commitment domain files, non-commitment history, and
// transaction segments are left alone. Filtering runs even when History pruning
// is off (commitment-only config).
func TestBuildBlackListForPruning_CommitmentHistory(t *testing.T) {
	preverified := snapcfg.Preverified{Items: snapcfg.PreverifiedItems{
		{Name: "history/v1.0-commitment.0-16.v"},
		{Name: "idx/v1.0-commitment.0-16.ef"},
		{Name: "accessor/v1.0-commitment.0-16.vi"},
		{Name: "history/v1.0-commitment.16-32.v"},
		{Name: "history/v1.0-accounts.0-16.v"},
		{Name: "domain/v1.0-commitment.0-16.kv"},
		{Name: "v1.0-000000-000100-transactions.seg"},
	}}

	// Archive keeps History/Blocks unlimited; only the commitment-history filter
	// should fire (regression guard for commitment-only configs).
	const minCommitmentHistoryStep = 16
	blackList, err := buildBlackListForPruning(prune.ArchiveMode, nil, 0, minCommitmentHistoryStep, 0, 0, 0, preverified)
	if err != nil {
		t.Fatal(err)
	}

	want := map[string]bool{
		"history/v1.0-commitment.0-16.v":   true,
		"idx/v1.0-commitment.0-16.ef":      true,
		"accessor/v1.0-commitment.0-16.vi": true,
	}
	for name := range want {
		if _, ok := blackList[name]; !ok {
			t.Errorf("expected %s to be blacklisted (To <= minCommitmentHistoryStep=%d)", name, minCommitmentHistoryStep)
		}
	}
	for _, keep := range []string{
		"history/v1.0-commitment.16-32.v", // above the window
		"history/v1.0-accounts.0-16.v",    // not commitment domain
		"domain/v1.0-commitment.0-16.kv",  // domain files are never filtered
		"v1.0-000000-000100-transactions.seg",
	} {
		if _, ok := blackList[keep]; ok {
			t.Errorf("%s must not be blacklisted", keep)
		}
	}
}

// TestBuildBlackListForPruning_CommitmentHistoryDisabled verifies that a zero
// minCommitmentHistoryStep disables commitment-history filtering entirely.
func TestBuildBlackListForPruning_CommitmentHistoryDisabled(t *testing.T) {
	preverified := snapcfg.Preverified{Items: snapcfg.PreverifiedItems{
		{Name: "history/v1.0-commitment.0-16.v"},
	}}
	blackList, err := buildBlackListForPruning(prune.ArchiveMode, nil, 0, 0, 0, 0, 0, preverified)
	if err != nil {
		t.Fatal(err)
	}
	if len(blackList) != 0 {
		t.Errorf("commitment-history filtering must be disabled at minCommitmentHistoryStep=0, got %v", blackList)
	}
}

// TestBuildBlackListForPruning_Receipts locks down the receipt-cache filter
// folded into buildBlackListForPruning: rcache-domain state-history files
// (idx/history/accessor) with To <= minReceiptsStep are blacklisted, while the
// rcache domain file, non-rcache history, and transaction segments are left
// alone. Filtering runs even when History pruning is off (the archive +
// --prune.receipts.distance config).
func TestBuildBlackListForPruning_Receipts(t *testing.T) {
	preverified := snapcfg.Preverified{Items: snapcfg.PreverifiedItems{
		{Name: "history/v1.0-rcache.0-16.v"},
		{Name: "idx/v1.0-rcache.0-16.ef"},
		{Name: "accessor/v1.0-rcache.0-16.vi"},
		{Name: "history/v1.0-rcache.16-32.v"},
		{Name: "history/v1.0-accounts.0-16.v"},
		{Name: "domain/v1.0-rcache.0-16.kv"},
		{Name: "v1.0-000000-000100-transactions.seg"},
	}}

	const minReceiptsStep = 16
	blackList, err := buildBlackListForPruning(prune.ArchiveMode, nil, 0, 0, minReceiptsStep, 0, 0, preverified)
	if err != nil {
		t.Fatal(err)
	}

	want := []string{
		"history/v1.0-rcache.0-16.v",
		"idx/v1.0-rcache.0-16.ef",
		"accessor/v1.0-rcache.0-16.vi",
	}
	for _, name := range want {
		if _, ok := blackList[name]; !ok {
			t.Errorf("expected %s to be blacklisted (To <= minReceiptsStep=%d)", name, minReceiptsStep)
		}
	}
	for _, keep := range []string{
		"history/v1.0-rcache.16-32.v",  // above the window
		"history/v1.0-accounts.0-16.v", // not rcache domain
		"domain/v1.0-rcache.0-16.kv",   // domain files are never filtered
		"v1.0-000000-000100-transactions.seg",
	} {
		if _, ok := blackList[keep]; ok {
			t.Errorf("%s must not be blacklisted", keep)
		}
	}
}

// TestBuildBlackListForPruning_ReceiptsDisabled verifies that a zero
// minReceiptsStep disables receipt-cache filtering entirely.
func TestBuildBlackListForPruning_ReceiptsDisabled(t *testing.T) {
	preverified := snapcfg.Preverified{Items: snapcfg.PreverifiedItems{
		{Name: "history/v1.0-rcache.0-16.v"},
	}}
	blackList, err := buildBlackListForPruning(prune.ArchiveMode, nil, 0, 0, 0, 0, 0, preverified)
	if err != nil {
		t.Fatal(err)
	}
	if len(blackList) != 0 {
		t.Errorf("receipt-cache filtering must be disabled at minReceiptsStep=0, got %v", blackList)
	}
}

// TestGetMinimumBlocksToDownload_ThreeCutoffs verifies the single pass resolves the
// history, commitment-history and receipts cutoffs to their respective steps
// independently.
func TestGetMinimumBlocksToDownload_ThreeCutoffs(t *testing.T) {
	const stepSize = 100
	// blockNum -> baseTxNum. step(baseTxNum) = floor((baseTxNum-(stepSize-1))/stepSize).
	br := &fakeBlockReader{
		frozenMax: 1000,
		bodies: []frozenBody{
			{blockNum: 100, baseTxNum: 10_000},
			{blockNum: 200, baseTxNum: 20_000},
			{blockNum: 300, baseTxNum: 30_000},
		},
	}
	// maxStateStep=150 → stateTxNum=15_000. Only block 100 (baseTxNum 10_000) is
	// below the cutoff, so minToDownload=1000-100=900 and minBlock=1000-900=100.
	tx := beginTestRoTx(t)
	minBlock, historyStep, commitmentStep, receiptsStep, err := getMinimumBlocksToDownload(context.Background(), br, tx, 150, stepSize, 100, 300, 200)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, uint64(100), minBlock)
	// step(10_000) = (10_000-99)/100 = 99 ; step(30_000) = (30_000-99)/100 = 299 ;
	// step(20_000) = (20_000-99)/100 = 199.
	assert.Equal(t, kv.Step(99), historyStep)
	assert.Equal(t, kv.Step(299), commitmentStep)
	assert.Equal(t, kv.Step(199), receiptsStep)
}

// TestGetMinimumBlocksToDownload_MinBlock pins the minBlockToDownload computation and
// the per-cutoff step against a smaller frozen range.
func TestGetMinimumBlocksToDownload_MinBlock(t *testing.T) {
	const stepSize = 100
	br := &fakeBlockReader{
		frozenMax: 300,
		bodies: []frozenBody{
			{blockNum: 100, baseTxNum: 10_000},
			{blockNum: 200, baseTxNum: 20_000},
			{blockNum: 300, baseTxNum: 30_000},
		},
	}
	// maxStateStep=150 → stateTxNum=15_000. Only block 100 (baseTxNum 10_000) is
	// below the cutoff, so minToDownload=300-100=200 and minBlock=300-200=100.
	tx := beginTestRoTx(t)
	minBlock, historyStep, commitmentStep, receiptsStep, err := getMinimumBlocksToDownload(context.Background(), br, tx, 150, stepSize, 200, 200, 100)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, uint64(100), minBlock)
	// step(20_000) = (20_000-99)/100 = 199 ; step(10_000) = (10_000-99)/100 = 99.
	assert.Equal(t, kv.Step(199), historyStep)
	assert.Equal(t, kv.Step(199), commitmentStep)
	assert.Equal(t, kv.Step(99), receiptsStep)
}

// TestGetMinimumBlocksToDownload_CutoffBelowFrozenBodies pins sentinel
// normalization: when a prune-to boundary block is not visited during the
// frozen-body scan (it falls below the first frozen body), the corresponding
// step must resolve to 0 — disabling that filter so nothing is blacklisted —
// rather than staying at the MaxUint32 sentinel, which would blacklist every
// matching history file and skip downloading data the node needs.
func TestGetMinimumBlocksToDownload_CutoffBelowFrozenBodies(t *testing.T) {
	const stepSize = 100
	br := &fakeBlockReader{
		frozenMax: 1000,
		bodies: []frozenBody{
			{blockNum: 500, baseTxNum: 50_000},
			{blockNum: 600, baseTxNum: 60_000},
			{blockNum: 700, baseTxNum: 70_000},
		},
	}
	tx := beginTestRoTx(t)
	// All three prune-to boundaries (50/60/70) sit below the first frozen body
	// (500), so none is hit during iteration and each step stays unset.
	_, historyStep, commitmentStep, receiptsStep, err := getMinimumBlocksToDownload(context.Background(), br, tx, 600, stepSize, 50, 60, 70)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, kv.Step(0), historyStep)
	assert.Equal(t, kv.Step(0), commitmentStep)
	assert.Equal(t, kv.Step(0), receiptsStep)
}
