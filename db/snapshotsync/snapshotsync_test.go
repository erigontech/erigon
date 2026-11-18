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
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/gointerfaces/downloaderproto"
)

func beginTestRoTx(t *testing.T) kv.Tx {
	t.Helper()
	tx, err := mdbxtest.NewTestDB(t, dbcfg.ChainDB).BeginRo(context.Background())
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
	dbservices.BlockSnapshots
	max uint64
}

func (f fakeSnapshots) SegmentsMax() uint64 { return f.max }

// fakeBlockReader stubs the BlockReader interface for getMinimumBlocksToDownload,
// implementing only the methods that function touches.
type fakeBlockReader struct {
	BlockReader
	frozenMax uint64
	bodies    []frozenBody
}

func (f *fakeBlockReader) Snapshots() dbservices.BlockSnapshots {
	return fakeSnapshots{max: f.frozenMax}
}

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
		if info.To > mergeHeight {
			t.Errorf("merge-spanning or post-merge tx segment unexpectedly blacklisted: %s (To=%d)", p, info.To)
		} else {
			sawPreMergeTx = true
		}
	}
	if !sawPreMergeTx {
		t.Error("expected at least one pre-merge tx segment to be blacklisted; got none")
	}
}

func TestLegacyCommitmentFiles(t *testing.T) {
	items := snapcfg.PreverifiedItems{
		{Name: "domain/v2.2-commitment.0-16.kv"},
		{Name: "domain/v2.0-commitment.0-16.kvi"},
		{Name: "domain/v2.2-commitment.16-32.kv"},
		{Name: "domain/v3.0-commitment.16-32.kv"},
		{Name: "domain/v2.0-commitment.16-32.bt"},
		{Name: "domain/v3.0-commitment.32-48.kv"},
		{Name: "domain/v2.0-commitment.32-48.bt"},
		{Name: "history/v2.0-commitment.0-16.v"},
		{Name: "idx/v2.0-commitment.0-16.ef"},
		{Name: "accessor/v2.0-commitment.0-16.vi"},
		{Name: "history/v2.0-commitment.16-32.v"},
		{Name: "history/v3.0-commitment.16-32.v"},
		{Name: "idx/v2.0-commitment.16-32.ef"},
		{Name: "history/v3.0-commitment.32-48.v"},
		{Name: "idx/v2.0-commitment.32-48.ef"},
		{Name: "idx/v2.0-commitment.48-64.ef"},
		{Name: "domain/v2.0-accounts.0-16.kv"},
		{Name: "domain/v2.0-accounts.0-16.bt"},
		{Name: "domain/v2.0-storage.0-16.kv"},
		{Name: "domain/v2.0-storage.0-16.kvei"},
		{Name: "domain/v2.0-code.0-16.kv"},
		{Name: "domain/v2.0-accounts.16-32.kv"},
		{Name: "history/v2.0-accounts.0-16.v"},
		{Name: "idx/v2.0-storage.0-16.ef"},
		{Name: "accessor/v2.0-accounts.0-16.vi"},
		{Name: "v1.0-000000-000100-transactions.seg"},
	}
	for _, v3 := range []bool{true, false} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("v3=%t/reverse=%t", v3, reverse), func(t *testing.T) {
				t.Cleanup(statecfg.SetCommitmentV3(v3))
				ordered := slices.Clone(items)
				if reverse {
					slices.Reverse(ordered)
				}
				want := map[string]bool{}
				var names []string
				if v3 {
					names = []string{"domain/v2.2-commitment.0-16.kv", "domain/v2.0-commitment.0-16.kvi", "domain/v2.2-commitment.16-32.kv", "history/v2.0-commitment.0-16.v", "idx/v2.0-commitment.0-16.ef", "accessor/v2.0-commitment.0-16.vi", "history/v2.0-commitment.16-32.v", "idx/v2.0-commitment.48-64.ef", "domain/v2.0-accounts.0-16.kv", "domain/v2.0-accounts.0-16.bt", "domain/v2.0-storage.0-16.kv", "domain/v2.0-storage.0-16.kvei"}
				} else {
					names = []string{"domain/v3.0-commitment.16-32.kv", "domain/v3.0-commitment.32-48.kv", "domain/v2.0-commitment.32-48.bt", "history/v3.0-commitment.16-32.v", "history/v3.0-commitment.32-48.v", "idx/v2.0-commitment.32-48.ef"}
				}
				for _, name := range names {
					want[name] = false
				}
				if v3 {
					want["domain/v2.2-commitment.0-16.kv"] = true
					want["domain/v2.0-accounts.0-16.kv"] = true
					want["domain/v2.0-accounts.0-16.bt"] = true
					want["domain/v2.0-storage.0-16.kv"] = true
					want["domain/v2.0-storage.0-16.kvei"] = true
				}
				require.Equal(t, want, incompatibleCommitmentFiles(ordered, 0))
			})
		}
	}
}

func TestCommitmentFilterKeepsARealManifestWithV3Off(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	cfg, ok := snapcfg.KnownCfg(networkname.Mainnet)
	require.True(t, ok)
	require.True(t, slices.ContainsFunc(cfg.Preverified.Items, func(p snapcfg.PreverifiedItem) bool {
		return strings.Contains(p.Name, "commitment.")
	}))
	require.Empty(t, incompatibleCommitmentFiles(cfg.Preverified.Items, 0))
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

// The state files end inside a block, so execution resumes there and replays the block's prefix
// from history. The history cutoff must keep the step that the block starts in.
func TestGetMinimumBlocksToDownload_KeepsHistoryOfTheResumedBlock(t *testing.T) {
	const stepSize = 100
	for _, tc := range []struct {
		name            string
		baseTxNum       uint64
		txCount         uint64
		historyPruneTo  uint64
		wantHistoryStep kv.Step
	}{
		{"state ends inside a block that starts a step earlier", 14_850, 200, 300, 147},
		{"state ends between blocks", 14_950, 50, 300, 299},
		{"cutoff below the resumed block", 14_850, 200, 100, 99},
		{"cutoff below the frozen bodies", 14_850, 200, 50, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			br := &fakeBlockReader{
				frozenMax: 1000,
				bodies: []frozenBody{
					{blockNum: 100, baseTxNum: 10_000, txCount: 2},
					{blockNum: 150, baseTxNum: tc.baseTxNum, txCount: tc.txCount},
					{blockNum: 151, baseTxNum: tc.baseTxNum + tc.txCount, txCount: 2},
					{blockNum: 300, baseTxNum: 30_000, txCount: 2},
				},
			}
			// maxStateStep=150: the state files hold txNums below 15_000. The history cutoff is at
			// step 299 for block 300 and at step 99 for block 100; block 50 is not frozen, so no
			// history is skipped.
			_, historyStep, _, _, err := getMinimumBlocksToDownload(context.Background(), br, beginTestRoTx(t), 150, stepSize, tc.historyPruneTo, 300, 300)
			require.NoError(t, err)
			assert.Equal(t, tc.wantHistoryStep, historyStep)
		})
	}
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

// TestReceiptsSegmentRetentionCutoff: when blocks are kept but state history is
// pruned, both rcache history and the log indexes must follow state history, so
// download agrees with retirement instead of fetching files it then deletes.
func TestReceiptsSegmentRetentionCutoff(t *testing.T) {
	const head = 1_000_000
	rcacheSeg := "history/v1.0-" + kv.RCacheDomain.String() + ".0-64.v"
	logIdxSeg := "idx/v1.0-" + kv.LogAddrIdx.String() + ".0-64.ef"

	blocksHistory := historyRetentionCutoff(prune.BlocksMode, head)
	assert.NotZero(t, blocksHistory, "blocks-mode history window must be finite")
	assert.Equal(t, uint64(0), blocksRetentionCutoff(prune.BlocksMode, nil, head), "blocks-mode blocks window is keep-all")
	assert.Equal(t, blocksHistory, receiptsSegmentRetentionCutoff(prune.BlocksMode, nil, head, rcacheSeg))
	assert.Equal(t, blocksHistory, receiptsSegmentRetentionCutoff(prune.BlocksMode, nil, head, logIdxSeg),
		"a blocks-mode node must not download log indexes that execution retirement then deletes")

	assert.Equal(t, blocksRetentionCutoff(prune.MinimalMode, nil, head), receiptsSegmentRetentionCutoff(prune.MinimalMode, nil, head, rcacheSeg))
}

// TestIsReceiptsSegmentPruned verifies segments are skipped at download time
// only when they lie entirely below the prune height: the segment containing
// the cutoff step must still be downloaded, otherwise blocks inside the
// retention window would have receipts but no log index.
func TestIsReceiptsSegmentPruned(t *testing.T) {
	const stepSize = 100
	const pruneHeight = 50 // minTxNum(50)=5000 → cutoff step 50

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for b := uint64(0); b <= 60; b++ {
		if err := rawdbv3.TxNums.Append(tx, b, (b+1)*stepSize-1); err != nil {
			t.Fatal(err)
		}
	}

	logIdx := func(from, to int) string {
		return fmt.Sprintf("idx/v1.0-%s.%d-%d.ef", kv.LogTopicIdx.String(), from, to)
	}
	cases := []struct {
		name   string
		seg    string
		pruned bool
	}{
		{"segment spanning the cutoff is kept", logIdx(32, 64), false},
		{"segment entirely below the cutoff is pruned", logIdx(16, 32), true},
		{"segment ending exactly at the cutoff is pruned", logIdx(48, 50), true},
		{"segment above the cutoff is kept", logIdx(64, 96), false},
		{"domain segment is never pruned", "domain/v1.0-rcache.16-32.kv", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := snapcfg.PreverifiedItem{Name: tc.seg}
			assert.Equal(t, tc.pruned, isReceiptsSegmentPruned(context.Background(), tx, rawdbv3.TxNums, p, stepSize, pruneHeight))
		})
	}

	t.Run("pruneHeight=0 keeps everything", func(t *testing.T) {
		p := snapcfg.PreverifiedItem{Name: logIdx(0, 16)}
		assert.False(t, isReceiptsSegmentPruned(context.Background(), tx, rawdbv3.TxNums, p, stepSize, 0))
	})
}

func TestGetMaxStepRangeInSnapshots(t *testing.T) {
	t.Run("valid domain entries", func(t *testing.T) {
		preverified := snapcfg.Preverified{
			Items: []snapcfg.PreverifiedItem{
				{Name: "domain/v1.0-accounts.0-10.kv"},
				{Name: "domain/v1.0-accounts.10-50.kv"},
				{Name: "domain/v1.0-storage.0-20.kv"},
			},
		}
		maxTo, err := getMaxStepRangeInSnapshots(preverified)
		assert.NoError(t, err)
		assert.Equal(t, uint64(50), maxTo)
	})

	t.Run("malformed domain entry fails fast", func(t *testing.T) {
		preverified := snapcfg.Preverified{
			Items: []snapcfg.PreverifiedItem{
				{Name: "domain/v1.0-accounts.0-10.kv"},
				{Name: "domain/malformed-entry"},
			},
		}
		_, err := getMaxStepRangeInSnapshots(preverified)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid domain snapshot filename")
	})
}

// KeepAllBlocksPruneMode means follow-history for receipts, not keep-all.
func TestReceiptsSegmentRetentionCutoff_FollowsHistoryWindow(t *testing.T) {
	t.Parallel()
	const head = 23_000_000
	const rcacheName = "history/v1.0-rcache.100-200.v"

	for _, tc := range []struct {
		name string
		mode prune.Mode
		want uint64
	}{
		{"minimal keeps only its history window", prune.MinimalMode, head - config3.MinimalPruneDistance},
		{"full keeps only its history window", prune.FullMode, head - config3.DefaultPruneDistance},
		{"blocks keeps only its history window", prune.BlocksMode, head - config3.DefaultPruneDistance},
		{"archive keeps everything", prune.ArchiveMode, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.True(t, tc.mode.ReceiptsFollowHistory(),
				"presets leave Receipts unset, which for receipts means follow-history")
			require.Equal(t, tc.want, receiptsSegmentRetentionCutoff(tc.mode, nil, head, rcacheName))
		})
	}

	require.Equal(t, uint64(head-config3.MinimalPruneDistance),
		receiptsSegmentRetentionCutoff(prune.MinimalMode, nil, head, "idx/v1.0-logaddrs.100-200.ef"))
	require.Zero(t, receiptsSegmentRetentionCutoff(prune.ArchiveMode, nil, head, "idx/v1.0-logaddrs.100-200.ef"))
}

type requestListBlockReader struct {
	fakeBlockReader
	freezing ethconfig.BlocksFreezing
}

func (r *requestListBlockReader) FreezingCfg() ethconfig.BlocksFreezing {
	return r.freezing
}

func (*requestListBlockReader) TxnumReader() rawdbv3.TxNumsReader { return rawdbv3.TxNumsReader{} }

type requestListDownloader struct {
	dbservices.DownloaderClient
	names []string
}

func (d *requestListDownloader) Download(_ context.Context, req *downloaderproto.DownloadRequest) error {
	for _, item := range req.Items {
		d.names = append(d.names, item.Path)
	}
	return nil
}

func TestSyncSnapshotsCommitmentRequestList(t *testing.T) {
	files := []struct {
		name       string
		v3, legacy bool
	}{
		{"domain/v2.2-commitment.0-16.kv", false, true},
		{"domain/v3.0-commitment.0-16.kv", true, false},
		{"domain/v2.0-commitment.0-16.bt", true, true},
		{"domain/v2.0-accounts.0-16.kv", true, true},
		{"domain/v2.0-accounts.0-16.bt", true, true},
		{"domain/v2.0-storage.0-16.kv", true, true},
		{"domain/v2.0-code.0-16.kv", true, true},
		{"domain/v2.2-commitment.16-32.kv", false, true},
		{"domain/v3.0-commitment.16-32.kv", true, false},
		{"domain/v2.0-commitment.16-32.bt", true, true},
		{"domain/v2.0-accounts.16-32.kv", true, true},
		{"domain/v3.0-commitment.32-48.kv", true, false},
		{"domain/v2.0-commitment.32-48.bt", true, false},
		{"history/v2.0-commitment.0-16.v", false, true},
		{"idx/v2.0-commitment.0-16.ef", false, true},
		{"history/v3.0-commitment.32-48.v", true, false},
		{"idx/v2.0-commitment.32-48.ef", true, false},
		{"history/v2.0-accounts.0-16.v", true, true},
		{"accessor/v2.0-accounts.0-16.vi", true, true},
	}
	cfg, ok := snapcfg.KnownCfg(networkname.Mainnet)
	require.True(t, ok)
	previous := *cfg
	t.Cleanup(func() { *cfg = previous })
	cfg.Local = false
	cfg.Preverified.Items = nil
	for _, file := range files {
		cfg.Preverified.Items = append(cfg.Preverified.Items, snapcfg.PreverifiedItem{Name: file.name, Hash: strings.Repeat("0", 40)})
	}
	for _, v3 := range []bool{false, true} {
		t.Run(fmt.Sprintf("v3=%v", v3), func(t *testing.T) {
			t.Cleanup(statecfg.SetCommitmentV3(v3))
			dirs := datadir.New(t.TempDir())
			downloader := &requestListDownloader{}
			err := SyncSnapshots(t.Context(), "test", "test", false, false, false, BlobRetention{}, prune.ArchiveMode, NoCaplin, nil, &requestListBlockReader{}, &chain.Config{ChainName: networkname.Mainnet}, downloader, ethconfig.Sync{KeepExecutionProofs: true}, 16, dirs)
			require.NoError(t, err)
			var want []string
			for _, file := range files {
				if v3 && file.v3 || !v3 && file.legacy {
					want = append(want, file.name)
				}
			}
			require.ElementsMatch(t, want, downloader.names)
		})
	}
}

func TestSyncSnapshotsCommitmentV3StateTip(t *testing.T) {
	legacyFiles := []string{
		"domain/v2.2-commitment.0-16.kv",
		"domain/v2.0-commitment.0-16.bt",
		"domain/v2.0-accounts.0-16.kv",
		"domain/v2.0-storage.0-16.kv",
		"domain/v2.0-code.0-16.kv",
		"domain/v2.2-commitment.16-32.kv",
		"domain/v2.0-commitment.16-32.bt",
		"domain/v2.0-accounts.16-32.kv",
		"domain/v2.0-storage.16-32.kv",
		"domain/v2.0-code.16-32.kv",
	}
	cfg, ok := snapcfg.KnownCfg(networkname.Mainnet)
	require.True(t, ok)
	previous := *cfg
	t.Cleanup(func() { *cfg = previous })
	cfg.Local = false
	for _, tc := range []struct {
		name       string
		v3         bool
		disableE3  bool
		extraFiles []string
		localFiles []string
		wantFile   string
	}{
		{name: "v3 on legacy only", v3: true, wantFile: ".0-16."},
		{name: "v3 on legacy only with state downloads disabled", v3: true, disableE3: true},
		{name: "v3 below state tip", v3: true, extraFiles: []string{"domain/v3.0-commitment.0-16.kv"}, wantFile: ".16-32."},
		{name: "v3 above a legacy-only range", v3: true, extraFiles: []string{"domain/v3.0-commitment.16-32.kv"}, wantFile: ".0-16."},
		{name: "v3 commitment split across one accounts range", v3: true, extraFiles: []string{"domain/v3.0-commitment.0-16.kv", "domain/v3.0-commitment.16-32.kv", "domain/v2.0-accounts.0-32.kv"}, wantFile: ".0-32."},
		{name: "v3 legacy commitment at the code tip", v3: true, extraFiles: []string{"domain/v3.0-commitment.0-16.kv", "domain/v3.0-commitment.16-32.kv", "domain/v2.2-commitment.32-48.kv", "domain/v2.0-code.32-48.kv"}, wantFile: ".32-48."},
		{name: "v3 code above every commitment", v3: true, extraFiles: []string{"domain/v3.0-commitment.0-16.kv", "domain/v3.0-commitment.16-32.kv", "domain/v2.0-code.32-48.kv"}, wantFile: "steps 32-48"},
		{name: "v3 on a converted datadir", v3: true, localFiles: []string{"v3.0-commitment.0-16.kv", "v3.0-commitment.16-32.kv"}},
		{name: "v3 on a datadir converted below the state tip", v3: true, localFiles: []string{"v3.0-commitment.0-16.kv"}, wantFile: ".16-32."},
		{name: "v3 code above a converted datadir", v3: true, localFiles: []string{"v3.0-commitment.0-16.kv", "v3.0-commitment.16-32.kv"}, extraFiles: []string{"domain/v2.0-code.32-48.kv"}, wantFile: "steps 32-48"},
		{name: "v3 on a datadir converted only above step 0", v3: true, localFiles: []string{"v3.0-commitment.16-32.kv"}, wantFile: ".0-16."},
		{name: "v3 on a datadir with a gap in its converted commitment", v3: true, localFiles: []string{"v3.0-commitment.0-16.kv", "v3.0-commitment.32-48.kv"}, wantFile: ".16-32."},
		{name: "v3 off legacy only"},
		{name: "v3 off with a v3-only range", extraFiles: []string{"domain/v3.0-commitment.32-48.kv", "domain/v2.0-accounts.32-48.kv"}, wantFile: ".32-48."},
		{name: "v3 off with a v3-only range below a legacy range", extraFiles: []string{"domain/v3.0-commitment.32-48.kv", "domain/v2.0-accounts.32-48.kv", "domain/v2.2-commitment.48-64.kv", "domain/v2.0-accounts.48-64.kv"}, wantFile: ".32-48."},
		{name: "v3 off with v3 commitment split across one accounts range", extraFiles: []string{"domain/v3.0-commitment.32-48.kv", "domain/v3.0-commitment.48-64.kv", "domain/v2.0-accounts.32-64.kv"}, wantFile: ".32-64."},
		{name: "v3 off with v3 commitment split across one accounts range below a legacy range", extraFiles: []string{"domain/v3.0-commitment.32-48.kv", "domain/v3.0-commitment.48-64.kv", "domain/v2.0-accounts.32-64.kv", "domain/v2.2-commitment.64-80.kv", "domain/v2.0-accounts.64-80.kv"}, wantFile: ".32-64."},
		{name: "v3 off with code above every legacy commitment", extraFiles: []string{"domain/v3.0-commitment.32-48.kv", "domain/v2.0-code.32-48.kv"}, wantFile: "steps 32-48"},
		{name: "v3 off with a commitment index past every history file", extraFiles: []string{"idx/v2.0-commitment.32-48.ef"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Cleanup(statecfg.SetCommitmentV3(tc.v3))
			cfg.Preverified.Items = nil
			for _, name := range legacyFiles {
				cfg.Preverified.Items = append(cfg.Preverified.Items, snapcfg.PreverifiedItem{Name: name, Hash: strings.Repeat("0", 40)})
			}
			for _, name := range tc.extraFiles {
				cfg.Preverified.Items = append(cfg.Preverified.Items, snapcfg.PreverifiedItem{Name: name, Hash: strings.Repeat("0", 40)})
			}
			dirs := datadir.New(t.TempDir())
			for _, name := range tc.localFiles {
				require.NoError(t, os.WriteFile(filepath.Join(dirs.SnapDomain, name), nil, 0o644))
			}
			downloader := &requestListDownloader{}
			err := SyncSnapshots(t.Context(), "test", "test", false, false, false, BlobRetention{}, prune.ArchiveMode, NoCaplin, nil, &requestListBlockReader{freezing: ethconfig.BlocksFreezing{DisableDownloadE3: tc.disableE3}}, &chain.Config{ChainName: networkname.Mainnet}, downloader, ethconfig.Sync{KeepExecutionProofs: true}, 16, dirs)
			if tc.disableE3 {
				require.NoError(t, err)
				for _, name := range downloader.names {
					require.False(t, strings.HasPrefix(name, "domain/"), name)
				}
				return
			}
			if tc.wantFile != "" {
				remedy := "commitment convert --v3"
				if len(tc.localFiles) > 0 {
					remedy = "--snap.skip-state-snapshot-download"
				}
				if !tc.v3 {
					remedy = "run with `--experimental.commitment-v3`"
				}
				require.ErrorContains(t, err, remedy)
				require.ErrorContains(t, err, tc.wantFile)
				require.Empty(t, downloader.names)
				return
			}
			require.NoError(t, err)
			want := append(slices.Clone(legacyFiles), tc.extraFiles...)
			if tc.v3 {
				want = slices.DeleteFunc(want, func(name string) bool {
					return strings.Contains(name, kv.CommitmentDomain.String())
				})
			}
			require.ElementsMatch(t, want, downloader.names)
		})
	}
}

func TestBlobSidecarDownloadFilter(t *testing.T) {
	seg := "v1.1-008620-008630-blobsidecars.seg"
	idx := "v1.1-008620-008630-blocksidecars.idx"
	require.True(t, isBlobSidecarFile(seg))
	require.True(t, isBlobSidecarFile(idx), "the blob index is named blocksidecars and must follow its segment")
	require.False(t, isBlobSidecarFile("v1.1-008620-008630-beaconblocks.seg"))
	require.False(t, isBlobSidecarFile("v1.1-000000-000500-bodies.seg"))

	for _, name := range []string{seg, idx} {
		require.False(t, blobSegmentBelow(name, 0), "zero keeps every segment")
		require.True(t, blobSegmentBelow(name, 8_630_000), "a segment ending at the start slot holds nothing kept")
		require.False(t, blobSegmentBelow(name, 8_629_999), "the segment holding the start slot is kept")
		require.False(t, blobSegmentBelow(name, 8_000_000))
	}
}

func TestBlobRetentionDownloadFloorKeepsTheServingWindow(t *testing.T) {
	const window = 131_072 + 32
	items := snapcfg.PreverifiedItems{
		{Name: "v1.1-000000-000500-bodies.seg"},
		{Name: "v1.1-008620-008630-blobsidecars.seg"},
		{Name: "v1.1-008620-008630-blocksidecars.idx"},
		{Name: "v1.1-013560-013570-blobsidecars.seg"},
		{Name: "v1.1-013560-013570-blocksidecars.idx"},
	}
	const tip = 13_570_000
	for _, tc := range []struct {
		name string
		r    BlobRetention
		want uint64
	}{
		{name: "no bound downloads everything", r: BlobRetention{ServeWindowSlots: window}, want: 0},
		{name: "old start slot is the floor", r: BlobRetention{FromSlot: 9_000_000, ServeWindowSlots: window}, want: 9_000_000},
		{name: "start slot inside the serving window keeps the window", r: BlobRetention{FromSlot: tip - 10, ServeWindowSlots: window}, want: tip - window},
		{name: "unknown window downloads everything", r: BlobRetention{FromSlot: 9_000_000, ServeWindowSlots: math.MaxUint64}, want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.r.SnapshotDir = t.TempDir()
			floor, err := tc.r.downloadFloor(items)
			require.NoError(t, err)
			require.Equal(t, uint64(tc.want), floor)
		})
	}
}

func TestBlobRetentionDownloadFloorKeepsLocalSegmentsContiguous(t *testing.T) {
	const tip = 13_570_000
	const window = 131_072 + 32
	items := snapcfg.PreverifiedItems{}
	for from := 8_990_000; from < tip; from += 10_000 {
		items = append(items,
			snapcfg.PreverifiedItem{Name: fmt.Sprintf("v1.1-%06d-%06d-blobsidecars.seg", from/1000, (from+10_000)/1000)},
			snapcfg.PreverifiedItem{Name: fmt.Sprintf("v1.1-%06d-%06d-blocksidecars.idx", from/1000, (from+10_000)/1000)},
		)
	}
	r := BlobRetention{SnapshotDir: t.TempDir(), FromSlot: tip, ServeWindowSlots: window}
	floor, err := r.downloadFloor(items)
	require.NoError(t, err)
	require.Equal(t, uint64(tip-window), floor)

	for _, name := range []string{
		"v1.1-009020-009030-blobsidecars.seg",
		"v1.1-009000-009010-blobsidecars.seg",
		"v1.1-009000-009010-blocksidecars.idx",
		"v1.1-008000-008010-beaconblocks.seg",
		"v1.1-008500-008510-blocksidecars.idx",
	} {
		require.NoError(t, os.WriteFile(filepath.Join(r.SnapshotDir, name), []byte("segment"), 0o644))
	}
	floor, err = r.downloadFloor(items)
	require.NoError(t, err)
	for _, p := range items {
		require.Equal(t, strings.Contains(p.Name, "008990-009000"), skipBlobDownload(p.Name, true, floor),
			"only segments and indexes before the local prefix may be skipped: %s", p.Name)
	}
	require.Equal(t, uint64(9_000_000), floor)
}

func TestSkipBlobDownload(t *testing.T) {
	for _, name := range []string{"v1.1-008620-008630-blobsidecars.seg", "v1.1-008620-008630-blocksidecars.idx"} {
		require.True(t, skipBlobDownload(name, false, 0), "a node without a blob archive downloads no blob file: %s", name)
		require.False(t, skipBlobDownload(name, true, 0), name)
		require.True(t, skipBlobDownload(name, true, 8_630_000), name)
		require.False(t, skipBlobDownload(name, true, 8_629_999), name)
	}
	for _, name := range []string{"v1.1-008620-008630-beaconblocks.seg", "v1.1-000000-000500-bodies.seg"} {
		require.False(t, skipBlobDownload(name, false, 8_630_000), name)
	}
}
