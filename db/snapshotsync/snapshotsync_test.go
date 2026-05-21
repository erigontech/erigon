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
	"strings"
	"testing"

	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/networkname"
)

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
	blackList, err := buildBlackListForPruning(prune.MinimalMode, nil, stepPrune, minBlockToDownload, blockPrune, preverified)
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
			if info.To > blockPrune {
				t.Errorf("transaction segment %s should not have been blacklisted (To=%d > blockPrune=%d)", p, info.To, blockPrune)
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
	blackList, err := buildBlackListForPruning(prune.BlocksMode, nil, stepPrune, 100_000, 0, preverified)
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
// interest is the {DefaultBlocksPruneMode, DefaultBlocksPruneMode} hybrid
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
			mode: prune.Mode{Initialised: true, History: prune.DefaultBlocksPruneMode, Blocks: prune.DefaultBlocksPruneMode},
			cc:   ccMainnet,
			want: true, // pre-merge tx must still be filtered
		},
		{
			name: "archive+blocks-override chain-history-expiry (no MergeHeight)",
			mode: prune.Mode{Initialised: true, History: prune.DefaultBlocksPruneMode, Blocks: prune.DefaultBlocksPruneMode},
			cc:   ccNoMerge,
			want: false, // no MergeHeight → nothing to filter
		},
		{
			name: "legacy full {DefaultBlocks, Distance}",
			mode: prune.Mode{Initialised: true, History: prune.Distance(100_000), Blocks: prune.DefaultBlocksPruneMode},
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
// the former isTransactionsSegmentExpired: when Blocks=DefaultBlocksPruneMode
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
		Blocks:      prune.DefaultBlocksPruneMode,
	}

	blackList, err := buildBlackListForPruning(legacyFull, cc, 64, 100_000, 0, preverified)
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
