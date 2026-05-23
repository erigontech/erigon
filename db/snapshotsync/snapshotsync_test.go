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

	"github.com/stretchr/testify/assert"

	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snaptype"
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
	// Prune 64 steps and contain at least all the blocks
	blackList, err := buildBlackListForPruning(true, 64, 100_000, 25_000_000, preverified)
	if err != nil {
		t.Fatal(err)
	}
	for p := range blackList {
		// take the snapshot file name and parse it to get the "from"
		info, _, ok := snaptype.ParseFileName("tmp", p)
		if !ok {
			continue
		}
		if strings.Contains(p, "transactions") {
			if info.From < 19_000_000 {
				t.Errorf("Should have pruned %s", p)
			}
			continue
		}
		if strings.Contains(p, "domain") {
			t.Errorf("Should not have pruned %s", p)
		}
		if info.To == maxStep {
			t.Errorf("Should not have pruned %s", p)
		}
	}

}

func TestCommitmentHistoryMinStep(t *testing.T) {
	cases := []struct {
		name          string
		maxStateStep  uint64
		distanceSteps uint64
		want          uint64
	}{
		{"unlimited-distance-disables-filter", 100, 0, 0},
		{"distance-larger-than-maxstep-disables-filter", 5, 10, 0},
		{"distance-equal-to-maxstep-disables-filter", 10, 10, 0},
		{"normal-case", 100, 8, 92},
		{"recent", 100, 1, 99},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, commitmentHistoryMinStep(tc.maxStateStep, tc.distanceSteps))
		})
	}
}

func TestBlocksToStepDistance(t *testing.T) {
	cases := []struct {
		name         string
		olderBlocks  uint64
		maxStateStep uint64
		maxBlock     uint64
		want         uint64
	}{
		{"zero-blocks-disables", 0, 100, 1_000_000, 0},
		{"zero-maxStep-disables", 50_000, 0, 1_000_000, 0},
		{"zero-maxBlock-disables", 50_000, 100, 0, 0},
		// 100k blocks * 100 steps / 1M blocks = 10 steps
		{"mainnet-like-100k-blocks-1M-chain", 100_000, 100, 1_000_000, 10},
		// 100k blocks * 5 steps / 50k blocks = 10 steps (chain too young, but math is well-defined)
		{"young-chain-blocks-exceed-maxBlock", 100_000, 5, 50_000, 10},
		// 50k * 100 / 1M = 5 steps
		{"half-window", 50_000, 100, 1_000_000, 5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, blocksToStepDistance(tc.olderBlocks, tc.maxStateStep, tc.maxBlock))
		})
	}
}

func TestShouldSkipCommitmentHistorySegment(t *testing.T) {
	cases := []struct {
		name    string
		file    string
		minStep uint64
		skip    bool
	}{
		// Filtering disabled.
		{"min-step-zero-keeps-everything", "history/v1.0-commitment.0-16.v", 0, false},

		// Non-commitment files are never filtered.
		{"non-commitment-history-kept", "history/v1.0-accounts.0-16.v", 50, false},
		{"transactions-segment-kept", "v1.0-000000-000100-transactions.seg", 50, false},
		{"unparseable-name-kept", "salt-blocks.txt", 50, false},

		// Commitment domain files are never filtered (only history/idx/accessor).
		{"commitment-domain-kept", "domain/v1.0-commitment.0-16.kv", 50, false},

		// Commitment-history files: filter by .To <= minStep.
		{"history-fully-below-window-skipped", "history/v1.0-commitment.0-16.v", 16, true},
		{"history-straddling-window-kept", "history/v1.0-commitment.0-16.v", 15, false},
		{"history-above-window-kept", "history/v1.0-commitment.16-32.v", 8, false},
		{"idx-fully-below-window-skipped", "idx/v1.0-commitment.0-16.ef", 32, true},
		{"accessor-fully-below-window-skipped", "accessor/v1.0-commitment.0-16.vi", 32, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.skip, shouldSkipCommitmentHistorySegment(tc.file, tc.minStep))
		})
	}
}
