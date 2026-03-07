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

	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/execution/chain/networkname"
)

func TestBlackListForSparse(t *testing.T) {
	c, ok := snapcfg.KnownCfg(networkname.Mainnet)
	if !ok {
		t.Fatal("no known cfg")
	}

	preverified := c.Preverified

	maxStep, err := getMaxStepRangeInSnapshots(preverified)
	if err != nil {
		t.Fatal(err)
	}

	keepRecentSteps := uint64(64)
	blackList := buildBlackListForSparse(keepRecentSteps, maxStep, preverified)
	t.Logf("Sparse blacklist: %d files (maxStep=%d, keepRecent=%d)", len(blackList), maxStep, keepRecentSteps)

	var stateBlacklisted, caplinBlacklisted, indexSkipped, recentDomainKept, blockSegKept int
	for _, p := range preverified.Items {
		name := p.Name
		_, blacklisted := blackList[name]

		if blacklisted {
			// Index files should never be blacklisted
			if !isDataFile(name) {
				t.Errorf("Index file should not be blacklisted: %s", name)
			}
			if isStateSnapshot(name) {
				stateBlacklisted++
			} else if isCaplinFile(name) {
				caplinBlacklisted++
			}
		}

		if !isDataFile(name) && !blacklisted {
			indexSkipped++
		}

		// Block-level .seg files should never be blacklisted
		if strings.HasSuffix(name, ".seg") && !isStateSnapshot(name) && !isCaplinFile(name) && blacklisted {
			t.Errorf("Block .seg file should not be blacklisted in sparse mode: %s", name)
		}
		if strings.HasSuffix(name, ".seg") && !isStateSnapshot(name) && !isCaplinFile(name) && !blacklisted {
			blockSegKept++
		}

		// Recent domain .kv files should not be blacklisted
		if strings.HasPrefix(name, "domain") && strings.HasSuffix(name, ".kv") {
			info, _, ok := snaptype.ParseFileName("", name)
			if ok && info.From >= maxStep-keepRecentSteps && blacklisted {
				t.Errorf("Recent domain file should not be blacklisted: %s (from=%d, threshold=%d)", name, info.From, maxStep-keepRecentSteps)
			}
			if ok && info.From >= maxStep-keepRecentSteps && !blacklisted {
				recentDomainKept++
			}
		}
	}

	t.Logf("State data blacklisted: %d, Caplin blacklisted: %d, Index files kept: %d, Block .seg kept: %d, Recent domain .kv kept: %d",
		stateBlacklisted, caplinBlacklisted, indexSkipped, blockSegKept, recentDomainKept)

	if stateBlacklisted == 0 {
		t.Error("Expected some state data files to be blacklisted in sparse mode")
	}
	if indexSkipped == 0 {
		t.Error("Expected some index files to be kept (not blacklisted)")
	}
	if recentDomainKept == 0 {
		t.Error("Expected some recent domain files to be kept locally")
	}
	if blockSegKept == 0 {
		t.Error("Expected block .seg files to be kept (not blacklisted)")
	}
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
