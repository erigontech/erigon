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

	blackList := buildBlackListForSparse(preverified)
	t.Logf("Sparse blacklist: %d files", len(blackList))

	var historyBlacklisted, efBlacklisted, caplinBlacklisted, domainBlacklisted int
	var indexKept, blockSegKept, domainKept int
	for _, p := range preverified.Items {
		name := p.Name
		_, blacklisted := blackList[name]

		if blacklisted {
			// Index files should never be blacklisted
			if !isDataFile(name) {
				t.Errorf("Index file should not be blacklisted: %s", name)
			}
			if strings.HasPrefix(name, "history") {
				historyBlacklisted++
			} else if strings.HasPrefix(name, "idx/") && strings.HasSuffix(name, ".ef") {
				efBlacklisted++
			} else if isCaplinFile(name) {
				caplinBlacklisted++
			} else if strings.HasPrefix(name, "domain") {
				domainBlacklisted++
			}
		}

		if !isDataFile(name) && !blacklisted {
			indexKept++
		}

		if strings.HasPrefix(name, "domain") && !blacklisted {
			domainKept++
		}

		// Block-level .seg files should never be blacklisted
		if strings.HasSuffix(name, ".seg") && !isStateSnapshot(name) && !isCaplinFile(name) && blacklisted {
			t.Errorf("Block .seg file should not be blacklisted in sparse mode: %s", name)
		}
		if strings.HasSuffix(name, ".seg") && !isStateSnapshot(name) && !isCaplinFile(name) && !blacklisted {
			blockSegKept++
		}
	}

	t.Logf("History blacklisted: %d, EF blacklisted: %d, Caplin blacklisted: %d, Domain blacklisted: %d",
		historyBlacklisted, efBlacklisted, caplinBlacklisted, domainBlacklisted)
	t.Logf("Index kept: %d, Block .seg kept: %d, Domain kept (latest only): %d",
		indexKept, blockSegKept, domainKept)

	if historyBlacklisted == 0 {
		t.Error("Expected some history .v files to be blacklisted")
	}
	if caplinBlacklisted == 0 {
		t.Error("Expected some caplin files to be blacklisted")
	}
	if indexKept == 0 {
		t.Error("Expected some index files to be kept")
	}
	if blockSegKept == 0 {
		t.Error("Expected block .seg files to be kept")
	}
	if domainKept == 0 {
		t.Error("Expected at least the latest domain files to be kept")
	}
	if domainBlacklisted == 0 {
		t.Error("Expected older domain files to be blacklisted")
	}

	// Verify only ONE domain .kv file per type is kept (the latest)
	keptByType := make(map[string]string)
	for _, p := range preverified.Items {
		if !strings.HasPrefix(p.Name, "domain") || !strings.HasSuffix(p.Name, ".kv") {
			continue
		}
		if _, bl := blackList[p.Name]; bl {
			continue
		}
		domainType, _, ok := parseDomainFile(p.Name)
		if !ok {
			continue
		}
		if prev, exists := keptByType[domainType]; exists {
			t.Errorf("Multiple domain .kv files kept for type %q: %s and %s", domainType, prev, p.Name)
		}
		keptByType[domainType] = p.Name
		t.Logf("  Kept latest %s: %s", domainType, p.Name)
	}

	// Must have commitment (determines execution start)
	if _, ok := keptByType["commitment"]; !ok {
		t.Error("Latest commitment domain file must be kept")
	}
	for _, required := range []string{"accounts", "code", "storage"} {
		if _, ok := keptByType[required]; !ok {
			t.Errorf("Latest %s domain file must be kept", required)
		}
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
