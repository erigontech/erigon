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
	"strconv"
	"strings"
	"testing"

	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/execution/chain/networkname"
)

func TestIsCommitmentHistorySegmentExpired(t *testing.T) {
	makeItem := func(fromStep, toStep uint64) snapcfg.PreverifiedItem {
		return snapcfg.PreverifiedItem{
			Name: "v1.0-commitment." + strconv.FormatUint(fromStep, 10) + "-" + strconv.FormatUint(toStep, 10) + ".v",
		}
	}
	cases := []struct {
		name              string
		commitmentMinStep uint64
		fromStep          uint64
		toStep            uint64
		expected          bool
	}{
		{"retention inactive (minStep=0)", 0, 0, 64, false},
		{"segment far below boundary", 2815, 0, 64, true},
		{"segment just below boundary", 2815, 2752, 2816, true},
		{"segment exactly at boundary", 2815, 2815, 2879, false},
		{"segment just above boundary", 2815, 2816, 2880, false},
		{"segment far above boundary", 2815, 2900, 2964, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := isCommitmentHistorySegmentExpired(makeItem(tc.fromStep, tc.toStep), tc.commitmentMinStep)
			if got != tc.expected {
				t.Errorf("got %v, want %v", got, tc.expected)
			}
		})
	}
	t.Run("unparseable filename", func(t *testing.T) {
		if isCommitmentHistorySegmentExpired(snapcfg.PreverifiedItem{Name: "garbage-filename"}, 2815) {
			t.Error("unparseable filename should not be marked expired")
		}
	})
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
