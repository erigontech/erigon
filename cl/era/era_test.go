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

//go:build integration

package era

import (
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const referenceEra1URL = "https://mainnet.era.nimbus.team/mainnet-00001-40cf2f3c.era"

// referenceEra1Path returns a cached copy of the nimbus mainnet era-1
// reference file, downloading it on first use. The test is skipped if the
// download is unavailable (offline CI).
func referenceEra1Path(t *testing.T) string {
	t.Helper()
	cacheDir := filepath.Join(os.TempDir(), "erigon-era-itest")
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		t.Fatalf("mkdir cache: %v", err)
	}
	path := filepath.Join(cacheDir, "mainnet-00001-40cf2f3c.era")
	if fi, err := os.Stat(path); err == nil && fi.Size() > 0 {
		return path
	}

	client := &http.Client{Timeout: 5 * time.Minute}
	resp, err := client.Get(referenceEra1URL)
	if err != nil {
		t.Skipf("cannot download reference era file (offline?): %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Skipf("reference era file fetch returned %s", resp.Status)
	}

	tmp := path + ".tmp"
	out, err := os.Create(tmp)
	if err != nil {
		t.Fatalf("create cache file: %v", err)
	}
	if _, err := io.Copy(out, resp.Body); err != nil {
		out.Close()
		os.Remove(tmp)
		t.Fatalf("download: %v", err)
	}
	out.Close()
	if err := os.Rename(tmp, path); err != nil {
		t.Fatalf("rename cache file: %v", err)
	}
	return path
}

func TestScanReferenceEra1(t *testing.T) {
	path := referenceEra1Path(t)
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()

	groups, err := Scan(f)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(groups) != 1 {
		t.Fatalf("group count = %d, want 1", len(groups))
	}
	g := groups[0]

	if !g.HasState {
		t.Error("group has no state record")
	}
	if g.StateIndex == nil {
		t.Fatal("group has no state SlotIndex")
	}
	if len(g.StateIndex.Offsets) != 1 {
		t.Errorf("state SlotIndex count = %d, want 1", len(g.StateIndex.Offsets))
	}
	if g.StateIndex.StartSlot != 1*SlotsPerHistoricalRoot {
		t.Errorf("state SlotIndex start slot = %d, want %d", g.StateIndex.StartSlot, SlotsPerHistoricalRoot)
	}

	if g.BlockIndex == nil {
		t.Fatal("era-1 group has no block SlotIndex")
	}
	if len(g.BlockIndex.Offsets) != SlotsPerHistoricalRoot {
		t.Errorf("block SlotIndex count = %d, want %d", len(g.BlockIndex.Offsets), SlotsPerHistoricalRoot)
	}
	if g.BlockIndex.StartSlot != 0 {
		t.Errorf("block SlotIndex start slot = %d, want 0", g.BlockIndex.StartSlot)
	}

	// Every real block sits at a unique offset; empty slots all share a
	// single sentinel offset (the minimum — it points at the group's
	// Version record, 8 bytes before the first block). So the count of
	// offset values that occur exactly once is the block-record count,
	// and block records + empty slots == SLOTS_PER_HISTORICAL_ROOT.
	freq := map[int64]int{}
	for _, off := range g.BlockIndex.Offsets {
		freq[off]++
	}
	uniqueOffsets, emptySlots := 0, 0
	for _, c := range freq {
		if c == 1 {
			uniqueOffsets++
		} else {
			emptySlots += c
		}
	}
	if g.BlockRecords != uniqueOffsets {
		t.Errorf("block records = %d, unique block-index offsets = %d", g.BlockRecords, uniqueOffsets)
	}
	if g.BlockRecords+emptySlots != SlotsPerHistoricalRoot {
		t.Errorf("blocks(%d) + empty(%d) = %d, want %d",
			g.BlockRecords, emptySlots, g.BlockRecords+emptySlots, SlotsPerHistoricalRoot)
	}
	if len(g.UnknownTypes) != 0 {
		t.Logf("unknown record types present: %v", g.UnknownTypes)
	}

	t.Logf("era-1: %d block records, %d empty slots", g.BlockRecords, emptySlots)
}
