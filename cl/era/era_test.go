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
	"bytes"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
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

func TestReadReferenceEra1(t *testing.T) {
	path := referenceEra1Path(t)
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()

	_, cfg, _, err := clparams.GetConfigsByNetworkName("mainnet")
	if err != nil {
		t.Fatalf("mainnet config: %v", err)
	}

	e, err := ReadEra(f, cfg)
	if err != nil {
		t.Fatalf("ReadEra: %v", err)
	}

	if e.Number != 1 {
		t.Errorf("era number = %d, want 1", e.Number)
	}
	if e.State == nil {
		t.Fatal("no state decoded")
	}
	if got := e.State.Slot(); got != 1*SlotsPerHistoricalRoot {
		t.Errorf("state slot = %d, want %d", got, SlotsPerHistoricalRoot)
	}
	if len(e.Blocks) != 7715 {
		t.Errorf("decoded blocks = %d, want 7715", len(e.Blocks))
	}

	// Spec verification rule: the root of each block in the era file must
	// match state.block_roots. The era-1 state is at slot 8192; its ring
	// buffer covers slots 1..8191 directly (slot 0 is the cross-era edge
	// case and is skipped here).
	verified := 0
	for _, blk := range e.Blocks {
		slot := blk.Block.Slot
		if slot < 1 || slot > SlotsPerHistoricalRoot-1 {
			continue
		}
		want, err := e.State.GetBlockRootAtSlot(slot)
		if err != nil {
			t.Fatalf("GetBlockRootAtSlot(%d): %v", slot, err)
		}
		got, err := blk.Block.HashSSZ()
		if err != nil {
			t.Fatalf("block.HashSSZ (slot %d): %v", slot, err)
		}
		if want != got {
			t.Fatalf("block root mismatch at slot %d: file %x, state.block_roots %x", slot, got, want)
		}
		verified++
	}
	if verified == 0 {
		t.Fatal("no blocks verified against state.block_roots")
	}
	t.Logf("era-1: %d blocks decoded, %d verified against state.block_roots", len(e.Blocks), verified)
}

func TestRoundTripReferenceEra1(t *testing.T) {
	path := referenceEra1Path(t)
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()

	_, cfg, _, err := clparams.GetConfigsByNetworkName("mainnet")
	if err != nil {
		t.Fatalf("mainnet config: %v", err)
	}

	// Decode the reference file.
	e1, err := ReadEra(f, cfg)
	if err != nil {
		t.Fatalf("ReadEra (reference): %v", err)
	}

	// Re-encode it with our writer.
	var buf bytes.Buffer
	if err := WriteEra(&buf, e1); err != nil {
		t.Fatalf("WriteEra: %v", err)
	}

	// Our output must still scan as a structurally valid single-group .era.
	groups, err := Scan(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("Scan (our output): %v", err)
	}
	if len(groups) != 1 {
		t.Fatalf("our output: %d groups, want 1", len(groups))
	}

	// Decode our output and compare to the reference, field by field.
	e2, err := ReadEra(bytes.NewReader(buf.Bytes()), cfg)
	if err != nil {
		t.Fatalf("ReadEra (our output): %v", err)
	}

	if e2.Number != e1.Number {
		t.Errorf("era number: ours %d, reference %d", e2.Number, e1.Number)
	}
	if len(e2.Blocks) != len(e1.Blocks) {
		t.Fatalf("block count: ours %d, reference %d", len(e2.Blocks), len(e1.Blocks))
	}

	// SSZ is canonical: a decode→encode round-trip is byte-identical, so
	// every block and the state must hash-tree-root identically.
	for i := range e1.Blocks {
		r1, err := e1.Blocks[i].Block.HashSSZ()
		if err != nil {
			t.Fatal(err)
		}
		r2, err := e2.Blocks[i].Block.HashSSZ()
		if err != nil {
			t.Fatal(err)
		}
		if r1 != r2 {
			t.Fatalf("block %d (slot %d) root mismatch after round-trip: %x vs %x",
				i, e1.Blocks[i].Block.Slot, r1, r2)
		}
	}

	s1, err := e1.State.HashSSZ()
	if err != nil {
		t.Fatal(err)
	}
	s2, err := e2.State.HashSSZ()
	if err != nil {
		t.Fatal(err)
	}
	if s1 != s2 {
		t.Fatalf("state root mismatch after round-trip: %x vs %x", s1, s2)
	}

	// And our output independently satisfies the era-spec integrity rule.
	for _, blk := range e2.Blocks {
		slot := blk.Block.Slot
		if slot < 1 || slot > SlotsPerHistoricalRoot-1 {
			continue
		}
		want, err := e2.State.GetBlockRootAtSlot(slot)
		if err != nil {
			t.Fatal(err)
		}
		got, err := blk.Block.HashSSZ()
		if err != nil {
			t.Fatal(err)
		}
		if want != got {
			t.Fatalf("our output: block root mismatch at slot %d", slot)
		}
	}

	t.Logf("round-trip ok: %d blocks + state re-encoded, all roots match; "+
		"reference %d bytes, ours %d bytes (snappy framing differs, payloads identical)",
		len(e2.Blocks), fileSize(t, path), buf.Len())
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return fi.Size()
}
