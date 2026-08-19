package commitment

// Measures what EIP-8297's storage layout costs and what its co-location buys.
//
// The embedding splits storage three ways: slots 0..63 sit in the account header
// under 34-byte keys sharing the account's stem; slots from 64 on go to the
// storage zone under 66-byte keys carrying BOTH the account stem digest and a
// per-256-slot group digest; and slots inside one group share that stem, which is
// the co-location the design is built around.
//
// Each pattern below writes the same NUMBER of slots to the same account, so the
// only thing that varies is where the embedding puts them.

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// pbinSlotAt returns a 32-byte big-endian slot number.
func pbinSlotAt(n uint64) []byte {
	var s [32]byte
	binary.BigEndian.PutUint64(s[24:], n)
	return s[:]
}

func TestPBinStorageLayoutCost(t *testing.T) {
	t.Parallel()

	const slots = 16
	addr := pbinOracleAddr(3)

	patterns := []struct {
		name string
		slot func(i int) uint64
	}{
		// 0..15: account header, 34-byte keys, one shared stem.
		{"header slots 0..15", func(i int) uint64 { return uint64(i) }},
		// 64..79: storage zone, one group (64/256 == 79/256 == 0), shared group stem.
		{"one group, adjacent 64..79", func(i int) uint64 { return 64 + uint64(i) }},
		// 256 apart: every slot lands in its own group, so no group stem is shared.
		{"one slot per group, 256 apart", func(i int) uint64 { return 256 * uint64(i+1) }},
		// Far apart: distinct groups and distinct high bits, the worst case for sharing.
		{"scattered across the zone", func(i int) uint64 { return 1 << (uint(i) + 20) }},
	}

	type row struct {
		name             string
		nodes, total     int
		leafKey, branch  int
		leaves, branches int
	}
	rows := make([]row, 0, len(patterns))

	buildRow := func(p struct {
		name string
		slot func(i int) uint64
	}) row {
		c := new(pbinTestCorpus).account(addr, 1, 100, pbinTestCodeHash(0))
		for i := range slots {
			c = c.storage(addr, pbinSlotAt(p.slot(i)), byte(i+1))
		}

		rec := &pbinWitnessRecorder{}
		_, pph := pbinWitnessProcess(t, c, rec)
		defer pph.Release()

		r := row{name: p.name}
		for _, n := range rec.byHash(t) {
			r.total += len(n)
			r.nodes++
			switch n[0] {
			case pbinLeafTag:
				r.leaves++
				r.leafKey += len(n) - 1 - pbinValueLength
			case pbinBranchTag:
				r.branches++
				r.branch += len(n)
			default:
				t.Fatalf("unknown tag %#x", n[0])
			}
		}
		return r
	}
	for _, p := range patterns {
		rows = append(rows, buildRow(p))
	}

	t.Logf("%d storage slots on one account, by where the embedding puts them:", slots)
	t.Logf("%-32s %6s %8s %8s %8s %7s %9s", "pattern", "nodes", "bytes", "leafkey", "branch", "leaves", "key/leaf")
	for _, r := range rows {
		t.Logf("%-32s %6d %8d %8d %8d %7d %9.1f",
			r.name, r.nodes, r.total, r.leafKey, r.branch, r.leaves,
			float64(r.leafKey)/float64(max(r.leaves, 1)))
	}

	base := rows[0].total
	for _, r := range rows[1:] {
		t.Logf("  %-30s %.2fx the header-slot case", r.name, float64(r.total)/float64(base))
	}

	// The header window is 64 slots wide, so slot 63 is a 34-byte key and slot 64
	// is a 66-byte one — the embedding's sharpest discontinuity.
	require.Less(t, rows[0].leafKey, rows[1].leafKey,
		"header slots must carry less key material than storage-zone slots")
}

// TestPBinStorageGroupSharing isolates co-location: the same 16 slots, once packed
// into one group and once spread one-per-group. Both are storage-zone keys of the
// same width, so any difference is the shared group stem alone.
//
// Two things to get right:
//
//   - Measure the PRUNED witness. Witnesses returns a superset that callers prune
//     with PBinWitnessNodesForKeys; the superset carries off-path siblings re-hashed
//     during the fold, and counting those reverses the sign of the result.
//   - Vary the right axis. Other accounts' storage diverges above this account's
//     stem and cancels in the difference, so filler on other accounts cannot move
//     the number. What matters is how many OTHER groups this account already holds.
func TestPBinStorageGroupSharing(t *testing.T) {
	t.Parallel()

	const slots = 16
	addr := pbinOracleAddr(5)

	// Two passes. The first builds the whole tree so the branch records exist; the
	// second proves ONLY the 16 slots. Measuring the first pass would include every
	// filler account and hide exactly the effect under test.
	measure := func(filler int, step uint64) (nodes, total int) {
		// Filler is untouched storage on the SAME account: that is the axis the
		// co-location property is about. Other accounts' keys diverge above this
		// account's stem and cancel between the two arms.
		full := new(pbinTestCorpus).account(addr, 1, 100, pbinTestCodeHash(0))
		for i := range filler {
			full = full.storage(addr, pbinSlotAt(1<<20+256*uint64(i)), 0x7f)
		}
		touched := new(pbinTestCorpus)
		for i := range slots {
			full = full.storage(addr, pbinSlotAt(64+step*uint64(i)), byte(i+1))
			touched = touched.storage(addr, pbinSlotAt(64+step*uint64(i)), byte(i+1))
		}

		pph, ms := pbinTestEngine(t)
		defer pph.Release()
		full.applyTo(t, ms)
		pbinTestProcess(t, pph, full.plainKeys, full.updates)

		pph.Reset()
		upd := WrapKeyUpdates(t, ModeDirect, pbinKeyHasher(), touched.plainKeys, touched.updates)
		got, proved, root, err := pph.Witnesses(context.Background(), upd, false, "")
		require.NoError(t, err)
		lean, err := PBinWitnessNodesForKeys(got, root, proved)
		require.NoError(t, err)
		for _, n := range lean {
			total += len(n)
			nodes++
		}
		return nodes, total
	}

	t.Logf("%8s %10s %10s %12s %10s", "other grps", "adjacent", "per-group", "co-loc saves", "of total")
	for _, filler := range []int{0, 16, 64, 256, 1024, 4096} {
		adjN, adjB := measure(filler, 1)
		sepN, sepB := measure(filler, 256)
		t.Logf("%8d %6d/%4dB %6d/%4dB %10dB %9.1f%%",
			filler, adjN, adjB, sepN, sepB, sepB-adjB, 100*float64(sepB-adjB)/float64(sepB))
	}
}

func pbinTestCodeHash(n byte) (h [32]byte) {
	h[31] = n
	return h
}

var _ = fmt.Sprintf
