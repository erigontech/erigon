package stagedsync

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
)

// fakePrevReader serves pre-block (first-touch) values for the builder, keyed by
// domain+key. Absent keys return nil (new key). It records how many times each
// key was read so a test can assert the builder only reads pre-block state once
// per key (subsequent prevs chain in-memory).
type fakePrevReader struct {
	vals  map[kv.Domain]map[string][]byte
	reads map[kv.Domain]map[string]int
}

func newFakePrevReader() *fakePrevReader {
	return &fakePrevReader{
		vals:  map[kv.Domain]map[string][]byte{},
		reads: map[kv.Domain]map[string]int{},
	}
}

func (r *fakePrevReader) set(d kv.Domain, key string, v []byte) {
	if r.vals[d] == nil {
		r.vals[d] = map[string][]byte{}
	}
	r.vals[d][key] = v
}

func (r *fakePrevReader) prevValue(d kv.Domain, key []byte, _ uint64) ([]byte, error) {
	ks := string(key)
	if r.reads[d] == nil {
		r.reads[d] = map[string]int{}
	}
	r.reads[d][ks]++
	return r.vals[d][ks], nil
}

// diffEntry is a decoded (key-without-step, step, prevValue) view of one
// DomainEntryDiff, for readable assertions independent of the ^step encoding.
type diffEntry struct {
	key  string
	step kv.Step
	prev string // "" means an explicit empty ([]byte{}) restore-marker (new key)
}

func decodeDiffs(t *testing.T, diffs []kv.DomainEntryDiff) []diffEntry {
	t.Helper()
	out := make([]diffEntry, 0, len(diffs))
	for _, d := range diffs {
		require.GreaterOrEqual(t, len(d.Key), 8, "diff key must carry an 8-byte step suffix")
		raw := []byte(d.Key)
		body := raw[:len(raw)-8]
		var inv uint64
		for _, b := range raw[len(raw)-8:] {
			inv = inv<<8 | uint64(b)
		}
		out = append(out, diffEntry{key: string(body), step: kv.Step(^inv), prev: string(d.Value)})
	}
	return out
}

const testStepSize = 4

func TestChangesetBuilder_NewKeyRecordsEmptyPrev(t *testing.T) {
	r := newFakePrevReader()
	b := newChangesetBuilder(r, testStepSize)

	b.record(kv.AccountsDomain, []byte("acct"), []byte("v1"), 0)
	require.NoError(t, b.err())

	got := decodeDiffs(t, b.result().Diffs[kv.AccountsDomain].GetDiffSet())
	require.Equal(t, []diffEntry{{key: "acct", step: 0, prev: ""}}, got)
}

func TestChangesetBuilder_FirstTouchKeepsPreBlockPrev(t *testing.T) {
	r := newFakePrevReader()
	r.set(kv.AccountsDomain, "acct", []byte("pre"))
	b := newChangesetBuilder(r, testStepSize)

	b.record(kv.AccountsDomain, []byte("acct"), []byte("v1"), 0)
	got := decodeDiffs(t, b.result().Diffs[kv.AccountsDomain].GetDiffSet())
	require.Equal(t, []diffEntry{{key: "acct", step: 0, prev: "pre"}}, got)
}

func TestChangesetBuilder_OverwriteSameStepKeepsFirstPrev(t *testing.T) {
	r := newFakePrevReader()
	r.set(kv.AccountsDomain, "acct", []byte("pre"))
	b := newChangesetBuilder(r, testStepSize)

	// Two writes in the same step: only the first-per-step entry survives, and
	// it carries the pre-block prev.
	b.record(kv.AccountsDomain, []byte("acct"), []byte("v1"), 0)
	b.record(kv.AccountsDomain, []byte("acct"), []byte("v2"), 1)
	got := decodeDiffs(t, b.result().Diffs[kv.AccountsDomain].GetDiffSet())
	require.Equal(t, []diffEntry{{key: "acct", step: 0, prev: "pre"}}, got)
	require.Equal(t, 1, r.reads[kv.AccountsDomain]["acct"], "pre-block read must happen once; later prevs chain in memory")
}

func TestChangesetBuilder_ABAStillRecordsEntry(t *testing.T) {
	r := newFakePrevReader()
	r.set(kv.AccountsDomain, "acct", []byte("A"))
	b := newChangesetBuilder(r, testStepSize)

	// A -> B -> A within one step: exec's replay records the first non-noop
	// (prev=A) and dedups the rest; the net-zero round-trip still yields an
	// entry restoring A.
	b.record(kv.AccountsDomain, []byte("acct"), []byte("B"), 0)
	b.record(kv.AccountsDomain, []byte("acct"), []byte("A"), 1)
	got := decodeDiffs(t, b.result().Diffs[kv.AccountsDomain].GetDiffSet())
	require.Equal(t, []diffEntry{{key: "acct", step: 0, prev: "A"}}, got)
}

func TestChangesetBuilder_NoOpWriteRecordsNothing(t *testing.T) {
	r := newFakePrevReader()
	r.set(kv.StorageDomain, "slot", []byte("X"))
	b := newChangesetBuilder(r, testStepSize)

	// Writing the same value the slot already holds is a no-op (mirrors
	// DomainPut's bytes.Equal(prev,v) skip): no diff entry.
	b.record(kv.StorageDomain, []byte("slot"), []byte("X"), 0)
	require.Empty(t, b.result().Diffs[kv.StorageDomain].GetDiffSet())
}

func TestChangesetBuilder_StraddleRecordsPerStepEntries(t *testing.T) {
	r := newFakePrevReader()
	r.set(kv.AccountsDomain, "acct", []byte("A"))
	b := newChangesetBuilder(r, testStepSize)

	// Block straddles a step edge (stepSize=4): txNum 3 is step 0, txNum 4 is
	// step 1. A key written in both steps yields two entries — step 0 with the
	// pre-block prev, step 1 with the intermediate value written in step 0.
	b.record(kv.AccountsDomain, []byte("acct"), []byte("B"), 3)
	b.record(kv.AccountsDomain, []byte("acct"), []byte("C"), 4)
	got := decodeDiffs(t, b.result().Diffs[kv.AccountsDomain].GetDiffSet())
	require.ElementsMatch(t, []diffEntry{
		{key: "acct", step: 0, prev: "A"},
		{key: "acct", step: 1, prev: "B"},
	}, got)
}

func TestChangesetBuilder_CodeDeleteWhenAbsentRecordsNothing(t *testing.T) {
	r := newFakePrevReader() // no prior code for the key
	b := newChangesetBuilder(r, testStepSize)

	// A code delete with no prior value is a no-op — the system-address code
	// "write" of empty bytes during an EIP-4788/2935 system call. Mirrors
	// SharedDomains.DomainDel's CodeDomain skip.
	b.record(kv.CodeDomain, []byte("sysaddr"), nil, 0)
	require.Empty(t, b.result().Diffs[kv.CodeDomain].GetDiffSet())
}

func TestChangesetBuilder_StorageDeleteWhenAbsentStillRecords(t *testing.T) {
	r := newFakePrevReader() // no prior slot value
	b := newChangesetBuilder(r, testStepSize)

	// Storage deletes are NOT skipped when absent (only CodeDomain is).
	b.record(kv.StorageDomain, []byte("slot"), nil, 0)
	got := decodeDiffs(t, b.result().Diffs[kv.StorageDomain].GetDiffSet())
	require.Equal(t, []diffEntry{{key: "slot", step: 0, prev: ""}}, got)
}

func TestChangesetBuilder_DeleteRecordsPrev(t *testing.T) {
	r := newFakePrevReader()
	r.set(kv.StorageDomain, "slot", []byte("V"))
	b := newChangesetBuilder(r, testStepSize)

	// A delete (empty new value) restores the prior value on unwind.
	b.record(kv.StorageDomain, []byte("slot"), nil, 0)
	got := decodeDiffs(t, b.result().Diffs[kv.StorageDomain].GetDiffSet())
	require.Equal(t, []diffEntry{{key: "slot", step: 0, prev: "V"}}, got)
}
