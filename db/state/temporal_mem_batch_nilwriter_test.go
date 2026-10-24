package state

import (
	"strings"
	"testing"

	"github.com/erigontech/erigon/db/kv"
)

// A MISSING DOMAIN WRITER MUST NOT KILL THE NODE, AND MUST NOT BE SILENT.
//
// SetDiff dereferences its receiver even when the diff is nil, so a nil entry in domainWriters
// panicked inside SetChangesetAccumulator and took the process with it — measured on soak live435
// pass 4 (9600 orders), reached from FCU via preExecFrontier.RetireBelow. 1200/2400/4800 ran clean,
// so the nil is a lifecycle window rather than a static shape, and the panic carried no indication
// of WHICH domain was missing. The counter is what makes skipping it a stopgap and not a
// paper-over: it names the domain for the next occurrence.
func TestSetChangesetAccumulatorSurvivesAMissingWriterAndCountsIt(t *testing.T) {
	before := NilDomainWriters()
	sd := &TemporalMemBatch{} // every writer nil — the worst case of the shape that crashed

	sd.SetChangesetAccumulator(nil) // must not panic

	after := NilDomainWriters()
	moved := 0
	for i := range after {
		if after[i] > before[i] {
			moved++
		}
	}
	if moved == 0 {
		t.Fatal("a missing writer was skipped WITHOUT being counted — the stopgap is silent, " +
			"which is the thing it exists not to be")
	}
}

// A write that lands on a batch Merge has already emptied must FAIL, not crash and not vanish.
//
// Merge hands the source batch's writers to the destination and nils every one of its own slots.
// A pre-exec round still running against that batch then dereferenced nothing and killed the node
// on every 9,600-order soak pass. Skipping the write instead would silently drop real state, so
// the only safe answer is an error the round can fail on.
func TestAWriteAfterMergeFailsInsteadOfCrashing(t *testing.T) {
	sd := &TemporalMemBatch{}
	// Exactly the state Merge leaves behind: every domain writer nil.
	sd.mergedAway.Store(true)

	err := sd.DomainPut(kv.AccountsDomain, "k", []byte("v"), 1, nil)
	if err == nil {
		t.Fatal("a write to a merged-away batch returned no error — it used to panic instead")
	}
	if !strings.Contains(err.Error(), "mergedAway=true") {
		t.Errorf("the error does not say the batch was merged away: %v", err)
	}

	// A delete takes the same path and must behave the same way.
	if err := sd.DomainDel(kv.AccountsDomain, "k", 1, nil); err == nil {
		t.Fatal("a delete to a merged-away batch returned no error")
	}
}

// Taking the writers EMPTIES the source, so it must only happen when the source is being closed.
//
// Merge's closeOther=false means "keep the source alive as the successor's read-through parent".
// The confiscation used to be inline and unconditional, so the source came out alive for READING
// and dead for WRITING, and the pre-exec round that still owned it killed the node on every
// 9,600-order soak pass.
func TestTakingTheWritersEmptiesTheSource(t *testing.T) {
	src := &TemporalMemBatch{}
	for i := range src.domainWriters {
		src.domainWriters[i] = &DomainBufferedWriter{discard: true}
	}
	src.forkableWriters = map[kv.ForkableId]kv.BufferedWriter{}
	dst := &TemporalMemBatch{pastForkableWriters: map[kv.ForkableId][]kv.BufferedWriter{}}

	// Untouched: a source kept alive keeps every writer and is not marked merged away.
	if src.mergedAway.Load() {
		t.Fatal("a fresh batch is already marked merged away")
	}
	for d := range src.domainWriters {
		if src.domainWriters[d] == nil {
			t.Fatalf("domain %s started with no writer", kv.Domain(d))
		}
	}

	dst.takeWritersFrom(src)

	if !src.mergedAway.Load() {
		t.Error("the source was emptied but not marked merged away, so the error cannot say why")
	}
	for d := range src.domainWriters {
		if src.domainWriters[d] != nil {
			t.Errorf("domain %s kept its writer; the destination never received it", kv.Domain(d))
		}
	}
	if got := len(dst.pastDomainWriters[kv.AccountsDomain]); got != 1 {
		t.Errorf("destination holds %d past writers for accounts, want 1", got)
	}
	// And the source is now refused rather than crashed.
	if err := src.DomainPut(kv.AccountsDomain, "k", []byte("v"), 1, nil); err == nil {
		t.Error("a write to the emptied source was accepted")
	}
}
