package state

import "testing"

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
