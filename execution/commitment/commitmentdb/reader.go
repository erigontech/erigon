package commitmentdb

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
)

// mode-D residual wrong-root diagnostic (2026-08-19).
// HistoryStateReader.Read compares its unbounded GetAsOf fallback
// against a step-bounded read AND verifies HistorySeek by directly
// walking K's history in (txN, maxTxN] via TraceKey. This isolates
// three classes of divergence:
//
//  1. Missed history entry: HistorySeek returned miss but K DOES
//     have a post-target history entry — HistorySeek is buggy for
//     this K. Smoking gun for wrong-root.
//  2. Unbounded file-set contamination: HistorySeek correctly
//     returned miss (no post-target history) but unbounded's file
//     walk reads a value that DIFFERS from the shadow's diff-replayed
//     value or from the step-bounded file walk — files were poisoned.
//  3. Benign shadow/file skew: unbounded returns shadow (via
//     getLatestFromDb gate satisfied), bounded returns file (skipped
//     shadow). Both are internally correct, they just disagree
//     because bounded doesn't consult shadow — happens for shallow
//     unwinds where files.EndTxN aligns with target's step.
//
// All three cases dump a sample to log; class 1 fires a WARN-level
// alarm because it's the class most likely to break the compute.
var (
	histReaderCompareCount   atomic.Uint64
	histReaderHitCount       atomic.Uint64
	histReaderMissCount      atomic.Uint64
	histReaderDivergeCount   atomic.Uint64
	histReaderSampleLogged   atomic.Uint64
	histReaderMissedHistSeek atomic.Uint64 // class 1 count

	// Per-file divergence histogram: (domain|fileRange) → count. Feeds
	// the dual-root diagnostic — when bounded doesn't match header,
	// the top offenders tell us WHICH files' contents contribute the
	// wrong values (aggregator retire/merge suspects vs shadow).
	histReaderDivergeByFileMu sync.Mutex
	histReaderDivergeByFile   = map[string]uint64{}
)

// HistReaderResetCounters resets the diagnostic counters. Call before
// each compute invocation so log output is per-compute.
func HistReaderResetCounters() {
	histReaderHitCount.Store(0)
	histReaderMissCount.Store(0)
	histReaderDivergeCount.Store(0)
	histReaderSampleLogged.Store(0)
	histReaderCompareCount.Store(0)
	histReaderMissedHistSeek.Store(0)
	histReaderDivergeByFileMu.Lock()
	histReaderDivergeByFile = map[string]uint64{}
	histReaderDivergeByFileMu.Unlock()
}

// HistReaderDivergentFileHistogramTop returns the top-N (source, count)
// pairs from the per-file divergence histogram, sorted descending.
// Fed into the dual-root diagnostic when bounded doesn't match header
// to pinpoint which files contribute the wrong values.
func HistReaderDivergentFileHistogramTop(n int) []struct {
	Source string
	Count  uint64
} {
	histReaderDivergeByFileMu.Lock()
	entries := make([]struct {
		Source string
		Count  uint64
	}, 0, len(histReaderDivergeByFile))
	for k, v := range histReaderDivergeByFile {
		entries = append(entries, struct {
			Source string
			Count  uint64
		}{Source: k, Count: v})
	}
	histReaderDivergeByFileMu.Unlock()
	sort.Slice(entries, func(i, j int) bool { return entries[i].Count > entries[j].Count })
	if len(entries) > n {
		entries = entries[:n]
	}
	return entries
}

func recordDivergentFileSource(domain kv.Domain, source string) {
	key := domain.String() + "|" + source
	histReaderDivergeByFileMu.Lock()
	histReaderDivergeByFile[key]++
	histReaderDivergeByFileMu.Unlock()
}

// HistReaderLogCounters logs the diagnostic counters.
func HistReaderLogCounters(domain kv.Domain, txN uint64) {
	log.Info("[dbg-hist-reader] compute-fallback stats",
		"domain", domain.String(),
		"txN", txN,
		"hits", histReaderHitCount.Load(),
		"misses", histReaderMissCount.Load(),
		"divergences", histReaderDivergeCount.Load(),
		"missedHistSeek", histReaderMissedHistSeek.Load())
}

type StateReader interface {
	WithHistory() bool
	CheckDataAvailable(d kv.Domain, step kv.Step) error
	Read(d kv.Domain, plainKey []byte, stepSize uint64) (enc []byte, step kv.Step, err error)
	Clone(tx kv.TemporalTx) StateReader
	// CloneForWorker clones the reader for a concurrent worker (trie-warmup /
	// concurrent-commitment mount). Behaves like Clone except reads are metered
	// into the per-worker accumulator carried by workerCtx, so concurrent
	// workers never touch the main goroutine's lock-free accumulator (a race)
	// or take the global metrics lock.
	CloneForWorker(workerCtx context.Context, tx kv.TemporalTx) StateReader
}

// ctxGetter is the optional context-aware read method (see
// temporalGetter.GetLatestContext). Worker readers type-assert for it so reads
// meter into the per-worker accumulator carried by the worker context.
type ctxGetter interface {
	GetLatestContext(ctx context.Context, name kv.Domain, k []byte) (v []byte, step kv.Step, err error)
}

type LatestStateReader struct {
	sharedDomains sd
	getter        kv.TemporalGetter
	srcTx         kv.TemporalTx
	// workerCtx, when non-nil, carries this worker's lock-free metrics
	// accumulator; reads route through getter.GetLatestContext(workerCtx, …) so
	// concurrent workers don't share the main accumulator. Nil on the main
	// reader, which meters into sd.mainWM via the plain GetLatest.
	workerCtx context.Context
}

func NewLatestStateReader(tx kv.TemporalTx, sd sd) *LatestStateReader {
	return &LatestStateReader{sharedDomains: sd, getter: sd.AsGetter(tx), srcTx: tx}
}

// NewLatestStateReaderForWorker is like NewLatestStateReader but reads meter
// into the per-worker accumulator carried by workerCtx (for concurrent
// workers). See LatestStateReader.workerCtx.
func NewLatestStateReaderForWorker(workerCtx context.Context, tx kv.TemporalTx, sd sd) *LatestStateReader {
	return &LatestStateReader{sharedDomains: sd, getter: sd.AsGetter(tx), srcTx: tx, workerCtx: workerCtx}
}

func (r *LatestStateReader) WithHistory() bool {
	return false
}

func (r *LatestStateReader) CheckDataAvailable(d kv.Domain, step kv.Step) error {
	// we're processing the latest state - in which case it needs to be writable
	if frozenSteps := r.getter.StepsInFiles(d); step < frozenSteps {
		return fmt.Errorf("%q state out of date: step %d, expected step %d", d, step, frozenSteps)
	}
	return nil
}

func (r *LatestStateReader) Read(d kv.Domain, plainKey []byte, stepSize uint64) (enc []byte, step kv.Step, err error) {
	if r.workerCtx != nil {
		if cg, ok := r.getter.(ctxGetter); ok {
			enc, step, err = cg.GetLatestContext(r.workerCtx, d, plainKey)
			if err != nil {
				return nil, 0, fmt.Errorf("LatestStateReader(GetLatestContext) %q: %w", d, err)
			}
			return enc, step, nil
		}
	}
	enc, step, err = r.getter.GetLatest(d, plainKey)
	if err != nil {
		return nil, 0, fmt.Errorf("LatestStateReader(GetLatest) %q: %w", d, err)
	}
	return enc, step, nil
}

func (r *LatestStateReader) Clone(tx kv.TemporalTx) StateReader {
	// Keep reading the source this reader was bound to. The tx passed by
	// clone/warmup callers targets the *compute* database, which may differ
	// from this reader's source — e.g. recomputing commitment in an empty db
	// while reading committed state from the source db (TouchChangedKeysFromHistory).
	// Before flush drained sd.mem this was masked because the in-memory batch
	// still held the source values; rebinding sd.AsGetter to the foreign compute
	// tx reads the wrong database and yields empty state (wrong root).
	return &LatestStateReader{sharedDomains: r.sharedDomains, getter: r.sharedDomains.AsGetter(r.srcTx), srcTx: r.srcTx, workerCtx: r.workerCtx}
}

// CloneForWorker clones into a worker reader that meters into workerCtx's
// per-worker accumulator. Source tx preserved, same as Clone.
func (r *LatestStateReader) CloneForWorker(workerCtx context.Context, tx kv.TemporalTx) StateReader {
	return NewLatestStateReaderForWorker(workerCtx, r.srcTx, r.sharedDomains)
}

// HistoryStateReader reads *full* historical state at specified txNum.
// `limitReadAsOfTxNum` here is used as timestamp for usual GetAsOf.
type HistoryStateReader struct {
	roTx               kv.TemporalTx
	limitReadAsOfTxNum uint64
}

func NewHistoryStateReader(roTx kv.TemporalTx, limitReadAsOfTxNum uint64) *HistoryStateReader {
	return &HistoryStateReader{
		roTx:               roTx,
		limitReadAsOfTxNum: limitReadAsOfTxNum,
	}
}

func (r *HistoryStateReader) WithHistory() bool {
	return true
}

func (r *HistoryStateReader) CheckDataAvailable(kv.Domain, kv.Step) error {
	return nil
}

func (r *HistoryStateReader) Read(d kv.Domain, plainKey []byte, stepSize uint64) (enc []byte, step kv.Step, err error) {
	histReaderCompareCount.Add(1)
	histSeek, hOk, hErr := r.roTx.HistorySeek(d, plainKey, r.limitReadAsOfTxNum)
	if hErr != nil {
		return nil, 0, fmt.Errorf("HistoryStateReader(HistorySeek) %q: (limitTxNum=%d): %w", d, r.limitReadAsOfTxNum, hErr)
	}
	if hOk {
		histReaderHitCount.Add(1)
		return histSeek, kv.Step(r.limitReadAsOfTxNum / stepSize), nil
	}
	histReaderMissCount.Add(1)

	// HistorySeek miss — GetAsOf falls through to unbounded GetLatest.
	// Sample 1-in-64 for cheap comparison. Only run the expensive
	// TraceKey walk when unbounded != bounded (which is rare — in most
	// mode-D runs it's ~0.1% of misses per the 2026-08-20 repros).
	sampleN := histReaderMissCount.Load()
	if sampleN&63 != 0 {
		unboundedVal, _, uErr := r.roTx.GetAsOf(d, plainKey, r.limitReadAsOfTxNum)
		if uErr != nil {
			return nil, 0, fmt.Errorf("HistoryStateReader(GetAsOf) %q: (limitTxNum=%d): %w", d, r.limitReadAsOfTxNum, uErr)
		}
		return unboundedVal, kv.Step(r.limitReadAsOfTxNum / stepSize), nil
	}

	// Sampled comparison path — cheap: 1 extra file read.
	maxStep := kv.Step(r.limitReadAsOfTxNum/stepSize) + 1
	unboundedVal, _, uErr := r.roTx.GetAsOf(d, plainKey, r.limitReadAsOfTxNum)
	if uErr != nil {
		return nil, 0, fmt.Errorf("HistoryStateReader(GetAsOf) %q: (limitTxNum=%d): %w", d, r.limitReadAsOfTxNum, uErr)
	}
	boundedVal, bEndStep, bFound, bErr := r.roTx.Debug().GetLatestFromFilesUpToStep(d, plainKey, maxStep)
	if bErr != nil {
		return nil, 0, fmt.Errorf("HistoryStateReader(GetLatestFromFilesUpToStep) %q: (limitTxNum=%d): %w", d, r.limitReadAsOfTxNum, bErr)
	}
	if bytes.Equal(unboundedVal, boundedVal) {
		return unboundedVal, kv.Step(r.limitReadAsOfTxNum / stepSize), nil
	}
	histReaderDivergeCount.Add(1)

	// Divergence detected — pay for TraceKey verification + full log.
	// This branch is rare (~0.01% of reads); overhead is bounded.
	maxAllowedTxN := uint64(maxStep) * stepSize
	sawEntryTxN, sawEntry := r.traceKeyForwardHead(d, plainKey, r.limitReadAsOfTxNum, maxAllowedTxN+stepSize*8)
	histSeekMissedRealEntry := sawEntry && sawEntryTxN >= r.limitReadAsOfTxNum
	if histSeekMissedRealEntry {
		histReaderMissedHistSeek.Add(1)
	}
	// Aggregate per-file (or shadow) divergence tally for the dual-root
	// diagnostic — always recorded on divergence, not just for sampled
	// full-detail log entries.
	shadowValRec, _, shadowFoundRec, _ := r.roTx.Debug().GetLatestFromDB(d, plainKey)
	_, _, ubStartTxNRec, ubEndTxNRec, _ := r.roTx.Debug().GetLatestFromFiles(d, plainKey, 0)
	var source string
	if shadowFoundRec && bytes.Equal(unboundedVal, shadowValRec) {
		source = "shadow"
	} else {
		source = fmt.Sprintf("file[%d,%d)", ubStartTxNRec, ubEndTxNRec)
	}
	recordDivergentFileSource(d, source)
	if histReaderSampleLogged.Load() < 5000 {
		histReaderSampleLogged.Add(1)
		shadowVal, shadowStep, shadowFound, _ := r.roTx.Debug().GetLatestFromDB(d, plainKey)
		unboundedFileVal, _, ubStartTxN, ubEndTxN, _ := r.roTx.Debug().GetLatestFromFiles(d, plainKey, 0)
		ubFromFile := "unknown"
		if shadowFound && bytes.Equal(unboundedVal, shadowVal) {
			ubFromFile = "shadow"
		} else if bytes.Equal(unboundedVal, unboundedFileVal) {
			ubFromFile = "file"
		}
		log.Warn("[dbg-hist-reader] divergence",
			"class", classifyDivergence(sawEntry, sawEntryTxN, r.limitReadAsOfTxNum, unboundedVal, boundedVal, shadowFound, shadowVal),
			"domain", d.String(),
			"key", fmt.Sprintf("%x", plainKey),
			"txN", r.limitReadAsOfTxNum,
			"maxStep", maxStep,
			"unbounded", fmt.Sprintf("%x", unboundedVal),
			"unboundedFromFile", ubFromFile,
			"unboundedFileRange", fmt.Sprintf("[%d,%d)", ubStartTxN, ubEndTxN),
			"bounded", fmt.Sprintf("%x", boundedVal),
			"boundedFound", bFound,
			"boundedFileEndStep", bEndStep,
			"shadow", fmt.Sprintf("%x", shadowVal),
			"shadowFound", shadowFound,
			"shadowStep", shadowStep,
			"histSeekSawEntryAt", sawEntryTxN,
			"histSeekMissedRealEntry", histSeekMissedRealEntry)
	}
	return unboundedVal, kv.Step(r.limitReadAsOfTxNum / stepSize), nil
}

// getUnboundedWithSource wraps GetAsOf's fallback path (via
// GetLatestFromDB / GetLatestFromFiles) to return WHICH file the value
// came from. The result should match plain GetAsOf but the return also
// carries the file range so we can identify the contamination source.
func (r *HistoryStateReader) getUnboundedWithSource(d kv.Domain, plainKey []byte) (v []byte, fileStartTxN, fileEndTxN uint64, fromShadow string, err error) {
	// Try shadow first — matches getLatestFromDb's semantic (subject to
	// the gate check which we can't cheaply replicate; we approximate by
	// checking shadow directly and reporting "shadow" if it returned).
	shadowVal, _, shadowFound, sErr := r.roTx.Debug().GetLatestFromDB(d, plainKey)
	_ = sErr
	// Read via files (matches getLatestFromFiles(k, 0) unbounded path).
	fileVal, fileFound, startTxN, endTxN, fErr := r.roTx.Debug().GetLatestFromFiles(d, plainKey, 0)
	if fErr != nil {
		return nil, 0, 0, "", fErr
	}
	// Real GetAsOf outcome — this is what compute uses.
	realVal, _, gErr := r.roTx.GetAsOf(d, plainKey, r.limitReadAsOfTxNum)
	if gErr != nil {
		return nil, 0, 0, "", gErr
	}
	// Tag the source of the real value.
	if shadowFound && bytes.Equal(realVal, shadowVal) {
		return realVal, 0, 0, "shadow", nil
	}
	if fileFound && bytes.Equal(realVal, fileVal) {
		return realVal, startTxN, endTxN, "file", nil
	}
	return realVal, 0, 0, "unknown", nil
}

// traceKeyForwardHead walks K's history in (fromTxN, toTxN] and returns
// the FIRST entry's txN. If no entries, returns (0, false).
// Used to verify HistorySeek's not-found result: if TraceKey finds an
// entry ≥ fromTxN+1 ≤ toTxN, HistorySeek missed it.
func (r *HistoryStateReader) traceKeyForwardHead(d kv.Domain, plainKey []byte, fromTxN, toTxN uint64) (txN uint64, found bool) {
	it, err := r.roTx.Debug().TraceKey(d, plainKey, fromTxN+1, toTxN)
	if err != nil {
		return 0, false
	}
	defer it.Close()
	if !it.HasNext() {
		return 0, false
	}
	firstTxN, _, terr := it.Next()
	if terr != nil {
		return 0, false
	}
	return firstTxN, true
}

// classifyDivergence returns a short class tag for the diagnostic log:
//   - class-1: HistorySeek missed a real entry (SMOKING GUN)
//   - class-2a: unbounded=shadow, bounded=file (benign shadow/file skew)
//   - class-2b: unbounded=file, bounded=file, values differ (files disagree)
//   - class-3: unbounded=shadow, bounded=file, bounded not found
//   - class-other: unclassified
func classifyDivergence(sawEntry bool, sawTxN, limitTxN uint64, unbounded, bounded []byte, shadowFound bool, shadowVal []byte) string {
	if sawEntry && sawTxN >= limitTxN {
		return "class-1-histSeek-missed-real-entry"
	}
	if shadowFound && bytes.Equal(unbounded, shadowVal) {
		if len(bounded) == 0 {
			return "class-3-shadow-vs-empty"
		}
		return "class-2a-unbounded=shadow-bounded=file"
	}
	if !shadowFound || !bytes.Equal(unbounded, shadowVal) {
		return "class-2b-files-disagree"
	}
	return "class-other"
}

// AsOf reports the history txNum this reader resolves state at.
func (r *HistoryStateReader) AsOf() uint64 { return r.limitReadAsOfTxNum }

func (r *HistoryStateReader) Clone(tx kv.TemporalTx) StateReader {
	return NewHistoryStateReader(tx, r.limitReadAsOfTxNum)
}

// CloneForWorker: history reads go straight to roTx.GetAsOf (no shared metrics
// accumulator), so it's identical to Clone.
func (r *HistoryStateReader) CloneForWorker(_ context.Context, tx kv.TemporalTx) StateReader {
	return NewHistoryStateReader(tx, r.limitReadAsOfTxNum)
}

// HistoryStateReaderBounded mirrors HistoryStateReader but replaces its
// unbounded GetAsOf fallback with a step-bounded read that excludes
// files whose endStep is past the target step. Used by Provider.Unwind's
// dual-root diagnostic: after the current compute produces a wrong
// root, re-run with this reader and log the resulting root — if it
// matches the header stateRoot, the unbounded fallback IS the bug.
type HistoryStateReaderBounded struct {
	roTx               kv.TemporalTx
	limitReadAsOfTxNum uint64
}

func NewHistoryStateReaderBounded(roTx kv.TemporalTx, limitReadAsOfTxNum uint64) *HistoryStateReaderBounded {
	return &HistoryStateReaderBounded{roTx: roTx, limitReadAsOfTxNum: limitReadAsOfTxNum}
}

func (r *HistoryStateReaderBounded) WithHistory() bool                           { return true }
func (r *HistoryStateReaderBounded) CheckDataAvailable(kv.Domain, kv.Step) error { return nil }

func (r *HistoryStateReaderBounded) Read(d kv.Domain, plainKey []byte, stepSize uint64) (enc []byte, step kv.Step, err error) {
	histSeek, hOk, hErr := r.roTx.HistorySeek(d, plainKey, r.limitReadAsOfTxNum)
	if hErr != nil {
		return nil, 0, fmt.Errorf("HistoryStateReaderBounded(HistorySeek) %q: (limitTxNum=%d): %w", d, r.limitReadAsOfTxNum, hErr)
	}
	if hOk {
		return histSeek, kv.Step(r.limitReadAsOfTxNum / stepSize), nil
	}
	maxStep := kv.Step(r.limitReadAsOfTxNum/stepSize) + 1
	boundedVal, endStep, found, bErr := r.roTx.Debug().GetLatestFromFilesUpToStep(d, plainKey, maxStep)
	if bErr != nil {
		return nil, 0, fmt.Errorf("HistoryStateReaderBounded(GetLatestFromFilesUpToStep) %q: (limitTxNum=%d): %w", d, r.limitReadAsOfTxNum, bErr)
	}
	if !found {
		return nil, 0, nil
	}
	return boundedVal, endStep, nil
}

func (r *HistoryStateReaderBounded) AsOf() uint64 { return r.limitReadAsOfTxNum }

func (r *HistoryStateReaderBounded) Clone(tx kv.TemporalTx) StateReader {
	return NewHistoryStateReaderBounded(tx, r.limitReadAsOfTxNum)
}

func (r *HistoryStateReaderBounded) CloneForWorker(_ context.Context, tx kv.TemporalTx) StateReader {
	return NewHistoryStateReaderBounded(tx, r.limitReadAsOfTxNum)
}

// FilesOnlyStateReader reads from .kv files only, capped at limitTxNum.
// On miss (key not present in any frozen .kv file ≤ limitTxNum), returns nil
// without any fallback. This is the right semantic for integrity checks and
// commitment rebuild that validate "what does the .kv snapshot at this
// boundary actually contain?": no consultation of history index, no
// consultation of current DB state, no consultation of .kv files past the
// boundary.
type FilesOnlyStateReader struct {
	roTx       kv.TemporalTx
	limitTxNum uint64
}

func NewFilesOnlyStateReader(roTx kv.TemporalTx, limitTxNum uint64) *FilesOnlyStateReader {
	return &FilesOnlyStateReader{roTx: roTx, limitTxNum: limitTxNum}
}

func (r *FilesOnlyStateReader) WithHistory() bool { return false }

func (r *FilesOnlyStateReader) CheckDataAvailable(kv.Domain, kv.Step) error { return nil }

func (r *FilesOnlyStateReader) Read(d kv.Domain, plainKey []byte, stepSize uint64) (enc []byte, step kv.Step, err error) {
	enc, ok, _, endTxNum, err := r.roTx.Debug().GetLatestFromFiles(d, plainKey, r.limitTxNum)
	if err != nil {
		return nil, 0, fmt.Errorf("FilesOnlyStateReader %q (limitTxNum=%d): %w", d, r.limitTxNum, err)
	}
	if !ok {
		return nil, 0, nil
	}
	return enc, kv.Step(endTxNum / stepSize), nil
}

func (r *FilesOnlyStateReader) Clone(tx kv.TemporalTx) StateReader {
	return NewFilesOnlyStateReader(tx, r.limitTxNum)
}

func (r *FilesOnlyStateReader) CloneForWorker(_ context.Context, tx kv.TemporalTx) StateReader {
	return NewFilesOnlyStateReader(tx, r.limitTxNum)
}

// StepBoundedFilesStateReader reads from .kv files only, capped at the file's
// end step (not an arbitrary txnum). Used by admin SetHead mode B for
// commitment-domain Branch reads: commitment domain has HistoryDisabled=true,
// so a txnum bound via GetAsOf doesn't time-travel.
type StepBoundedFilesStateReader struct {
	roTx    kv.TemporalTx
	maxStep kv.Step
}

func NewStepBoundedFilesStateReader(roTx kv.TemporalTx, maxStep kv.Step) *StepBoundedFilesStateReader {
	return &StepBoundedFilesStateReader{roTx: roTx, maxStep: maxStep}
}

func (r *StepBoundedFilesStateReader) WithHistory() bool                           { return false }
func (r *StepBoundedFilesStateReader) CheckDataAvailable(kv.Domain, kv.Step) error { return nil }

func (r *StepBoundedFilesStateReader) Read(d kv.Domain, plainKey []byte, stepSize uint64) (enc []byte, step kv.Step, err error) {
	enc, fileEndStep, found, err := r.roTx.Debug().GetLatestFromFilesUpToStep(d, plainKey, r.maxStep)
	if err != nil {
		return nil, 0, fmt.Errorf("StepBoundedFilesStateReader %q (maxStep=%d): %w", d, r.maxStep, err)
	}
	if !found {
		return nil, 0, nil
	}
	return enc, fileEndStep, nil
}

func (r *StepBoundedFilesStateReader) Clone(tx kv.TemporalTx) StateReader {
	return NewStepBoundedFilesStateReader(tx, r.maxStep)
}

func (r *StepBoundedFilesStateReader) CloneForWorker(_ context.Context, tx kv.TemporalTx) StateReader {
	return NewStepBoundedFilesStateReader(tx, r.maxStep)
}

// SplitStateReader implements commitmentdb.StateReader using (potentially) different state readers for commitment
// data and account/storage/code data.
type SplitStateReader struct {
	commitmentReader StateReader
	plainStateReader StateReader
	withHistory      bool
}

var _ StateReader = (*SplitStateReader)(nil) // compile-time type assertion

// A history reader that reads:
//   - commitment data as-of txnum commitmentAsOf
//   - account/storage/code data as-of txnum dataAsOf
func NewSplitHistoryReader(tx kv.TemporalTx, commitmentAsOf uint64, dataAsOf uint64, withHistory bool) *SplitStateReader {
	return &SplitStateReader{
		commitmentReader: NewHistoryStateReader(tx, commitmentAsOf),
		plainStateReader: NewHistoryStateReader(tx, dataAsOf),
		withHistory:      withHistory,
	}
}

func (r *SplitStateReader) WithHistory() bool {
	return r.withHistory
}

// PlainStateAsOf reports the history txNum the plain-state (account/storage/code)
// reader resolves at, when that reader is history-backed (ok=false otherwise).
func (r *SplitStateReader) PlainStateAsOf() (uint64, bool) {
	if h, ok := r.plainStateReader.(*HistoryStateReader); ok {
		return h.limitReadAsOfTxNum, true
	}
	return 0, false
}

func (r *SplitStateReader) CheckDataAvailable(_ kv.Domain, _ kv.Step) error {
	return nil
}

func (r *SplitStateReader) Read(d kv.Domain, plainKey []byte, stepSize uint64) ([]byte, kv.Step, error) {
	if d == kv.CommitmentDomain {
		return r.commitmentReader.Read(d, plainKey, stepSize)
	}
	return r.plainStateReader.Read(d, plainKey, stepSize)
}

func (r *SplitStateReader) Clone(tx kv.TemporalTx) StateReader {
	return NewCommitmentSplitStateReader(r.commitmentReader.Clone(tx), r.plainStateReader.Clone(tx), r.withHistory)
}

// CloneForWorker propagates the worker clone to sub-readers so an embedded
// LatestStateReader (the commitment reader) meters into the per-worker
// accumulator instead of the shared one.
func (r *SplitStateReader) CloneForWorker(workerCtx context.Context, tx kv.TemporalTx) StateReader {
	return NewCommitmentSplitStateReader(r.commitmentReader.CloneForWorker(workerCtx, tx), r.plainStateReader.CloneForWorker(workerCtx, tx), r.withHistory)
}

func NewCommitmentSplitStateReader(commitmentReader StateReader, plainStateReader StateReader, withHistory bool) *SplitStateReader {
	return &SplitStateReader{
		commitmentReader: commitmentReader,
		plainStateReader: plainStateReader,
		withHistory:      withHistory,
	}
}

type CommitmentReplayStateReader struct {
	*SplitStateReader
}

func NewCommitmentReplayStateReader(ttx, tx kv.TemporalTx, tsd sd, plainStateAsOf uint64) *CommitmentReplayStateReader {
	return &CommitmentReplayStateReader{
		NewCommitmentSplitStateReader(NewLatestStateReader(ttx, tsd), NewHistoryStateReader(tx, plainStateAsOf), false),
	}
}

// txLatestReader reads a domain's latest state straight from a pinned RO tx via
// tx.GetLatest, bypassing any SharedDomains in-memory batch or aggregator-shared
// branch cache. The head-capture build's own commitment fold mutates that shared
// cache, so a SharedDomains-backed latest reader would observe post-state branches;
// reading the pinned snapshot directly keeps the parent(B) commitment plane clean.
type txLatestReader struct {
	tx kv.TemporalTx
}

func (r *txLatestReader) WithHistory() bool                           { return false }
func (r *txLatestReader) CheckDataAvailable(kv.Domain, kv.Step) error { return nil }

func (r *txLatestReader) Read(d kv.Domain, plainKey []byte, stepSize uint64) ([]byte, kv.Step, error) {
	enc, step, err := r.tx.GetLatest(d, plainKey)
	if err != nil {
		return nil, 0, fmt.Errorf("txLatestReader(GetLatest) %q: %w", d, err)
	}
	return enc, step, nil
}

// Clone/CloneForWorker keep reading the pinned snapshot: the tx passed by
// warmup callers targets a different (compute) database and would read empty
// parent commitment. The witness build runs sequential commitment, so these
// are not exercised on the hot path, but preserving the pinned tx is correct.
func (r *txLatestReader) Clone(kv.TemporalTx) StateReader { return &txLatestReader{tx: r.tx} }
func (r *txLatestReader) CloneForWorker(context.Context, kv.TemporalTx) StateReader {
	return &txLatestReader{tx: r.tx}
}

// NewHeadCaptureStateReader composes the dual-tx reader used by minimal-node
// witness head-capture collapse detection. The CommitmentDomain resolves from
// pinnedParentTx's latest state read directly (the parent(B) commitment plane held
// by a pinned RO snapshot, the only source a minimal node has for parent commitment)
// while account/storage/code resolve from committedTx's history at plainStateAsOf.
// withHistory=false so the collapse-detection fold's PutBranch calls accumulate
// branches in the build's own in-memory batch (discarded on Close, never flushed).
func NewHeadCaptureStateReader(pinnedParentTx kv.TemporalTx, committedTx kv.TemporalTx, plainStateAsOf uint64) *CommitmentReplayStateReader {
	return newHeadCaptureStateReader(pinnedParentTx, committedTx, plainStateAsOf, false)
}

// NewHeadCaptureTrieStateReader is the head-capture reader for the witness-trie phase:
// it reads identically to NewHeadCaptureStateReader but reports WithHistory()==true so
// the read-only witness-capture fold's PutBranch calls no-op, matching the durable path
// (whose trie phase uses a history reader). Writing branches during capture would corrupt
// the captured node set.
func NewHeadCaptureTrieStateReader(pinnedParentTx kv.TemporalTx, committedTx kv.TemporalTx, plainStateAsOf uint64) *CommitmentReplayStateReader {
	return newHeadCaptureStateReader(pinnedParentTx, committedTx, plainStateAsOf, true)
}

func newHeadCaptureStateReader(pinnedParentTx kv.TemporalTx, committedTx kv.TemporalTx, plainStateAsOf uint64, withHistory bool) *CommitmentReplayStateReader {
	return &CommitmentReplayStateReader{
		NewCommitmentSplitStateReader(
			&txLatestReader{tx: pinnedParentTx},
			NewHistoryStateReader(committedTx, plainStateAsOf),
			withHistory,
		),
	}
}

func (crsr *CommitmentReplayStateReader) Clone(tx kv.TemporalTx) StateReader {
	// commitmentReader (LatestStateReader) gets the new tx so warmup goroutines
	// use a fresh read-only transaction on the temp DB.
	// plainStateReader (HistoryStateReader) keeps its original outer-DB tx:
	// that tx holds the real account/storage history that GetAsOf needs.
	// Replacing it with the temp-DB tx (ttx) would make GetAsOf return empty
	// data and produce wrong post-state roots.
	return &CommitmentReplayStateReader{
		SplitStateReader: NewCommitmentSplitStateReader(
			crsr.commitmentReader.Clone(tx),
			crsr.plainStateReader,
			crsr.withHistory,
		),
	}
}

// CloneForWorker mirrors Clone but meters the commitment (Latest) reader into
// the per-worker accumulator carried by workerCtx.
func (crsr *CommitmentReplayStateReader) CloneForWorker(workerCtx context.Context, tx kv.TemporalTx) StateReader {
	return &CommitmentReplayStateReader{
		SplitStateReader: NewCommitmentSplitStateReader(
			crsr.commitmentReader.CloneForWorker(workerCtx, tx),
			crsr.plainStateReader,
			crsr.withHistory,
		),
	}
}

// RebuildStateReader creates a StateReader for building commitment from scratch, block-by-block.
// Commitment is read from SharedDomains' in-memory batch (LatestStateReader) because we are generating
// it incrementally - prior commitment state lives in the MemBatch, not yet on disk.
// Plain state (acc/storage/code) is read from history since it already exists in DB/files.
//
//   - commitment domain: LatestStateReader via SharedDomains (reads in-memory MemBatch being built)
//   - acc/storage/code:  HistoryStateReader as-of plainStateAsOf (reads persisted plain state)
type RebuildStateReader struct {
	commitmentReader StateReader
	plainStateReader StateReader
	plainStateAsOf   uint64
	sd               sd
}

var _ StateReader = (*RebuildStateReader)(nil)

func NewRebuildStateReader(tx kv.TemporalTx, sharedDomains sd, plainStateAsOf uint64) *RebuildStateReader {
	return &RebuildStateReader{
		commitmentReader: NewLatestStateReader(tx, sharedDomains),
		plainStateReader: NewHistoryStateReader(tx, plainStateAsOf),
		plainStateAsOf:   plainStateAsOf,
		sd:               sharedDomains,
	}
}

func (r *RebuildStateReader) WithHistory() bool {
	// we lie it is without history so we can exercise SharedDomain's in-memory DomainPut(kv.CommitmentDomain)
	return false
}

func (r *RebuildStateReader) CheckDataAvailable(_ kv.Domain, _ kv.Step) error {
	return nil
}

func (r *RebuildStateReader) Read(d kv.Domain, plainKey []byte, stepSize uint64) ([]byte, kv.Step, error) {
	if d == kv.CommitmentDomain {
		return r.commitmentReader.Read(d, plainKey, stepSize)
	}
	return r.plainStateReader.Read(d, plainKey, stepSize)
}

func (r *RebuildStateReader) Clone(tx kv.TemporalTx) StateReader {
	return NewRebuildStateReader(tx, r.sd, r.plainStateAsOf)
}

// CloneForWorker mirrors Clone but the commitment (Latest) reader meters into
// the per-worker accumulator carried by workerCtx.
func (r *RebuildStateReader) CloneForWorker(workerCtx context.Context, tx kv.TemporalTx) StateReader {
	return &RebuildStateReader{
		commitmentReader: NewLatestStateReaderForWorker(workerCtx, tx, r.sd),
		plainStateReader: NewHistoryStateReader(tx, r.plainStateAsOf),
		plainStateAsOf:   r.plainStateAsOf,
		sd:               r.sd,
	}
}
