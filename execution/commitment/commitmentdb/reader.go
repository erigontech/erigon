package commitmentdb

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon/db/kv"
)

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
	enc, _, err = r.roTx.GetAsOf(d, plainKey, r.limitReadAsOfTxNum)
	if err != nil {
		return enc, 0, fmt.Errorf("HistoryStateReader(GetAsOf) %q: (limitTxNum=%d): %w", d, r.limitReadAsOfTxNum, err)
	}
	return enc, kv.Step(r.limitReadAsOfTxNum / stepSize), nil
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

// CloneForWorker: files-only reads go straight to the tx (no shared metrics
// accumulator), so it's identical to Clone.
func (r *FilesOnlyStateReader) CloneForWorker(_ context.Context, tx kv.TemporalTx) StateReader {
	return NewFilesOnlyStateReader(tx, r.limitTxNum)
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
