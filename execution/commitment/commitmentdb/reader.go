package commitmentdb

import (
	"fmt"

	"github.com/erigontech/erigon/db/kv"
)

type StateReader interface {
	WithHistory() bool
	CheckDataAvailable(d kv.Domain, step kv.Step) error
	Read(d kv.Domain, plainKey []byte, stepSize uint64) (enc []byte, step kv.Step, err error)
	Clone(tx kv.TemporalTx) StateReader
}

type LatestStateReader struct {
	sharedDomains sd
	getter        kv.TemporalGetter
}

func NewLatestStateReader(tx kv.TemporalTx, sd sd) *LatestStateReader {
	return &LatestStateReader{sharedDomains: sd, getter: sd.AsGetter(tx)}
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
	enc, step, err = r.getter.GetLatest(d, plainKey)
	if err != nil {
		return nil, 0, fmt.Errorf("LatestStateReader(GetLatest) %q: %w", d, err)
	}
	return enc, step, nil
}

func (r *LatestStateReader) Clone(tx kv.TemporalTx) StateReader {
	return NewLatestStateReader(tx, r.sharedDomains)
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

func (r *HistoryStateReader) Clone(tx kv.TemporalTx) StateReader {
	return NewHistoryStateReader(tx, r.limitReadAsOfTxNum)
}

// LimitedHistoryStateReader reads from *limited* (i.e. *without-recent-files*) state at specified txNum, otherwise from *latest*.
// `limitReadAsOfTxNum` here is used for unusual operation: "hide recent .kv files and read the latest state from files".
type LimitedHistoryStateReader struct {
	HistoryStateReader
	sharedDomains sd
	getter        kv.TemporalGetter
}

func NewLimitedHistoryStateReader(roTx kv.TemporalTx, sd sd, limitReadAsOfTxNum uint64) *LimitedHistoryStateReader {
	return &LimitedHistoryStateReader{
		HistoryStateReader: HistoryStateReader{
			roTx:               roTx,
			limitReadAsOfTxNum: limitReadAsOfTxNum,
		},
		sharedDomains: sd,
		getter:        sd.AsGetter(roTx),
	}
}

func (r *LimitedHistoryStateReader) WithHistory() bool {
	return false
}

// Reason why we have `kv.TemporalDebugTx.GetLatestFromFiles' call here: `state.RebuildCommitmentFiles` can build commitment.kv from account.kv.
// Example: we have account.0-16.kv and account.16-18.kv, let's generate commitment.0-16.kv => it means we need to make account.16-18.kv invisible
// and then read "latest state" like there is no account.16-18.kv
func (r *LimitedHistoryStateReader) Read(d kv.Domain, plainKey []byte, stepSize uint64) (enc []byte, step kv.Step, err error) {
	var ok bool
	var endTxNum uint64
	// reading from domain files this way will dereference domain key correctly,
	// GetAsOf itself does not dereference keys in commitment domain values
	enc, ok, _, endTxNum, err = r.roTx.Debug().GetLatestFromFiles(d, plainKey, r.limitReadAsOfTxNum)
	if err != nil {
		return nil, 0, fmt.Errorf("LimitedHistoryStateReader(GetLatestFromFiles) %q: (limitTxNum=%d): %w", d, r.limitReadAsOfTxNum, err)
	}
	if !ok {
		enc = nil
	} else {
		step = kv.Step(endTxNum / stepSize)
	}
	if enc == nil {
		enc, step, err = r.getter.GetLatest(d, plainKey)
		if err != nil {
			return nil, 0, fmt.Errorf("LimitedHistoryStateReader(GetLatest) %q: %w", d, err)
		}
	}
	return enc, step, nil
}

func (r *LimitedHistoryStateReader) Clone(tx kv.TemporalTx) StateReader {
	return NewLimitedHistoryStateReader(tx, r.sharedDomains, r.limitReadAsOfTxNum)
}

// SplitStateReader implements commitmentdb.StateReader using (potentially) different state readers for commitment
// data and account/storage/code data.
type SplitStateReader struct {
	commitmentReader StateReader
	plainStateReader StateReader
	withHistory      bool
}

var _ StateReader = (*SplitStateReader)(nil)

func (r *SplitStateReader) WithHistory() bool {
	return r.withHistory
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
			false,
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

// SimulationPlainStateReader is the plain-state half of the reader used by eth_simulateV1 when
// computing the commitment hash for a simulated block on top of a frozen (historical) parent block.
//
// Reads are routed as follows:
//   - If the key is in the shared-domains mem batch (dirty — modified by the simulation):
//     returns the post-simulation value directly from the mem batch.
//   - Otherwise (clean — unmodified sibling accounts):
//     returns the historical value via GetAsOf at limitReadAsOfTxNum, so the trie
//     reflects the canonical state at the parent block rather than the node's current
//     latest DB state (which varies with sync progress and causes non-determinism).
type SimulationPlainStateReader struct {
	sd                 sd
	histReader         *HistoryStateReader
	limitReadAsOfTxNum uint64
}

func NewSimulationPlainStateReader(roTx kv.TemporalTx, sd sd, limitReadAsOfTxNum uint64) *SimulationPlainStateReader {
	return &SimulationPlainStateReader{
		sd:                 sd,
		histReader:         NewHistoryStateReader(roTx, limitReadAsOfTxNum),
		limitReadAsOfTxNum: limitReadAsOfTxNum,
	}
}

func (r *SimulationPlainStateReader) WithHistory() bool { return true }

func (r *SimulationPlainStateReader) CheckDataAvailable(kv.Domain, kv.Step) error { return nil }

func (r *SimulationPlainStateReader) Read(d kv.Domain, plainKey []byte, stepSize uint64) (enc []byte, step kv.Step, err error) {
	// Check the mem batch first: if the account is dirty (modified by simulation), use
	// the post-simulation value so leaf hashes are computed with the correct new state.
	if v, s, ok := r.sd.MemGetLatest(d, plainKey); ok {
		return v, s, nil
	}
	// Clean account (not touched by simulation): use the historical value so sibling
	// branch hashes are computed from the canonical parent-block state, not from the
	// node's current latest DB (which differs across nodes and causes non-determinism).
	return r.histReader.Read(d, plainKey, stepSize)
}

func (r *SimulationPlainStateReader) Clone(tx kv.TemporalTx) StateReader {
	return NewSimulationPlainStateReader(tx, r.sd, r.limitReadAsOfTxNum)
}

// SimulationStateReader is the unified state reader used by eth_simulateV1 for ALL domains
// (CommitmentDomain, AccountsDomain, StorageDomain, CodeDomain) when computing the
// commitment hash for simulated blocks on top of a frozen (historical) parent block.
//
// WithHistory returns false so that PutBranch writes modified trie branches to sd.mem.
// This is essential for multi-block simulations: block N's branch modifications are
// persisted to sd.mem and can be read back by block N+1, so the trie is consistent
// across simulated blocks without writing to the canonical DB.
//
// Read routing:
//   - Key is in sd.mem (dirty — modified by any prior simulated block or the current one):
//     returns the post-simulation value from the in-memory batch.
//   - CommitmentDomain (clean — trie branch not in sd.mem):
//     GetAsOf at commitmentAsOfTxNum (= canonical base-parent commitment).
//   - Other domains (clean — account/storage/code not touched by any simulation):
//     GetAsOf at plainStateAsOfTxNum (= canonical base-parent account state).
type SimulationStateReader struct {
	sd                   sd
	commitmentReader     *HistoryStateReader
	plainStateReader     *HistoryStateReader
	commitmentAsOfTxNum  uint64
	plainStateAsOfTxNum  uint64
}

func NewSimulationStateReader(roTx kv.TemporalTx, sd sd, commitmentAsOfTxNum, plainStateAsOfTxNum uint64) *SimulationStateReader {
	return &SimulationStateReader{
		sd:                  sd,
		commitmentReader:    NewHistoryStateReader(roTx, commitmentAsOfTxNum),
		plainStateReader:    NewHistoryStateReader(roTx, plainStateAsOfTxNum),
		commitmentAsOfTxNum: commitmentAsOfTxNum,
		plainStateAsOfTxNum: plainStateAsOfTxNum,
	}
}

// WithHistory returns false so that PutBranch writes modified trie branches to sd.mem,
// making them available for subsequent simulated blocks.
func (r *SimulationStateReader) WithHistory() bool { return false }

func (r *SimulationStateReader) CheckDataAvailable(kv.Domain, kv.Step) error { return nil }

func (r *SimulationStateReader) Read(d kv.Domain, plainKey []byte, stepSize uint64) (enc []byte, step kv.Step, err error) {
	// Check sd.mem first: if the key was modified by any prior simulated block or by
	// the current simulation, use that value (accounts, storage, or trie branches).
	if v, s, ok := r.sd.MemGetLatest(d, plainKey); ok {
		return v, s, nil
	}
	// Not in sd.mem (clean). For CommitmentDomain branches, read from the canonical
	// base-parent commitment; for other domains, read from the canonical base-parent
	// account/storage state.
	if d == kv.CommitmentDomain {
		return r.commitmentReader.Read(d, plainKey, stepSize)
	}
	return r.plainStateReader.Read(d, plainKey, stepSize)
}

func (r *SimulationStateReader) Clone(tx kv.TemporalTx) StateReader {
	return NewSimulationStateReader(tx, r.sd, r.commitmentAsOfTxNum, r.plainStateAsOfTxNum)
}

// A history reader that reads:
//   - commitment data as-of  commitmentAsOf txnum
//   - account/storage/code data as-of plainsStateAsOf txnum
func NewSplitHistoryReader(tx kv.TemporalTx, commitmentAsOf uint64, plainStateAsOf uint64, withHistory bool) *SplitStateReader {
	return &SplitStateReader{
		commitmentReader: NewHistoryStateReader(tx, commitmentAsOf),
		plainStateReader: NewHistoryStateReader(tx, plainStateAsOf),
		withHistory:      withHistory,
	}
}
