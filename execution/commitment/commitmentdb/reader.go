package commitmentdb

import (
	"fmt"

	"github.com/erigontech/erigon/db/kv"
)

type StateReader interface {
	WithHistory() bool
	CheckDataAvailable(d kv.Domain, step kv.Step) error
	Read(d kv.Domain, plainKey []byte, stepSize uint64) (enc []byte, step kv.Step, err error)
	Clone(tx kv.TemporalTx, getter kv.TemporalGetter) StateReader
}

type LatestStateReader struct {
	getter kv.TemporalGetter
}

func NewLatestStateReader(tx kv.TemporalGetter) *LatestStateReader {
	return &LatestStateReader{getter: tx}
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

func (r *LatestStateReader) Clone(_ kv.TemporalTx, getter kv.TemporalGetter) StateReader {
	return NewLatestStateReader(getter)
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

func (r *HistoryStateReader) Clone(tx kv.TemporalTx, _ kv.TemporalGetter) StateReader {
	return NewHistoryStateReader(tx, r.limitReadAsOfTxNum)
}

// LimitedHistoryStateReader reads from *limited* (i.e. *without-recent-files*) state at specified txNum, otherwise from *latest*.
// `limitReadAsOfTxNum` here is used for unusual operation: "hide recent .kv files and read the latest state from files".
type LimitedHistoryStateReader struct {
	HistoryStateReader
	getter kv.TemporalGetter
}

func NewLimitedHistoryStateReader(roTx kv.TemporalTx, getter kv.TemporalGetter, limitReadAsOfTxNum uint64) *LimitedHistoryStateReader {
	return &LimitedHistoryStateReader{
		HistoryStateReader: HistoryStateReader{
			roTx:               roTx,
			limitReadAsOfTxNum: limitReadAsOfTxNum,
		},
		getter: getter,
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

func (r *LimitedHistoryStateReader) Clone(tx kv.TemporalTx, getter kv.TemporalGetter) StateReader {
	return NewLimitedHistoryStateReader(tx, getter, r.limitReadAsOfTxNum)
}

// splitStateReader implements commitmentdb.StateReader using (potentially) different state readers for commitment
// data and account/storage/code data.
type splitStateReader struct {
	commitmentReader StateReader
	plainStateReader StateReader
	withHistory      bool
}

var _ StateReader = (*splitStateReader)(nil)

func (r splitStateReader) WithHistory() bool {
	return r.withHistory
}

func (r splitStateReader) CheckDataAvailable(_ kv.Domain, _ kv.Step) error {
	return nil
}

func (r splitStateReader) Read(d kv.Domain, plainKey []byte, stepSize uint64) ([]byte, kv.Step, error) {
	if d == kv.CommitmentDomain {
		return r.commitmentReader.Read(d, plainKey, stepSize)
	}
	return r.plainStateReader.Read(d, plainKey, stepSize)
}

func (r splitStateReader) Clone(tx kv.TemporalTx, getter kv.TemporalGetter) StateReader {
	return NewCommitmentSplitStateReader(r.commitmentReader.Clone(tx, getter), r.plainStateReader.Clone(tx, getter), r.withHistory)
}

func NewCommitmentSplitStateReader(commitmentReader StateReader, plainStateReader StateReader, withHistory bool) StateReader {
	return splitStateReader{
		commitmentReader: commitmentReader,
		plainStateReader: plainStateReader,
		withHistory:      withHistory,
	}
}

func NewCommitmentReplayStateReader(tx kv.TemporalTx, getter kv.TemporalGetter, plainStateAsOf uint64) StateReader {
	// Claim that during replay we do not operate on history, so we can temporarily save commitment state
	return NewCommitmentSplitStateReader(NewLatestStateReader(getter), NewHistoryStateReader(tx, plainStateAsOf), false)
}
