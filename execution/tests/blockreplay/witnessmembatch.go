package blockreplay

import (
	"os"
	"strconv"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/changeset"
)

// witnessReadNanos busy-spins a modelled cold-domain/file read latency on every
// GetLatest that falls through to the flat witness (i.e. a versionMap/mem miss —
// the reads that cost ~90µs in production). Zero = pure-compute (default). Set
// WITNESS_READ_NANOS to model read-bound parallelism (the production case, where
// workers overlap IO). Env-gated so default behaviour is unchanged.
var witnessReadNanos = func() int64 {
	if v := os.Getenv("WITNESS_READ_NANOS"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return 0
}()

// witnessMemBatch is the kv.TemporalMemBatch seam SharedDomains reads through,
// backed by a flat witness instead of a temporal source. Before Seal, DomainPut
// loads the witness (the fixture's pre-state, encoded by the production Writer);
// after Seal, writes and every other operation forward to an embedded real mem
// batch. GetLatest serves exec writes first, then the read-only witness — so a
// complete witness never falls through to the underlying tx.
type witnessMemBatch struct {
	kv.TemporalMemBatch // delegate: exec writes + all non-overridden methods
	witness             map[kv.Domain]map[string][]byte
	sealed              bool
}

func newWitnessMemBatch(delegate kv.TemporalMemBatch) *witnessMemBatch {
	return &witnessMemBatch{
		TemporalMemBatch: delegate,
		witness:          map[kv.Domain]map[string][]byte{},
	}
}

// Seal ends witness loading; subsequent writes go to the delegate.
func (w *witnessMemBatch) Seal() { w.sealed = true }

func (w *witnessMemBatch) DomainPut(domain kv.Domain, k string, v []byte, txNum uint64, preval []byte) error {
	if w.sealed {
		return w.TemporalMemBatch.DomainPut(domain, k, v, txNum, preval)
	}
	m := w.witness[domain]
	if m == nil {
		m = map[string][]byte{}
		w.witness[domain] = m
	}
	m[k] = append([]byte(nil), v...)
	return nil
}

func (w *witnessMemBatch) DomainDel(domain kv.Domain, k string, txNum uint64, preval []byte) error {
	if w.sealed {
		return w.TemporalMemBatch.DomainDel(domain, k, txNum, preval)
	}
	if m := w.witness[domain]; m != nil {
		delete(m, k)
	}
	return nil
}

func (w *witnessMemBatch) GetLatest(domain kv.Domain, key []byte) ([]byte, kv.Step, bool) {
	if v, step, ok := w.TemporalMemBatch.GetLatest(domain, key); ok {
		return v, step, true
	}
	if m := w.witness[domain]; m != nil {
		if v, ok := m[string(key)]; ok {
			spin(witnessReadNanos)
			return v, 0, true
		}
	}
	return nil, 0, false
}

// changesetHolder is the structural view of the concrete mem batch's changeset
// API. SharedDomains does unguarded sd.mem.(accHolder) assertions for changeset
// bookkeeping; embedding the interface does not promote these (they are not part
// of kv.TemporalMemBatch), so forward them explicitly to the delegate.
type changesetHolder interface {
	GetChangesetByBlockNum(blockNumber uint64) (common.Hash, *changeset.StateChangeSet)
	GetChangesetByHash(blockNumber uint64, blockHash common.Hash) *changeset.StateChangeSet
	GetChangesetAccumulator() *changeset.StateChangeSet
	SetChangesetAccumulator(acc *changeset.StateChangeSet)
	SavePastChangesetAccumulator(blockHash common.Hash, blockNumber uint64, acc *changeset.StateChangeSet)
}

func (w *witnessMemBatch) GetChangesetByBlockNum(blockNumber uint64) (common.Hash, *changeset.StateChangeSet) {
	return w.TemporalMemBatch.(changesetHolder).GetChangesetByBlockNum(blockNumber)
}
func (w *witnessMemBatch) GetChangesetByHash(blockNumber uint64, blockHash common.Hash) *changeset.StateChangeSet {
	return w.TemporalMemBatch.(changesetHolder).GetChangesetByHash(blockNumber, blockHash)
}
func (w *witnessMemBatch) GetChangesetAccumulator() *changeset.StateChangeSet {
	return w.TemporalMemBatch.(changesetHolder).GetChangesetAccumulator()
}
func (w *witnessMemBatch) SetChangesetAccumulator(acc *changeset.StateChangeSet) {
	w.TemporalMemBatch.(changesetHolder).SetChangesetAccumulator(acc)
}
func (w *witnessMemBatch) SavePastChangesetAccumulator(blockHash common.Hash, blockNumber uint64, acc *changeset.StateChangeSet) {
	w.TemporalMemBatch.(changesetHolder).SavePastChangesetAccumulator(blockHash, blockNumber, acc)
}
