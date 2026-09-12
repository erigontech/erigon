package blockreplay

import (
	"os"
	"strconv"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/changeset"
)

// witnessReadNanos busy-spins a modelled read latency on every GetLatest that
// falls through to the flat witness. Zero (default) = pure-compute; set
// WITNESS_READ_NANOS to model read-bound parallelism.
var witnessReadNanos = func() int64 {
	if v := os.Getenv("WITNESS_READ_NANOS"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return 0
}()

// witnessMemBatch is the kv.TemporalMemBatch seam SharedDomains reads through,
// backed by a flat witness. Before Seal, DomainPut loads the witness (the
// fixture's pre-state); after Seal, writes forward to the embedded real mem
// batch. GetLatest serves exec writes first, then the read-only witness.
type witnessMemBatch struct {
	kv.TemporalMemBatch
	witness map[kv.Domain]map[string][]byte
	sealed  bool
}

func newWitnessMemBatch(delegate kv.TemporalMemBatch) *witnessMemBatch {
	return &witnessMemBatch{
		TemporalMemBatch: delegate,
		witness:          map[kv.Domain]map[string][]byte{},
	}
}

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

// changesetHolder forwards the concrete mem batch's changeset API: SharedDomains
// type-asserts for these methods, and embedding kv.TemporalMemBatch does not
// promote them.
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
