package blockreplay

import (
	"sort"
	"strings"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/changeset"
)

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
			return v, 0, true
		}
	}
	return nil, 0, false
}

// prefixKeys returns the live keys under prefix in sorted order, merging the
// delegate's entries (exec writes) with the read-only witness pre-state. A key
// present on the delegate shadows the witness at that key; its merged GetLatest
// value wins, and a post-Seal delete (recorded as an empty tombstone) drops the
// key entirely — so a witness storage slot the block cleared is not resurrected.
func (w *witnessMemBatch) prefixKeys(domain kv.Domain, prefix []byte, roTx kv.Tx) ([]string, error) {
	cands := map[string]struct{}{}
	if err := w.TemporalMemBatch.IteratePrefix(domain, prefix, roTx, func(k, v []byte) (bool, error) {
		cands[string(k)] = struct{}{}
		return true, nil
	}); err != nil {
		return nil, err
	}
	if m := w.witness[domain]; m != nil {
		p := string(prefix)
		for k := range m {
			if strings.HasPrefix(k, p) {
				cands[k] = struct{}{}
			}
		}
	}
	keys := make([]string, 0, len(cands))
	for k := range cands {
		if v, _, ok := w.GetLatest(domain, []byte(k)); ok && len(v) > 0 {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys, nil
}

func (w *witnessMemBatch) IteratePrefix(domain kv.Domain, prefix []byte, roTx kv.Tx, it func(k []byte, v []byte) (cont bool, err error)) error {
	keys, err := w.prefixKeys(domain, prefix, roTx)
	if err != nil {
		return err
	}
	for _, k := range keys {
		v, _, _ := w.GetLatest(domain, []byte(k))
		cont, err := it([]byte(k), v)
		if err != nil {
			return err
		}
		if !cont {
			break
		}
	}
	return nil
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
	SetCommitmentDiff(acc *changeset.StateChangeSet)
	SetCommitmentDiffRaw(d *kv.DomainDiff)
	CommitmentDiff() *kv.DomainDiff
	PutCommitmentBranchDiff(k string, v []byte, txNum uint64, preval []byte, diff *kv.DomainDiff) error
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

func (w *witnessMemBatch) SetCommitmentDiff(acc *changeset.StateChangeSet) {
	w.TemporalMemBatch.(changesetHolder).SetCommitmentDiff(acc)
}

func (w *witnessMemBatch) SetCommitmentDiffRaw(d *kv.DomainDiff) {
	w.TemporalMemBatch.(changesetHolder).SetCommitmentDiffRaw(d)
}

func (w *witnessMemBatch) CommitmentDiff() *kv.DomainDiff {
	return w.TemporalMemBatch.(changesetHolder).CommitmentDiff()
}

func (w *witnessMemBatch) PutCommitmentBranchDiff(k string, v []byte, txNum uint64, preval []byte, diff *kv.DomainDiff) error {
	return w.TemporalMemBatch.(changesetHolder).PutCommitmentBranchDiff(k, v, txNum, preval, diff)
}
