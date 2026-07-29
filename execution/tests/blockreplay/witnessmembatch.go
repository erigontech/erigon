package blockreplay

import (
	"bytes"
	"fmt"
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
	// writes records the domain keys written after Seal — the replay's actual
	// write-set, used to check the replay wrote exactly the reference key set (an
	// extra or missing write is a correctness bug that commitment-off replay,
	// which validates neither receipts nor trie root, otherwise cannot see).
	writes map[kv.Domain]map[string]struct{}
	sealed bool
}

func newWitnessMemBatch(delegate kv.TemporalMemBatch) *witnessMemBatch {
	return &witnessMemBatch{
		TemporalMemBatch: delegate,
		witness:          map[kv.Domain]map[string][]byte{},
		writes:           map[kv.Domain]map[string]struct{}{},
	}
}

// Seal ends witness loading; subsequent writes go to the delegate.
func (w *witnessMemBatch) Seal() { w.sealed = true }

func (w *witnessMemBatch) recordWrite(domain kv.Domain, k string) {
	m := w.writes[domain]
	if m == nil {
		m = map[string]struct{}{}
		w.writes[domain] = m
	}
	m[k] = struct{}{}
}

func (w *witnessMemBatch) DomainPut(domain kv.Domain, k string, v []byte, txNum uint64, preval []byte) error {
	if w.sealed {
		w.recordWrite(domain, k)
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
		w.recordWrite(domain, k)
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

func (w *witnessMemBatch) HasPrefix(domain kv.Domain, prefix []byte, roTx kv.Tx) ([]byte, []byte, bool, error) {
	keys, err := w.prefixKeys(domain, prefix, roTx)
	if err != nil {
		return nil, nil, false, err
	}
	if len(keys) == 0 {
		return nil, nil, false, nil
	}
	v, _, _ := w.GetLatest(domain, []byte(keys[0]))
	return []byte(keys[0]), v, true, nil
}

func (w *witnessMemBatch) HasPrefixInRAM(domain kv.Domain, prefix []byte) bool {
	if w.TemporalMemBatch.HasPrefixInRAM(domain, prefix) {
		return true
	}
	if m := w.witness[domain]; m != nil {
		p := string(prefix)
		for k := range m {
			if strings.HasPrefix(k, p) {
				if v, _, ok := w.GetLatest(domain, []byte(k)); ok && len(v) > 0 {
					return true
				}
			}
		}
	}
	return false
}

// writeSetDiff reports extra STATE the replay produced that the reference output
// set (want) does not cover: an account/storage/code key the replay wrote that is
// not in want AND whose post-block value actually differs from its pre-block
// witness base. That is the corruption a commitment-off replay (no trie root, no
// receipts) cannot otherwise see. It deliberately does NOT require an exact
// key-set match: the serial reference capture records touched-but-unchanged
// (no-op) writes that the parallel replay skips, so an exact match is infeasible
// and those benign differences must not fail. A deleted account's per-slot
// storage/code clears (recorded only at the account level in want.Deleted) are
// likewise excluded. Commitment/receipt-cache domains are recomputed and ignored.
func (w *witnessMemBatch) writeSetDiff(want *Outputs) []string {
	deleted := map[string]struct{}{}
	for a := range want.Deleted {
		deleted[string(a[:])] = struct{}{}
	}
	wantAcct := map[string]struct{}{}
	for a := range want.Accounts {
		wantAcct[string(a[:])] = struct{}{}
	}
	for a := range want.Deleted {
		wantAcct[string(a[:])] = struct{}{}
	}
	wantCode := map[string]struct{}{}
	for a := range want.Code {
		wantCode[string(a[:])] = struct{}{}
	}
	wantStorage := map[string]struct{}{}
	for a, slots := range want.Storage {
		for k := range slots {
			wantStorage[string(a[:])+string(k[:])] = struct{}{}
		}
	}

	var diffs []string
	// extraStateChanges flags replay writes outside want whose value changed vs
	// the pre-block witness base — real state the reference is missing.
	extraStateChanges := func(kind string, domain kv.Domain, wantKeys map[string]struct{}, deletedPrefixLen int) {
		base := w.witness[domain]
		for k := range w.writes[domain] {
			if _, ok := wantKeys[k]; ok {
				continue // in want — its value is checked by the output diff
			}
			post, _, _ := w.TemporalMemBatch.GetLatest(domain, []byte(k))
			if deletedPrefixLen > 0 && len(k) >= deletedPrefixLen {
				if _, del := deleted[k[:deletedPrefixLen]]; del {
					// Every slot/code write under a self-destructed account must
					// be a tombstone; a surviving live value is a real divergence
					// (CollectOutputs never rescans deleted accounts, so this is
					// the only place it is caught).
					if len(post) > 0 {
						diffs = append(diffs, fmt.Sprintf("live %s write under deleted account: %x", kind, k))
					}
					continue
				}
			}
			if !bytes.Equal(post, base[k]) {
				diffs = append(diffs, fmt.Sprintf("extra %s write changes state: %x", kind, k))
			}
		}
	}
	extraStateChanges("account", kv.AccountsDomain, wantAcct, 0)
	extraStateChanges("code", kv.CodeDomain, wantCode, 20)
	extraStateChanges("storage", kv.StorageDomain, wantStorage, 20)
	sort.Strings(diffs)
	return diffs
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
