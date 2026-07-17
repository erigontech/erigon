package stagedsync

import (
	"bytes"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/changeset"
)

// prevValueReader supplies the pre-block (first-touch) value of a domain key as
// raw stored bytes, or nil when absent. Only the first write to a key in a block
// consults it; later writes chain their prev in memory.
type prevValueReader interface {
	prevValue(domain kv.Domain, key []byte, txNum uint64) ([]byte, error)
}

// changesetBuilder reconstructs a block's per-domain changeset (the ChangeSets3
// entries) from the per-tx write stream, mirroring the domain-write recording so
// the bytes are identical to exec's: prev values chain in memory (first touch
// read as-of pre-block via the reader), no-op puts are skipped, and entries are
// keyed by step = txNum/stepSize. It is a generic encoder — domain-specific
// choices (code-delete-when-absent, account self-destruct cascade) are applied by
// the caller before record; the builder just recreates the DomainUpdate stream.
type changesetBuilder struct {
	reader   prevValueReader
	stepSize uint64
	cs       *changeset.StateChangeSet
	running  [kv.DomainLen]map[string][]byte
	firstErr error
}

func newChangesetBuilder(reader prevValueReader, stepSize uint64) *changesetBuilder {
	b := &changesetBuilder{
		reader:   reader,
		stepSize: stepSize,
		cs:       &changeset.StateChangeSet{},
	}
	for i := range b.running {
		b.running[i] = map[string][]byte{}
	}
	return b
}

// record folds one domain write into the changeset. newVal == nil is a delete;
// a non-nil newVal is a put (skipped when it equals the current value, exactly as
// SharedDomains.DomainPut does). txNum is the write's tx position; the entry is
// keyed by step = txNum/stepSize so a block straddling a step edge yields one
// entry per step for the same key.
func (b *changesetBuilder) record(domain kv.Domain, key []byte, newVal []byte, txNum uint64) {
	ks := string(key)
	running := b.running[domain]
	prev, seen := running[ks]
	if !seen {
		p, err := b.reader.prevValue(domain, key, txNum)
		if err != nil {
			if b.firstErr == nil {
				b.firstErr = err
			}
			return
		}
		prev = p
	}

	// Put whose value already matches the current value is a no-op: no history
	// entry, and the running value is unchanged.
	if newVal != nil && bytes.Equal(prev, newVal) {
		running[ks] = prev
		return
	}

	// A code delete with no prior value is a no-op (nothing to restore): mirrors
	// SharedDomains.DomainDel, which returns early for CodeDomain when prevVal is
	// nil. Other domains record the delete even against an absent prev.
	if newVal == nil && domain == kv.CodeDomain && len(prev) == 0 {
		running[ks] = nil
		return
	}

	step := kv.Step(txNum / b.stepSize)
	b.cs.Diffs[domain].DomainUpdate(key, step, prev)

	if newVal == nil {
		running[ks] = nil
	} else {
		running[ks] = common.Copy(newVal)
	}
}

func (b *changesetBuilder) err() error { return b.firstErr }

func (b *changesetBuilder) result() *changeset.StateChangeSet { return b.cs }
