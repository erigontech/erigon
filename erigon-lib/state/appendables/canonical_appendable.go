package appendables

import (
	"context"
	"encoding/binary"

	"github.com/erigontech/erigon-lib/kv"
)

// stores only canonical values (either canonicity known beforehand; or unwind removes non-canonical values)
// examples: bor events/spans/milestones/checkpoints; caplin beaconblocks; receipts (only for canonical blocks)
type CanonicalAppendable struct {
	*BaseAppendable
	valsTbl string
	mp      TsNumMapper
}

func NewCanonicalAppendable() *CanonicalAppendable {
	return &CanonicalAppendable{BaseAppendable: &BaseAppendable{}}
}

func (a *CanonicalAppendable) SetTsNumMapper(mapper TsNumMapper) {
	a.mp = mapper
}

func (a *CanonicalAppendable) Get(tsNum uint64, tx kv.Tx) (VVType, error) {
	// first look into snapshots..
	lastTsNum := a.VisibleSegmentsMaxTsNum()
	if tsNum <= lastTsNum {
		if a.baseKeySameAsTsNum {
			// can do binary search or loop over visible segments and find which segment contains tsNum
			// and then get from there
			var v *VisibleSegment
			return v.Get(tsNum)
		} else {
			// TODO: loop over all visible segments and find which segment contains tsNum
		}
	}

	// then db
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, tsNum)
	return tx.GetOne(a.valsTbl, key)
}

func (a *CanonicalAppendable) Put(tsNum uint64, value VVType, tx kv.RwTx) error {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, tsNum)
	return tx.Append(a.valsTbl, key, value)
}

func (a *CanonicalAppendable) Prune(ctx context.Context, baseKeyTo, limit uint64, rwTx kv.RwTx) error {
	// similar to unwind, but with a limit and going reverse
	return nil
}

func (a *CanonicalAppendable) Unwind(ctx context.Context, baseKeyFrom uint64, rwTx kv.RwTx) error {
	stre := a.mp.Get(baseKeyFrom, MaxUint64, rwTx)
	defer stre.Close()
	vkey, err := stre.Next()
	if err != nil {
		return err
	}

	stre.Close()

	c, err := rwTx.Cursor(a.valsTbl)
	if err != nil {
		return err
	}
	defer c.Close()

	firstK, _, err := c.Seek(vkey)
	if err != nil {
		return err
	}
	if firstK == nil {
		return nil
	}

	for k, _, err := c.Current(); k != nil; k, _, err = c.Next() {
		if err != nil {
			return err
		}

		if err := rwTx.Delete(a.valsTbl, k); err != nil {
			return err
		}
	}

	return nil
}
