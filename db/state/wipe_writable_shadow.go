// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package state

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
)

// WipeWritableShadowPast clears every writable-domain MDBX entry whose
// txnum coordinate > lastTxNum. The post-state contract is per-tx, not
// per-step: the last tx surviving the wipe in any writable-shadow
// table is exactly lastTxNum (= the last txnum of toBlock), regardless
// of whether lastTxNum happens to coincide with a step boundary.
//
// Mode-B admin SetHead (docs/plans/20260525-admin-sethead-unwind-design.md,
// sub-op #2) calls this after snapshot files past toBlock have been
// trimmed. With the per-tx contract above, the MDBX writable-domain
// shadow holds nothing newer than lastTxNum, so the next forward
// execution sees the snapshot files as the authoritative state and
// no DB-shadow override of a write past toBlock survives.
//
// Algorithm — two phases, per writable domain (accounts, storage, code,
// commitment):
//
//  1. **Whole-step wipe past the boundary step.**
//     Wipe ValuesTable entries with step > stepContaining (where
//     stepContaining = lastTxNum / stepSize, the step that contains
//     lastTxNum). These steps cover only txnums > lastTxNum.
//
//  2. **Boundary-step diff-replay.**
//     The writable shadow's per-entry encoding is step-keyed only — no
//     per-txnum granularity. Within step stepContaining, a key's stored
//     value reflects the latest write to that key in that step,
//     whichever txnum that was. For aligned cuts (lastTxNum is the
//     LAST txnum of step stepContaining), every write within the step
//     is at txnum ≤ lastTxNum, so the existing values are correct and
//     no replay is needed. For non-aligned cuts, the value may have
//     been written at txnum > lastTxNum and so reflects "the future"
//     relative to our new tip; we must restore it to its
//     as-of-lastTxNum form.
//
//     We compute the state diff for the boundary step from history:
//     iterate HistoryKeyTxNumRange(d, lastTxNum+1, boundaryStepEnd),
//     and for each key found there, call GetAsOf(d, key, lastTxNum+1)
//     (ts is exclusive so +1 to include lastTxNum's own writes). The
//     result is written directly into the shadow via cursor Put/Delete
//     (see applyReplay) — the (key, target) snapshot is collected
//     BEFORE history.prune runs in this same call, since prune would
//     otherwise wipe the records GetAsOf reads.
//
//     For aligned cuts the iterator range is empty (lastTxNum+1 ==
//     boundaryStepEnd) so phase 2 is a no-op and we keep the aligned
//     fast path.
//
//  3. **History + standalone-II prune.** Existing prune machinery with
//     txFrom = lastTxNum+1, txTo = math.MaxUint64.
//
// Preconditions enforced inline:
//
//   - StepSize() > 0.
//
// Data dependency for the non-aligned path: history files (.v) for the
// affected domains must cover (lastTxNum, boundaryStepEnd]. If history
// is locally absent for that range, GetAsOf returns wrong values and
// the wipe corrupts the shadow. Mode B's caller is responsible for
// ensuring history is present (today: assumes minimal-prune locality;
// follow-up CWork.1 will trigger on-demand download as an extension of
// the existing fork-unwind temp-download pattern).
//
// Caller owns tx lifecycle and the commit. WipeWritableShadowPast does
// not commit and does not flush MDBX.
func (a *Aggregator) WipeWritableShadowPast(ctx context.Context, tx kv.TemporalRwTx, lastTxNum uint64) error {
	stepSize := a.StepSize()
	if stepSize == 0 {
		return fmt.Errorf("WipeWritableShadowPast: StepSize() == 0")
	}

	stepContaining := lastTxNum / stepSize
	stepBoundary := stepContaining + 1
	boundaryStepEndTxNum := stepBoundary * stepSize
	isAligned := lastTxNum+1 == boundaryStepEndTxNum

	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	atRo := a.BeginFilesRo()
	defer atRo.Close()

	// Domain coverage for the wipe falls into two groups:
	//
	//   - diffReplayDomains: history-tracked domains where we can
	//     pull as-of-lastTxNum values for each touched key in the
	//     boundary step. Accounts/storage/code (always) + receipts
	//     (history-enabled per config) get this treatment.
	//   - wholeStepWipeDomains: domains where per-txnum replay isn't
	//     possible — either because HistoryDisabled (commitment) or
	//     because the IiCfg is disabled by default (rcache). The
	//     boundary step is wiped entirely; the caller is expected to
	//     repopulate (for commitment, via Provider.Unwind's
	//     ensureCommitmentAtBlockApply step; for rcache, naturally
	//     rebuilt by subsequent exec).
	diffReplayDomains := []kv.Domain{kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain, kv.ReceiptDomain}
	wholeStepWipeDomains := []kv.Domain{kv.CommitmentDomain, kv.RCacheDomain}
	allWritableDomains := append([]kv.Domain{}, diffReplayDomains...)
	allWritableDomains = append(allWritableDomains, wholeStepWipeDomains...)

	// For non-aligned cuts: snapshot the per-domain (key, target-value)
	// pairs for the boundary-step replay BEFORE the history prune below
	// removes the history records we need to read. GetAsOf consults the
	// history index + values, both of which the prune phase wipes for
	// txnums > lastTxNum.
	type replayPlan struct {
		domain kv.Domain
		keys   [][]byte
		target [][]byte
	}
	var plans []replayPlan
	if !isAligned {
		for _, name := range diffReplayDomains {
			keys, err := collectKeysChangedInRange(tx, name, lastTxNum+1, boundaryStepEndTxNum)
			if err != nil {
				return fmt.Errorf("WipeWritableShadowPast: collect replay keys (%s): %w", name, err)
			}
			if len(keys) == 0 {
				continue
			}
			targets := make([][]byte, len(keys))
			for i, k := range keys {
				v, _, gerr := tx.GetAsOf(name, k, lastTxNum+1)
				if gerr != nil {
					return fmt.Errorf("WipeWritableShadowPast: GetAsOf(%s, key=%x, ts=%d): %w", name, k, lastTxNum+1, gerr)
				}
				if len(v) > 0 {
					targets[i] = append([]byte(nil), v...)
				}
			}
			plans = append(plans, replayPlan{domain: name, keys: keys, target: targets})
		}
	}

	// Per-domain wipe (step > stepBoundary, i.e. strictly past the
	// boundary step) + history prune past lastTxNum. Applies to all
	// writable domains.
	for _, name := range allWritableDomains {
		d := a.d[name]
		dt := atRo.d[name]

		if err := wipeDomainValuesPastStep(tx, d, stepBoundary); err != nil {
			return fmt.Errorf("WipeWritableShadowPast: domain=%s ValuesTable: %w", d.FilenameBase, err)
		}

		if _, err := dt.ht.prune(ctx, tx, lastTxNum+1, math.MaxUint64, math.MaxUint64, true, logEvery); err != nil {
			return fmt.Errorf("WipeWritableShadowPast: domain=%s history prune: %w", d.FilenameBase, err)
		}
	}

	// Whole-step wipe at stepContaining for non-replayable domains.
	// Commitment branches written by forward exec during the boundary
	// step would otherwise survive the per-step wipe above (they're
	// AT stepBoundary-1, not past it) and corrupt post-mode-B trie
	// reads (caught live as G3.15). RCacheDomain gets the same
	// treatment for consistency — its IiCfg.Disable=true default
	// means per-txnum replay returns nothing anyway.
	for _, name := range wholeStepWipeDomains {
		d := a.d[name]
		if err := wipeDomainValuesAtStep(tx, d, stepContaining); err != nil {
			return fmt.Errorf("WipeWritableShadowPast: domain=%s boundary-step wipe: %w", d.FilenameBase, err)
		}
	}

	// Boundary-step diff-replay for history-tracked domains.
	if !isAligned {
		var stepBytes [8]byte
		binary.BigEndian.PutUint64(stepBytes[:], ^stepContaining)
		for _, p := range plans {
			d := a.d[p.domain]
			if err := applyReplay(tx, d, p.keys, p.target, stepBytes); err != nil {
				return fmt.Errorf("WipeWritableShadowPast: apply boundary-step replay (%s): %w", d.FilenameBase, err)
			}
		}
	}

	for _, iit := range atRo.standaloneIIs() {
		if err := iit.unwind(ctx, tx, lastTxNum+1, math.MaxUint64, math.MaxUint64, logEvery, true); err != nil {
			return fmt.Errorf("WipeWritableShadowPast: II=%s unwind: %w", iit.ii.FilenameBase, err)
		}
	}

	return nil
}

// collectKeysChangedInRange returns the deduped set of keys with at
// least one history record at txnum in [fromTs, toTs).
//
// Must be called BEFORE history.prune runs for the same range —
// post-prune the history index doesn't carry the records we need.
func collectKeysChangedInRange(tx kv.TemporalRwTx, name kv.Domain, fromTs, toTs uint64) ([][]byte, error) {
	it, err := tx.Debug().HistoryKeyTxNumRange(name, int(fromTs), int(toTs), order.Asc, -1)
	if err != nil {
		return nil, fmt.Errorf("HistoryKeyTxNumRange(%s, [%d, %d)): %w", name, fromTs, toTs, err)
	}
	defer it.Close()

	seen := map[string]struct{}{}
	var out [][]byte
	for it.HasNext() {
		k, _, terr := it.Next()
		if terr != nil {
			return nil, fmt.Errorf("HistoryKeyTxNumRange next(%s): %w", name, terr)
		}
		kStr := string(k)
		if _, dup := seen[kStr]; dup {
			continue
		}
		seen[kStr] = struct{}{}
		kCopy := make([]byte, len(k))
		copy(kCopy, k)
		out = append(out, kCopy)
	}
	return out, nil
}

// applyReplay rewrites the writable-shadow ValuesTable entries for
// `keys` at the boundary step so that each entry holds its
// as-of-lastTxNum value (or is deleted if the as-of value is empty).
// `targets` are pre-computed by GetAsOf before the history prune
// destroys the records GetAsOf would read.
//
//   - LargeValues=true: ValuesTable key = fullKey + stepBytes.
//     Empty target → tx.Delete; non-empty → tx.Put.
//   - LargeValues=false (DupSort): primary key = fullKey, value =
//     stepBytes + raw. Walk via cursor SeekBothRange(fullKey, stepBytes);
//     if a dup at exactly this step exists, DeleteCurrent; if target
//     is non-empty, Put(fullKey, stepBytes + target).
//
// Direct cursor ops bypass the DomainBufferedWriter / TemporalMemBatch
// path — that path is built for forward-execution write batching and
// for DupSort it can leave the existing dup intact alongside the new
// one (Put adds when any byte past the step prefix differs, and the
// buffered writer's Flush PutCurrent path doesn't always land on the
// right cursor position for a value-prefix-only delete). The direct
// cursor approach mirrors wipeDomainValuesPastStep's proven semantics.
//
// History is not touched here: WipeWritableShadowPast's history.prune
// (run after key collection, before this call) already removes records
// past lastTxNum; the writable shadow we just rewrote is the only thing
// GetLatest reads.
func applyReplay(tx kv.TemporalRwTx, d *Domain, keys [][]byte, targets [][]byte, stepBytes [8]byte) error {
	if len(keys) != len(targets) {
		return fmt.Errorf("applyReplay: keys (%d) and targets (%d) length mismatch", len(keys), len(targets))
	}
	if d.LargeValues {
		for i, k := range keys {
			fullKey := make([]byte, 0, len(k)+8)
			fullKey = append(fullKey, k...)
			fullKey = append(fullKey, stepBytes[:]...)
			if len(targets[i]) == 0 {
				if err := tx.Delete(d.ValuesTable, fullKey); err != nil {
					return fmt.Errorf("Delete(%s, fullKey=%x): %w", d.ValuesTable, fullKey, err)
				}
				continue
			}
			if err := tx.Put(d.ValuesTable, fullKey, targets[i]); err != nil {
				return fmt.Errorf("Put(%s, fullKey=%x): %w", d.ValuesTable, fullKey, err)
			}
		}
		return nil
	}

	c, err := tx.RwCursorDupSort(d.ValuesTable)
	if err != nil {
		return fmt.Errorf("RwCursorDupSort(%s): %w", d.ValuesTable, err)
	}
	defer c.Close()
	for i, k := range keys {
		foundVal, ferr := c.SeekBothRange(k, stepBytes[:])
		if ferr != nil {
			return fmt.Errorf("SeekBothRange(%s, key=%x, step): %w", d.ValuesTable, k, ferr)
		}
		if len(foundVal) >= 8 && bytes.Equal(foundVal[:8], stepBytes[:]) {
			if derr := c.DeleteCurrent(); derr != nil {
				return fmt.Errorf("DeleteCurrent(%s, key=%x): %w", d.ValuesTable, k, derr)
			}
		}

		if len(targets[i]) == 0 {
			continue
		}

		val := make([]byte, 0, 8+len(targets[i]))
		val = append(val, stepBytes[:]...)
		val = append(val, targets[i]...)
		if perr := c.Put(k, val); perr != nil {
			return fmt.Errorf("Put(%s, key=%x): %w", d.ValuesTable, k, perr)
		}
	}
	return nil
}

// wipeDomainValuesPastStep deletes ValuesTable rows whose step
// coordinate >= stepBoundary. With WipeWritableShadowPast's invariant
// `stepBoundary = (lastTxNum / stepSize) + 1`, step `stepBoundary - 1`
// is the step containing lastTxNum, and steps >= stepBoundary contain
// only txnums > lastTxNum — i.e. writes past toBlock that must not
// survive the wipe.
//
// The boundary step itself (stepBoundary - 1) is preserved here.
// WipeWritableShadowPast handles boundary-step correctness via the
// separate diff-replay path for non-aligned cuts.
//
// The step coordinate's location depends on the domain's LargeValues
// setting:
//
//   - LargeValues=true: rows are key=fullKey+stepBytes, value=raw.
//     stepBytes is the last 8 bytes of the key, encoded as
//     ^binary.BigEndian.Uint64(step).
//   - LargeValues=false (DupSort): rows are key=fullKey,
//     value=stepBytes+raw. stepBytes is the first 8 bytes of the value,
//     same encoding.
//
// Mirrors the encoding d.unwind uses on its diff-entry write path.
func wipeDomainValuesPastStep(tx kv.RwTx, d *Domain, stepBoundary uint64) error {
	return wipeDomainValuesFiltered(tx, d, func(step uint64) bool { return step >= stepBoundary })
}

// wipeDomainValuesAtStep deletes ValuesTable rows whose step
// coordinate equals exactly `targetStep`. The whole-step wipe
// variant used by mode B for HistoryDisabled or
// IiCfg-disabled-by-default domains (commitment, RCache) where
// per-txnum diff-replay can't reach boundary-step entries written by
// forward exec past lastTxNum.
//
// Note: this is destructive across the entire step, including
// pre-lastTxNum entries written in the same step. For commitment
// this is acceptable because Provider.Unwind's Apply phase
// repopulates the step from the recompute primitive's branch
// collector. For RCache the cache rebuilds on subsequent exec.
func wipeDomainValuesAtStep(tx kv.RwTx, d *Domain, targetStep uint64) error {
	return wipeDomainValuesFiltered(tx, d, func(step uint64) bool { return step == targetStep })
}

// wipeDomainValuesFiltered cursor-walks the ValuesTable and deletes
// every row whose decoded step satisfies the supplied predicate. The
// step coordinate's location depends on the domain's LargeValues
// setting (same as the encoder used by d.unwind on diff-entry write):
//
//   - LargeValues=true: rows are key=fullKey+stepBytes, value=raw.
//     stepBytes is the last 8 bytes of the key, encoded as
//     ^binary.BigEndian.Uint64(step).
//   - LargeValues=false (DupSort): rows are key=fullKey,
//     value=stepBytes+raw. stepBytes is the first 8 bytes of the
//     value, same encoding.
func wipeDomainValuesFiltered(tx kv.RwTx, d *Domain, predicate func(step uint64) bool) error {
	if d.LargeValues {
		c, err := tx.RwCursor(d.ValuesTable)
		if err != nil {
			return err
		}
		defer c.Close()
		for k, _, err := c.First(); k != nil; k, _, err = c.Next() {
			if err != nil {
				return err
			}
			if len(k) < 8 {
				continue
			}
			encodedStep := binary.BigEndian.Uint64(k[len(k)-8:])
			step := ^encodedStep
			if predicate(step) {
				if err := c.DeleteCurrent(); err != nil {
					return err
				}
			}
		}
		return nil
	}

	c, err := tx.RwCursorDupSort(d.ValuesTable)
	if err != nil {
		return err
	}
	defer c.Close()
	for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
		if err != nil {
			return err
		}
		if len(v) < 8 {
			continue
		}
		encodedStep := binary.BigEndian.Uint64(v[:8])
		step := ^encodedStep
		if predicate(step) {
			if err := c.DeleteCurrent(); err != nil {
				return err
			}
		}
	}
	return nil
}
