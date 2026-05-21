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
	"fmt"
	"os"
	"path/filepath"

	"github.com/erigontech/erigon/db/datastruct/btindex"
	"github.com/erigontech/erigon/db/datastruct/existence"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/db/version"
)

// BeginFilesRoWithOverrides opens a files-RO view like BeginFilesRo, then —
// for this transaction only — substitutes the visible domain values file
// covering each override path's step range with a FilesItem opened on that
// path. It is used to validate a staged (not-yet-live) snapshot file in the
// context of the otherwise-live state, without touching the live Aggregator
// or any other transaction. See
// docs/plans/20260520-phase7-staged-adoption-design.md §7b-3.
//
// Each override path must be a domain values (.kv) file whose accessors
// (.bt / .kvi / .kvei, as the domain requires) sit alongside it in the same
// directory. History (.v) and inverted-index (.ef) overrides are out of
// scope — the Stage-2 validators read only the domain values file.
//
// The substitute FilesItems are owned by the returned AggregatorRoTx and
// released by its Close().
func (a *Aggregator) BeginFilesRoWithOverrides(overridePaths []string) (*AggregatorRoTx, error) {
	ac := a.BeginFilesRo()
	for _, p := range overridePaths {
		if err := ac.applyDomainOverride(p); err != nil {
			ac.Close()
			return nil, fmt.Errorf("override %s: %w", filepath.Base(p), err)
		}
	}
	return ac, nil
}

// applyDomainOverride opens the FilesItem at path and swaps it into the
// matching DomainRoTx.files slot for this AggregatorRoTx only. It must run
// before any read on the affected domain — BeginFilesRoWithOverrides
// guarantees this by applying overrides immediately after BeginFilesRo,
// when the per-file reader caches are still unbuilt.
func (at *AggregatorRoTx) applyDomainOverride(path string) error {
	name := filepath.Base(path)
	info, _, ok := snaptype.ParseFileName("", name)
	if !ok || info.TypeString == "" {
		return fmt.Errorf("not a parseable state file name")
	}
	if info.Ext != ".kv" {
		return fmt.Errorf("override must be a domain values (.kv) file, got %q", info.Ext)
	}
	dom, err := kv.String2Domain(info.TypeString)
	if err != nil {
		return err
	}
	dt := at.d[dom]
	if dt == nil {
		return fmt.Errorf("domain %s not present in this view", dom)
	}
	fi, err := dt.d.openDomainFileItemAt(filepath.Dir(path), kv.Step(info.From), kv.Step(info.To))
	if err != nil {
		return err
	}
	if err := dt.substituteFile(fi); err != nil {
		fi.closeFiles()
		return err
	}
	at.substitutes = append(at.substitutes, fi)
	return nil
}

// substituteFile swaps fi into the visible-file slot covering the same
// txnum range, for this DomainRoTx only. It must run before the first read
// on this DomainRoTx — the per-file reader caches are keyed by slice index
// and are not invalidated here.
func (dt *DomainRoTx) substituteFile(fi *FilesItem) error {
	idx := -1
	for i := range dt.files {
		if dt.files[i].startTxNum == fi.startTxNum && dt.files[i].endTxNum == fi.endTxNum {
			idx = i
			break
		}
	}
	if idx < 0 {
		return fmt.Errorf("no live %s file covering txnum range [%d, %d)", dt.name, fi.startTxNum, fi.endTxNum)
	}

	// dt.files is the domainVisible's slice, shared across every other
	// transaction — clone before mutating.
	newFiles := make(visibleFiles, len(dt.files))
	copy(newFiles, dt.files)
	replaced := newFiles[idx].src
	newFiles[idx] = visibleFile{
		startTxNum: fi.startTxNum,
		endTxNum:   fi.endTxNum,
		i:          idx,
		src:        fi,
	}
	dt.files = newFiles

	// beginFilesRo pinned `replaced` with a refcount increment; this tx no
	// longer references it (Close decrements the cloned slice, which holds
	// the substitute instead). Release `replaced` now to keep the books
	// balanced.
	visibleFiles{{src: replaced}}.refcntDecrement(dt.d.FilenameBase, dt.d.logger)
	return nil
}

// openDomainFileItemAt opens a domain values file and its accessors from
// snapDir as a standalone FilesItem. The item is not tracked by
// d.dirtyFiles; the caller (AggregatorRoTx.Close) closes it. Unlike
// openDirtyFiles, a missing accessor is a hard error — a staged file with
// an absent accessor cannot be validated.
func (d *Domain) openDomainFileItemAt(snapDir string, fromStep, toStep kv.Step) (*FilesItem, error) {
	entries, err := os.ReadDir(snapDir)
	if err != nil {
		return nil, fmt.Errorf("read staging dir: %w", err)
	}
	dirEntries := make([]string, 0, len(entries))
	for _, e := range entries {
		dirEntries = append(dirEntries, e.Name())
	}

	item := newFilesItem(uint64(fromStep)*d.stepSize, uint64(toStep)*d.stepSize, d.stepSize, d.stepsInFrozenFile)

	kvPath, kvVer, ok, err := version.MatchVersionedFile(d.kvFileNameMask(fromStep, toStep), dirEntries, snapDir)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("no %s values file for steps %d-%d in %s", d.FilenameBase, fromStep, toStep, snapDir)
	}
	d.FileVersion.DataKV.MustSupport(kvVer, filepath.Base(kvPath))
	if item.decompressor, err = seg.NewDecompressor(kvPath); err != nil {
		return nil, fmt.Errorf("open %s decompressor: %w", filepath.Base(kvPath), err)
	}

	if d.Accessors.Has(statecfg.AccessorHashMap) {
		p, ver, ok, err := version.MatchVersionedFile(d.kviAccessorFileNameMask(fromStep, toStep), dirEntries, snapDir)
		if err != nil || !ok {
			item.closeFiles()
			return nil, fmt.Errorf("no .kvi accessor for %s steps %d-%d: %w", d.FilenameBase, fromStep, toStep, err)
		}
		d.FileVersion.AccessorKVI.MustSupport(ver, filepath.Base(p))
		if item.index, err = recsplit.OpenIndex(p); err != nil {
			item.closeFiles()
			return nil, fmt.Errorf("open .kvi accessor: %w", err)
		}
	}
	if d.Accessors.Has(statecfg.AccessorBTree) {
		p, ver, ok, err := version.MatchVersionedFile(d.kvBtAccessorFileNameMask(fromStep, toStep), dirEntries, snapDir)
		if err != nil || !ok {
			item.closeFiles()
			return nil, fmt.Errorf("no .bt accessor for %s steps %d-%d: %w", d.FilenameBase, fromStep, toStep, err)
		}
		d.FileVersion.AccessorBT.MustSupport(ver, filepath.Base(p))
		if item.bindex, err = btindex.OpenBtreeIndexWithDecompressor(p, btindex.DefaultBtreeM, d.dataReader(item.decompressor)); err != nil {
			item.closeFiles()
			return nil, fmt.Errorf("open .bt accessor: %w", err)
		}
	}
	if d.Accessors.Has(statecfg.AccessorExistence) {
		p, ver, ok, err := version.MatchVersionedFile(d.kvExistenceIdxFileNameMask(fromStep, toStep), dirEntries, snapDir)
		if err != nil || !ok {
			item.closeFiles()
			return nil, fmt.Errorf("no .kvei accessor for %s steps %d-%d: %w", d.FilenameBase, fromStep, toStep, err)
		}
		d.FileVersion.AccessorKVEI.MustSupport(ver, filepath.Base(p))
		if item.existence, err = existence.OpenFilter(p, false); err != nil {
			item.closeFiles()
			return nil, fmt.Errorf("open .kvei accessor: %w", err)
		}
	}
	return item, nil
}
