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

package reset

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	g "github.com/anacrolix/generics"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/preverified"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snaptype"
)

type CaplinResetFile struct {
	Name    string
	Path    string
	Family  string
	From    uint64
	To      uint64
	Torrent bool
}

type CaplinResetDBRows struct {
	Table string
	From  uint64
	To    uint64
	Rows  int
}

type CaplinResetRange = snapshotsync.CaplinRange

type CaplinResetPlan struct {
	Files              []CaplinResetFile
	OnlyCopies         []CaplinResetFile
	DBRows             []CaplinResetDBRows
	BlockRanges        []CaplinResetRange
	Progress           uint64
	ProgressAfter      uint64
	StoredRetention    clparams.CaplinRetention
	StoredRetentionSet bool

	roots  []caplinResetRoot
	tables map[string]struct{}
}

type caplinResetRoot struct {
	slot      uint64
	root      common.Hash
	stateRoot common.Hash
	hasState  bool
}

type caplinResetDBRow struct {
	table string
	key   []byte
}

func (root caplinResetRoot) derivedRows() []caplinResetDBRow {
	rows := []caplinResetDBRow{
		{table: kv.CanonicalBlockRoots, key: base_encoding.Encode64ToBytes4(root.slot)},
		{table: kv.BlockRootToSlot, key: root.root[:]},
		{table: kv.BlockRootToStateRoot, key: root.root[:]},
		{table: kv.BlockRootToParentRoot, key: root.root[:]},
	}
	if root.hasState {
		rows = append(rows, caplinResetDBRow{table: kv.StateRootToBlockRoot, key: root.stateRoot[:]})
	}
	return rows
}

func OpenCaplinDB(ctx context.Context, path string, logger log.Logger, readonly bool) (kv.RwDB, error) {
	if _, err := os.Stat(filepath.Join(path, "mdbx.dat")); errors.Is(err, os.ErrNotExist) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	probe, err := mdbx.New(dbcfg.CaplinDB, logger).Path(path).Accede(true).Readonly(true).
		WithTableCfg(func(kv.TableCfg) kv.TableCfg { return kv.TableCfg{} }).Open(ctx)
	if err != nil {
		return nil, err
	}
	var tables kv.TableCfg
	err = probe.View(ctx, func(tx kv.Tx) error {
		var names []string
		names, err = tx.ListTables()
		if err != nil {
			return err
		}
		tables = make(kv.TableCfg, len(names))
		for _, name := range names {
			if cfg, ok := kv.ChaindataTablesCfg[name]; ok {
				tables[name] = cfg
			}
		}
		return nil
	})
	probe.Close()
	if err != nil {
		return nil, err
	}
	return mdbx.New(dbcfg.CaplinDB, logger).Path(path).Accede(true).Readonly(readonly).
		WithTableCfg(func(kv.TableCfg) kv.TableCfg { return tables }).Open(ctx)
}

func BuildCaplinResetPlan(ctx context.Context, dirs datadir.Dirs, items preverified.SortedItems, db kv.RoDB, removeUnknown bool, logger log.Logger) (CaplinResetPlan, error) {
	plan := CaplinResetPlan{}
	coverage := caplinResetCoverage(items)
	reset := Reset{Logger: logger, PreverifiedSnapshots: items, RemoveUnknown: removeUnknown, Dirs: &dirs}
	for _, prefix := range []string{"", "caplin"} {
		dir := filepath.Join(dirs.Snap, prefix)
		entries, err := os.ReadDir(dir)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return CaplinResetPlan{}, err
		}
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			baseName := entry.Name()
			isPartial := strings.HasSuffix(baseName, ".part")
			snapName, isTorrent := strings.CutSuffix(strings.TrimSuffix(baseName, ".part"), ".torrent")
			itemName := filepath.ToSlash(filepath.Join(prefix, snapName))
			info, _, ok := snaptype.ParseFileName("", itemName)
			if !ok {
				continue
			}
			classified, ok := snapshotsync.CaplinFileInfo(itemName, info)
			if !ok {
				continue
			}
			if info.Type == nil || !info.Type.Versions().Supports(info.Version) {
				continue
			}
			item, found := items.Get(itemName)
			doRemove := reset.decideRemove(resetItemInfo{
				filePath:      OsFilePath(filepath.Join(dir, baseName)),
				snapName:      itemName,
				hash:          g.OptionFromTuple(item.Hash, found),
				isTorrent:     isTorrent,
				inPreverified: found,
			})
			if !doRemove {
				continue
			}
			family := caplinResetFamily(info, classified)
			file := CaplinResetFile{
				Name:    itemName,
				Path:    filepath.Join(dir, baseName),
				Family:  family,
				From:    classified.From,
				To:      classified.To,
				Torrent: isTorrent,
			}
			plan.Files = append(plan.Files, file)
			if !isPartial && !isTorrent && info.Ext == ".seg" && !caplinResetHasCoverage(coverage, caplinResetFamily(info, classified), classified.From, classified.To) {
				plan.OnlyCopies = append(plan.OnlyCopies, file)
			}
			if !isPartial && !isTorrent && info.Ext == ".seg" && classified.Family == snapshotsync.CaplinBlocks {
				plan.BlockRanges = append(plan.BlockRanges, CaplinResetRange{From: classified.From, To: classified.To})
			}
		}
	}
	plan.BlockRanges = caplinResetRanges(plan.BlockRanges)
	plan.OnlyCopies = caplinResetOnlyCopies(plan.OnlyCopies)
	if err := plan.readDB(ctx, db); err != nil {
		return CaplinResetPlan{}, err
	}
	return plan, nil
}

func caplinResetCoverage(items preverified.SortedItems) map[string][]CaplinResetRange {
	coverage := make(map[string][]CaplinResetRange)
	for _, item := range items {
		info, _, ok := snaptype.ParseFileName("", item.Name)
		if !ok || info.Ext != ".seg" {
			continue
		}
		classified, ok := snapshotsync.CaplinFileInfo(item.Name, info)
		if !ok {
			continue
		}
		if info.Type == nil || !info.Type.Versions().Supports(info.Version) {
			continue
		}
		key := caplinResetFamily(info, classified)
		coverage[key] = append(coverage[key], CaplinResetRange{From: classified.From, To: classified.To})
	}
	for key, ranges := range coverage {
		coverage[key] = snapshotsync.ScanCaplinRanges(ranges).Covered
	}
	return coverage
}

func caplinResetFamily(info snaptype.FileInfo, file snapshotsync.CaplinFile) string {
	switch file.Family {
	case snapshotsync.CaplinBlocks:
		return "blocks"
	case snapshotsync.CaplinSidecars:
		return "sidecars"
	default:
		return "state/" + info.TypeString
	}
}

func isCaplinResetName(name string) bool {
	info, _, ok := snaptype.ParseFileName("", name)
	if !ok {
		return false
	}
	_, ok = snapshotsync.CaplinFileInfo(name, info)
	return ok
}

func caplinResetHasCoverage(coverage map[string][]CaplinResetRange, key string, from, to uint64) bool {
	for _, replacement := range coverage[key] {
		if replacement.From > from {
			break
		}
		if replacement.From <= from && replacement.To >= to {
			return true
		}
	}
	return false
}

func caplinResetRanges(ranges []CaplinResetRange) []CaplinResetRange {
	ranges = slices.DeleteFunc(ranges, func(item CaplinResetRange) bool { return item.From >= item.To })
	slices.SortFunc(ranges, func(a, b CaplinResetRange) int {
		return cmp.Or(cmp.Compare(a.From, b.From), cmp.Compare(a.To, b.To))
	})
	return slices.Compact(ranges)
}

func caplinResetOnlyCopies(files []CaplinResetFile) []CaplinResetFile {
	slices.SortStableFunc(files, func(a, b CaplinResetFile) int {
		return cmp.Or(strings.Compare(a.Family, b.Family), cmp.Compare(a.From, b.From), cmp.Compare(a.To, b.To))
	})
	return slices.CompactFunc(files, func(a, b CaplinResetFile) bool {
		return a.Family == b.Family && a.From == b.From && a.To == b.To
	})
}

func (plan *CaplinResetPlan) readDB(ctx context.Context, db kv.RoDB) error {
	if db == nil {
		return nil
	}
	return db.View(ctx, func(tx kv.Tx) error {
		names, err := tx.ListTables()
		if err != nil {
			return err
		}
		plan.tables = make(map[string]struct{}, len(names))
		for _, name := range names {
			plan.tables[name] = struct{}{}
		}
		if _, ok := plan.tables[kv.DatabaseInfo]; ok {
			plan.StoredRetention, plan.StoredRetentionSet, err = beacon_indicies.ReadCaplinRetention(tx)
			if err != nil {
				return err
			}
		}
		if _, ok := plan.tables[kv.LastBeaconSnapshot]; ok {
			plan.Progress, err = beacon_indicies.ReadLastBeaconSnapshot(tx)
			if err != nil {
				return err
			}
		}
		plan.ProgressAfter = plan.Progress
		if len(plan.BlockRanges) == 0 {
			return nil
		}
		if _, ok := plan.tables[kv.CanonicalBlockRoots]; !ok {
			return nil
		}
		var roots []caplinResetRoot
		for _, resetRange := range plan.BlockRanges {
			rangeRoots, err := caplinResetRootsInRange(tx, resetRange, plan.Progress)
			if err != nil {
				return err
			}
			roots = append(roots, rangeRoots...)
		}
		seen := make(map[string]struct{}, len(roots))
		for i := range roots {
			key := fmt.Sprintf("%d:%x", roots[i].slot, roots[i].root)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			if _, ok := plan.tables[kv.BlockRootToStateRoot]; ok {
				value, err := tx.GetOne(kv.BlockRootToStateRoot, roots[i].root[:])
				if err != nil {
					return err
				}
				if len(value) == len(common.Hash{}) {
					roots[i].stateRoot = common.BytesToHash(value)
					roots[i].hasState = true
				}
			}
		}
		plan.roots = roots
		plan.ProgressAfter = min(plan.Progress, plan.BlockRanges[0].From)
		if err := plan.buildDBRows(tx); err != nil {
			return err
		}
		return nil
	})
}

func caplinResetRootsInRange(tx kv.Tx, resetRange CaplinResetRange, progress uint64) ([]caplinResetRoot, error) {
	if progress == 0 {
		return nil, nil
	}
	resetRange.To = min(resetRange.To, progress)
	if resetRange.From >= resetRange.To {
		return nil, nil
	}
	cursor, err := tx.Cursor(kv.CanonicalBlockRoots)
	if err != nil {
		return nil, err
	}
	defer cursor.Close()
	var roots []caplinResetRoot
	key, value, err := cursor.Seek(base_encoding.Encode64ToBytes4(resetRange.From))
	if err != nil {
		return nil, err
	}
	for ; ; key, value, err = cursor.Next() {
		if err != nil {
			return nil, err
		}
		if key == nil {
			break
		}
		if len(key) != 4 {
			continue
		}
		slot := base_encoding.Decode64FromBytes4(key)
		if slot >= resetRange.To {
			break
		}
		roots = append(roots, caplinResetRoot{slot: slot, root: common.BytesToHash(value)})
	}
	return roots, nil
}

func (plan *CaplinResetPlan) buildDBRows(tx kv.Tx) error {
	if len(plan.roots) == 0 {
		return nil
	}
	counts := make(map[string]map[CaplinResetRange]map[string]struct{})
	rootRanges := make(map[uint64]CaplinResetRange, len(plan.roots))
	for _, root := range plan.roots {
		resetRange, ok := rootRanges[root.slot]
		if !ok {
			for _, candidate := range plan.BlockRanges {
				if candidate.From <= root.slot && root.slot < candidate.To {
					resetRange = candidate
					break
				}
			}
			rootRanges[root.slot] = resetRange
		}
		if resetRange == (CaplinResetRange{}) {
			continue
		}
		for _, row := range root.derivedRows() {
			if _, ok := plan.tables[row.table]; !ok {
				continue
			}
			value, err := tx.GetOne(row.table, row.key)
			if err != nil {
				return err
			}
			if len(value) == 0 {
				continue
			}
			if counts[row.table] == nil {
				counts[row.table] = make(map[CaplinResetRange]map[string]struct{})
			}
			if counts[row.table][resetRange] == nil {
				counts[row.table][resetRange] = make(map[string]struct{})
			}
			counts[row.table][resetRange][string(row.key)] = struct{}{}
		}
	}
	for table, ranges := range counts {
		for resetRange, keys := range ranges {
			plan.DBRows = append(plan.DBRows, CaplinResetDBRows{Table: table, From: resetRange.From, To: resetRange.To, Rows: len(keys)})
		}
	}
	slices.SortFunc(plan.DBRows, func(a, b CaplinResetDBRows) int {
		return cmp.Or(strings.Compare(a.Table, b.Table), cmp.Compare(a.From, b.From))
	})
	return nil
}

func (plan CaplinResetPlan) Apply(ctx context.Context, db kv.RwDB, remove func(string) error, force bool) error {
	if len(plan.OnlyCopies) != 0 && !force {
		return fmt.Errorf("caplin reset refuses to delete only copy: %s", formatCaplinResetOnlyCopy(plan.OnlyCopies[0]))
	}
	if db != nil && (len(plan.roots) != 0 || plan.Progress != plan.ProgressAfter) {
		if err := db.Update(ctx, func(tx kv.RwTx) error {
			for _, root := range plan.roots {
				for _, row := range root.derivedRows() {
					if _, ok := plan.tables[row.table]; ok {
						if err := tx.Delete(row.table, row.key); err != nil {
							return err
						}
					}
				}
			}
			if plan.Progress != plan.ProgressAfter {
				if _, ok := plan.tables[kv.LastBeaconSnapshot]; ok {
					return beacon_indicies.WriteLastBeaconSnapshot(tx, plan.ProgressAfter)
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	for _, file := range plan.Files {
		if err := remove(file.Path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove Caplin file %q: %w", file.ReportName(), err)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	return nil
}

func (file CaplinResetFile) ReportName() string {
	name := file.Name
	baseName := filepath.Base(file.Path)
	if strings.HasSuffix(baseName, ".part") {
		return name + ".part"
	}
	if file.Torrent {
		return name + ".torrent"
	}
	return name
}

func (plan CaplinResetPlan) Format(w io.Writer) {
	fmt.Fprintln(w, "caplin reset plan")
	for _, file := range plan.Files {
		fmt.Fprintf(w, "delete %s %s %s\n", file.Family, file.ReportName(), formatCaplinResetRange(file.From, file.To))
	}
	for _, file := range plan.OnlyCopies {
		fmt.Fprintf(w, "only copy %s %s %s\n", file.Family, file.Name, formatCaplinResetRange(file.From, file.To))
	}
	for _, rows := range plan.DBRows {
		fmt.Fprintf(w, "db rows %s %s %d\n", rows.Table, formatCaplinResetRange(rows.From, rows.To), rows.Rows)
	}
	fmt.Fprintf(w, "progress %d -> %d\n", plan.Progress, plan.ProgressAfter)
	if plan.StoredRetentionSet {
		fmt.Fprintf(w, "stored regime %s (kept)\n", plan.StoredRetention.Mode.String())
	} else {
		fmt.Fprintln(w, "stored regime none (kept)")
	}
	fmt.Fprintln(w, "next start fetches Caplin files selected by its retention fields")
}

func formatCaplinResetRange(from, to uint64) string {
	return fmt.Sprintf("%d..%d", from, to)
}

func formatCaplinResetOnlyCopy(file CaplinResetFile) string {
	return file.Family + " " + file.Name + " " + formatCaplinResetRange(file.From, file.To)
}
