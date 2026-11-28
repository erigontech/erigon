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

package antiquary

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	datadirreset "github.com/erigontech/erigon/db/datadir/reset"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snaptype"
)

type CaplinDuRequest struct {
	Chain        string
	Dirs         datadir.Dirs
	BeaconConfig *clparams.BeaconChainConfig
	IndexDB      kv.RoDB
	Manifest     snapcfg.PreverifiedItems
	Verbose      bool
}

type CaplinDuResult struct {
	Chain                string                          `json:"chain"`
	Regime               string                          `json:"regime"`
	RegimeStored         bool                            `json:"regime_stored"`
	Floors               *clparams.CaplinRetentionFloors `json:"floors,omitempty"`
	HeadSlot             uint64                          `json:"head_slot"`
	HeadSlotFromSegment  bool                            `json:"head_slot_from_segment,omitempty"`
	Rows                 []CaplinDuRow                   `json:"rows"`
	MDBX                 CaplinDuMDBX                    `json:"mdbx"`
	TotalBytes           uint64                          `json:"total_bytes"`
	Estimates            []CaplinDuEstimate              `json:"estimates"`
	EstimatesUnavailable bool                            `json:"estimates_unavailable,omitempty"`
}

type CaplinDuRow struct {
	Family         string `json:"family"`
	Tier           string `json:"tier"`
	FromSlot       uint64 `json:"from_slot"`
	ToSlot         uint64 `json:"to_slot"`
	Files          int    `json:"files"`
	Bytes          uint64 `json:"bytes"`
	InMDBX         bool   `json:"in_mdbx,omitempty"`
	BucketGranular bool   `json:"bucket_granular,omitempty"`
	HasData        bool   `json:"-"`
}

type CaplinDuMDBX struct {
	Bytes  uint64          `json:"bytes"`
	Tables []CaplinDuTable `json:"tables"`
}

type CaplinDuTable struct {
	Name  string `json:"name"`
	Bytes uint64 `json:"bytes"`
}

type CaplinDuEstimate struct {
	Regime   string `json:"regime"`
	Bytes    uint64 `json:"bytes"`
	NotLocal int    `json:"not_local"`
}

type caplinDuFile struct {
	item   snapcfg.PreverifiedItem
	family snapshotsync.CaplinFileFamily
	from   uint64
	to     uint64
	bytes  uint64
}

type caplinDuSpan struct {
	from  uint64
	to    uint64
	files int
	bytes uint64
	has   bool
}

func OpenCaplinDuIndexDB(ctx context.Context, dir string, logger log.Logger) (kv.RwDB, error) {
	return datadirreset.OpenCaplinDB(ctx, dir, logger, true)
}

func CollectCaplinDu(ctx context.Context, req CaplinDuRequest) (CaplinDuResult, error) {
	files, err := collectCaplinDuFiles(req.Dirs)
	if err != nil {
		return CaplinDuResult{}, err
	}
	blocks := caplinDuFilesSpan(files, snapshotsync.CaplinBlocks)
	blobs := caplinDuFilesSpan(files, snapshotsync.CaplinSidecars)
	states := caplinDuFilesSpan(files, snapshotsync.CaplinStates)
	blobHot, err := caplinDuHotSpan(req.Dirs.CaplinBlobs)
	if err != nil {
		return CaplinDuResult{}, fmt.Errorf("read blob store: %w", err)
	}
	columnHot, err := caplinDuHotSpan(req.Dirs.CaplinColumnData)
	if err != nil {
		return CaplinDuResult{}, fmt.Errorf("read column store: %w", err)
	}

	result := CaplinDuResult{
		Chain:  req.Chain,
		Regime: "none stored",
		Rows: []CaplinDuRow{
			caplinDuRow("beacon blocks", "cold", blocks, false, false),
			caplinDuRow("beacon blocks", "hot", caplinDuSpan{}, false, false),
			caplinDuRow("blob sidecars", "cold", blobs, false, false),
			caplinDuRow("blob sidecars", "hot", blobHot, false, true),
			caplinDuRow("data columns", "hot", columnHot, false, true),
			caplinDuRow("beacon state", "cold", states, false, false),
		},
	}

	var stored clparams.CaplinRetention
	var storedFound bool
	var dbRange caplinDuSpan
	var dbBytes uint64
	var tables []CaplinDuTable
	if req.IndexDB != nil {
		stored, storedFound, dbRange, dbBytes, tables, err = readCaplinDuDB(ctx, req.IndexDB, req.Dirs.CaplinIndexing, req.Verbose)
		if err != nil {
			return CaplinDuResult{}, err
		}
		result.Rows[1] = caplinDuRow("beacon blocks", "hot", dbRange, true, false)
	}
	result.HeadSlot = dbRange.to
	if !dbRange.has {
		result.HeadSlot = blocks.to
		result.HeadSlotFromSegment = blocks.has
	}
	result.RegimeStored = storedFound
	if storedFound {
		if req.BeaconConfig != nil {
			stored, err = stored.Normalize(req.BeaconConfig)
			if err != nil {
				return CaplinDuResult{}, err
			}
		}
		result.Regime = stored.Mode.String()
		if req.BeaconConfig != nil {
			floors, err := stored.Floors(req.BeaconConfig, result.HeadSlot)
			if err != nil {
				return CaplinDuResult{}, err
			}
			result.Floors = &floors
		}
	}
	result.MDBX = CaplinDuMDBX{Bytes: dbBytes, Tables: tables}
	result.TotalBytes = dbBytes
	for _, row := range result.Rows {
		if !row.InMDBX {
			result.TotalBytes += row.Bytes
		}
	}
	if req.BeaconConfig == nil {
		result.EstimatesUnavailable = true
		return result, nil
	}
	result.Estimates, err = caplinDuEstimates(req, files, stored, storedFound, result.HeadSlot)
	if err != nil {
		return CaplinDuResult{}, err
	}
	return result, nil
}

func collectCaplinDuFiles(dirs datadir.Dirs) ([]caplinDuFile, error) {
	var files []caplinDuFile
	for _, prefix := range []string{"", "caplin"} {
		dir := filepath.Join(dirs.Snap, prefix)
		entries, err := os.ReadDir(dir)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			name := entry.Name()
			itemName := filepath.ToSlash(filepath.Join(prefix, name))
			info, _, ok := snaptype.ParseFileName("", itemName)
			if !ok {
				continue
			}
			if info.Ext != ".seg" && info.Ext != ".idx" {
				continue
			}
			classified, ok := snapshotsync.CaplinFileInfo(itemName, info)
			if !ok {
				continue
			}
			stat, err := entry.Info()
			if err != nil {
				return nil, err
			}
			file := caplinDuFile{
				item:   snapcfg.PreverifiedItem{Name: itemName},
				family: classified.Family,
				from:   classified.From,
				to:     classified.To,
				bytes:  uint64(stat.Size()),
			}
			files = append(files, file)
		}
	}
	return files, nil
}

func caplinDuFilesSpan(files []caplinDuFile, family snapshotsync.CaplinFileFamily) caplinDuSpan {
	var result caplinDuSpan
	for _, file := range files {
		if file.family != family {
			continue
		}
		if !result.has || file.from < result.from {
			result.from = file.from
		}
		if file.to > result.to {
			result.to = file.to
		}
		result.files++
		result.bytes += file.bytes
		result.has = true
	}
	return result
}

func caplinDuRow(family, tier string, span caplinDuSpan, inMDBX, bucketGranular bool) CaplinDuRow {
	return CaplinDuRow{Family: family, Tier: tier, FromSlot: span.from, ToSlot: span.to, Files: span.files, Bytes: span.bytes, InMDBX: inMDBX, BucketGranular: bucketGranular, HasData: span.has}
}

func caplinDuHotSpan(root string) (caplinDuSpan, error) {
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return caplinDuSpan{}, nil
	}
	if err != nil {
		return caplinDuSpan{}, err
	}
	var result caplinDuSpan
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		bucket, err := strconv.ParseUint(entry.Name(), 10, 64)
		if err != nil || strconv.FormatUint(bucket, 10) != entry.Name() {
			continue
		}
		bucketBytes, bucketFiles, err := caplinDuBucketStats(filepath.Join(root, entry.Name()))
		if err != nil {
			return caplinDuSpan{}, err
		}
		if bucketFiles == 0 {
			continue
		}
		from := bucket * blob_storage.BucketSlots
		if !result.has || from < result.from {
			result.from = from
		}
		if to := from + blob_storage.BucketSlots; to > result.to {
			result.to = to
		}
		result.has = true
		result.bytes += bucketBytes
		result.files += bucketFiles
	}
	return result, nil
}

func CaplinDuOldestHotSlot(root string) (clparams.StoredSlot, error) {
	span, err := caplinDuHotSpan(root)
	if err != nil {
		return clparams.StoredSlot{}, err
	}
	return clparams.StoredSlot{Slot: span.from, Holds: span.has}, nil
}

func caplinDuBucketStats(root string) (uint64, int, error) {
	var bytes uint64
	var files int
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if strings.HasSuffix(entry.Name(), ".tmp") || strings.HasPrefix(filepath.Ext(entry.Name()), ".torrent") {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			bytes += uint64(info.Size())
			files++
		}
		return nil
	})
	if err != nil {
		return 0, 0, err
	}
	return bytes, files, nil
}

func readCaplinDuDB(ctx context.Context, db kv.RoDB, dbPath string, verbose bool) (clparams.CaplinRetention, bool, caplinDuSpan, uint64, []CaplinDuTable, error) {
	var stored clparams.CaplinRetention
	var storedFound bool
	var blockRange caplinDuSpan
	var blockBytes uint64
	var tables []CaplinDuTable
	err := db.View(ctx, func(tx kv.Tx) error {
		names, err := tx.ListTables()
		if err != nil {
			return err
		}
		if slices.Contains(names, kv.DatabaseInfo) {
			stored, storedFound, err = beacon_indicies.ReadCaplinRetention(tx)
			if err != nil {
				return err
			}
		}
		if slices.Contains(names, kv.BeaconBlocks) {
			blockRange, err = caplinDuTableRange(tx, kv.BeaconBlocks)
			if err != nil {
				return err
			}
			blockBytes, err = tx.BucketSize(kv.BeaconBlocks)
			if err != nil {
				return err
			}
			blockRange.bytes = blockBytes
		}
		if slices.Contains(names, kv.CanonicalBlockRoots) {
			canonicalSlot, _, err := beacon_indicies.ReadCanonicalHead(tx)
			if err != nil {
				return err
			}
			if !blockRange.has || canonicalSlot > blockRange.to {
				blockRange.to = canonicalSlot
				blockRange.has = true
			}
		}
		tables, err = caplinDuTables(tx, names, verbose)
		return err
	})
	if err != nil {
		return clparams.CaplinRetention{}, false, caplinDuSpan{}, 0, nil, err
	}
	dataBytes, err := caplinDuFileSize(filepath.Join(dbPath, "mdbx.dat"))
	if err != nil {
		return clparams.CaplinRetention{}, false, caplinDuSpan{}, 0, nil, err
	}
	return stored, storedFound, blockRange, dataBytes, tables, nil
}

func caplinDuTableRange(tx kv.Tx, table string) (caplinDuSpan, error) {
	cursor, err := tx.Cursor(table)
	if err != nil {
		return caplinDuSpan{}, err
	}
	defer cursor.Close()
	first, _, err := cursor.First()
	if err != nil {
		return caplinDuSpan{}, err
	}
	last, _, err := cursor.Last()
	if err != nil {
		return caplinDuSpan{}, err
	}
	if first == nil || last == nil {
		return caplinDuSpan{}, nil
	}
	firstSlot, err := dbutils.DecodeBlockNumber(first[:min(len(first), dbutils.NumberLength)])
	if err != nil {
		return caplinDuSpan{}, err
	}
	lastSlot, err := dbutils.DecodeBlockNumber(last[:min(len(last), dbutils.NumberLength)])
	if err != nil {
		return caplinDuSpan{}, err
	}
	return caplinDuSpan{from: firstSlot, to: lastSlot, has: true}, nil
}

func caplinDuTables(tx kv.Tx, names []string, verbose bool) ([]CaplinDuTable, error) {
	tables := make([]CaplinDuTable, 0, len(names))
	for _, name := range names {
		bytes, err := tx.BucketSize(name)
		if err != nil {
			return nil, err
		}
		tables = append(tables, CaplinDuTable{Name: name, Bytes: bytes})
	}
	slices.SortFunc(tables, func(a, b CaplinDuTable) int {
		if a.Bytes != b.Bytes {
			return cmp.Compare(b.Bytes, a.Bytes)
		}
		return strings.Compare(a.Name, b.Name)
	})
	if !verbose && len(tables) > 3 {
		tables = tables[:3]
	}
	return tables, nil
}

func caplinDuFileSize(path string) (uint64, error) {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return uint64(info.Size()), nil
}

func caplinDuEstimates(req CaplinDuRequest, files []caplinDuFile, stored clparams.CaplinRetention, storedFound bool, headSlot uint64) ([]CaplinDuEstimate, error) {
	items := req.Manifest
	if req.Manifest == nil {
		var err error
		items, err = snapcfg.PreverifiedItemsForDatadir(&req.Dirs, req.Chain)
		if err != nil {
			return nil, err
		}
	}
	sizes := make(map[string]uint64, len(files))
	for _, file := range files {
		sizes[file.item.Name] = file.bytes
	}
	retentions := []clparams.CaplinRetention{
		{Mode: clparams.CaplinPruneModeArchive},
		{Mode: clparams.CaplinPruneModeSpec},
	}
	if storedFound {
		if stored.Mode == clparams.CaplinPruneModePruned {
			retentions = append(retentions, stored)
		}
	}
	result := make([]CaplinDuEstimate, 0, len(retentions))
	for _, candidate := range retentions {
		kept, err := snapshotsync.CaplinFiles(items, candidate, req.BeaconConfig, headSlot)
		if err != nil {
			return nil, fmt.Errorf("%s estimate: %w", candidate.Mode.String(), err)
		}
		estimate := CaplinDuEstimate{Regime: candidate.Mode.String()}
		for _, item := range kept {
			bytes, ok := sizes[item.Name]
			if !ok {
				path := filepath.Join(req.Dirs.Snap, filepath.FromSlash(item.Name))
				stat, err := os.Stat(path)
				if errors.Is(err, os.ErrNotExist) {
					estimate.NotLocal++
					continue
				}
				if err != nil {
					return nil, err
				}
				if !stat.Mode().IsRegular() {
					estimate.NotLocal++
					continue
				}
				bytes = uint64(stat.Size())
			}
			estimate.Bytes += bytes
		}
		result = append(result, estimate)
	}
	return result, nil
}

func FormatCaplinDu(w io.Writer, result CaplinDuResult) {
	regime := result.Regime
	if result.RegimeStored {
		regime += " (stored)"
	}
	floors := "none"
	if result.Floors != nil {
		floors = result.Floors.String()
	}
	head := formatCaplinDuNumber(result.HeadSlot)
	if result.HeadSlotFromSegment {
		head += " (segment upper bound)"
	}
	fmt.Fprintf(w, "chain %s · regime %s · floors %s · head slot %s\n\n", result.Chain, regime, floors, head)
	fmt.Fprintf(w, "%-15s %-5s %9s %9s %5s %9s\n", "family", "tier", "from slot", "to slot", "files", "size")
	for _, row := range result.Rows {
		from, to := "-", "-"
		if row.HasData {
			from, to = formatCaplinDuNumber(row.FromSlot), formatCaplinDuNumber(row.ToSlot)
			if row.BucketGranular {
				from, to = "~"+from, "~"+to
			}
		}
		files := "-"
		if row.Files > 0 {
			files = strconv.Itoa(row.Files)
		}
		size := common.ByteCount(row.Bytes)
		if row.InMDBX {
			size += " (mdbx)"
		}
		fmt.Fprintf(w, "%-15s %-5s %9s %9s %5s %9s\n", row.Family, row.Tier, from, to, files, size)
	}
	fmt.Fprintln(w)
	tables := make([]string, 0, len(result.MDBX.Tables))
	for _, table := range result.MDBX.Tables {
		tables = append(tables, table.Name+" "+common.ByteCount(table.Bytes))
	}
	tableText := ""
	if len(tables) > 0 {
		tableText = " (" + strings.Join(tables, ", ") + ")"
	}
	fmt.Fprintf(w, "%-15s %-5s %9s %9s %5s %9s%s\n", "caplin mdbx", "-", "-", "-", "-", common.ByteCount(result.MDBX.Bytes), tableText)
	fmt.Fprintf(w, "total                                             %10s\n\n", common.ByteCount(result.TotalBytes))
	if result.EstimatesUnavailable {
		fmt.Fprintln(w, "estimated size per regime unavailable")
		return
	}
	fmt.Fprintln(w, "estimated size per regime")
	for _, estimate := range result.Estimates {
		notLocal := ""
		if estimate.NotLocal > 0 {
			notLocal = fmt.Sprintf(" (%d files not local, size unknown)", estimate.NotLocal)
		}
		fmt.Fprintf(w, "%-10s %10s%s\n", estimate.Regime, common.ByteCount(estimate.Bytes), notLocal)
	}
}

func FormatCaplinDuJSON(w io.Writer, result CaplinDuResult) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(result)
}

func formatCaplinDuNumber(value uint64) string {
	return strconv.FormatUint(value, 10)
}
