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
	"cmp"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/recsplit/multiencseq"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/statecfg"
)

// HistoryPatch sets the value that Key had before its change at TxNum. When the history of
// Key has no change at TxNum, the change is added.
type HistoryPatch struct {
	Key   []byte
	TxNum uint64
	Value []byte
}

// LatestPatch sets the value of Key in the newest domain file that has Key.
type LatestPatch struct {
	Key, Value []byte
}

type patchedRange struct {
	from, to kv.Step
	exts     []string
}

var (
	historyFileExts = []string{"v", "vi", "ef", "efi"}
	domainFileExts  = []string{"kv", "bt", "kvei", "kvi"}
)

// PatchDomainFiles rewrites the history and domain files that the patches touch, moves the
// replaced files to snapshots/patch-backup and reloads the files. The caller must not hold
// readers of the aggregator's files.
func (a *Aggregator) PatchDomainFiles(ctx context.Context, name kv.Domain, history []HistoryPatch, latest []LatestPatch, logger log.Logger) error {
	if len(history) == 0 && len(latest) == 0 {
		return nil
	}
	stage, err := a.newPatchStage()
	if err != nil {
		return err
	}
	defer dir.RemoveAll(stage) //nolint:errcheck
	ranges, staged, err := a.writePatchedFiles(ctx, name, history, latest, stage, logger)
	if err != nil {
		return err
	}
	return a.replaceFiles(name, ranges, staged, logger)
}

func (a *Aggregator) newPatchStage() (string, error) {
	stage := filepath.Join(a.dirs.Snap, "patch")
	if err := dir.RemoveAll(stage); err != nil {
		return "", err
	}
	return stage, os.MkdirAll(stage, 0o755)
}

// replaceFiles moves the files of ranges to snapshots/patch-backup, moves the staged files
// to their live paths and reloads the files.
func (a *Aggregator) replaceFiles(name kv.Domain, ranges []patchedRange, staged map[string]string, logger log.Logger) error {
	a.CloseFilesNoReopen()
	backup := filepath.Join(a.dirs.Snap, "patch-backup", time.Now().UTC().Format("20060102-150405"))
	if err := os.MkdirAll(backup, 0o755); err != nil {
		return err
	}
	base := a.d[name].FilenameBase
	for _, r := range ranges {
		if err := movePatchedOriginals(a.dirs, backup, base, r); err != nil {
			return err
		}
	}
	for src, dst := range staged {
		if err := os.Rename(src, dst); err != nil {
			return fmt.Errorf("promote %s: %w", src, err)
		}
	}
	logger.Info("[patch] files replaced", "domain", name, "ranges", len(ranges), "backup", backup)
	return a.ReloadFiles()
}

// writePatchedFiles writes the patched files to stage and returns the replaced ranges and
// the live path of each staged file.
func (a *Aggregator) writePatchedFiles(ctx context.Context, name kv.Domain, history []HistoryPatch, latest []LatestPatch, stage string, logger log.Logger) ([]patchedRange, map[string]string, error) {
	at := a.BeginFilesRo()
	defer at.Close()
	dt := at.d[name]
	staged := map[string]string{}
	var ranges []patchedRange

	vs, efs, err := historyFilePairs(dt)
	if err != nil {
		return nil, nil, err
	}
	byFile := make(map[int][]HistoryPatch)
	for _, p := range history {
		i := slices.IndexFunc(vs, func(f visibleFile) bool { return f.startTxNum <= p.TxNum && p.TxNum < f.endTxNum })
		if i < 0 {
			return nil, nil, fmt.Errorf("%s history of %x at txNum %d is not in files", name, p.Key, p.TxNum)
		}
		byFile[i] = append(byFile[i], p)
	}
	for i, patches := range byFile {
		r, err := rewriteHistoryFile(ctx, dt.ht, vs[i], efs[i], patches, stage, staged, logger)
		if err != nil {
			return nil, nil, fmt.Errorf("patch %s: %w", vs[i].Fullpath(), err)
		}
		ranges = append(ranges, r)
	}

	byKV := make(map[int][]LatestPatch)
	for _, p := range latest {
		_, found, startTxNum, endTxNum, err := dt.getLatestFromFiles(p.Key, nil, kv.NoStepBound)
		if err != nil {
			return nil, nil, err
		}
		if !found {
			return nil, nil, fmt.Errorf("%s latest of %x is not in files", name, p.Key)
		}
		i := slices.IndexFunc(dt.files, func(f visibleFile) bool { return f.startTxNum == startTxNum && f.endTxNum == endTxNum })
		byKV[i] = append(byKV[i], p)
	}
	for i, patches := range byKV {
		r, err := rewriteDomainFile(ctx, dt, dt.files[i], patches, stage, staged, logger)
		if err != nil {
			return nil, nil, fmt.Errorf("patch %s: %w", dt.files[i].Fullpath(), err)
		}
		ranges = append(ranges, r)
	}
	return ranges, staged, nil
}

func historyFilePairs(dt *DomainRoTx) (vs, efs []visibleFile, err error) {
	for _, v := range dt.ht.files {
		j := slices.IndexFunc(dt.ht.iit.files, func(ef visibleFile) bool { return ef.startTxNum == v.startTxNum && ef.endTxNum == v.endTxNum })
		if j < 0 {
			return nil, nil, fmt.Errorf("%s has no .ef of the same range", v.Fullpath())
		}
		vs, efs = append(vs, v), append(efs, dt.ht.iit.files[j])
	}
	return vs, efs, nil
}

// rewriteHistoryFile copies a .v/.ef pair, with the values of the patched txNums replaced
// and the missing txNums added, and builds their accessors.
func rewriteHistoryFile(ctx context.Context, ht *HistoryRoTx, vFile, efFile visibleFile, patches []HistoryPatch, stage string, staged map[string]string, logger log.Logger) (patchedRange, error) {
	h, ii := ht.h, ht.h.InvertedIndex
	r := patchedRange{from: kv.Step(vFile.startTxNum / ht.stepSize), to: kv.Step(vFile.endTxNum / ht.stepSize), exts: historyFileExts}
	if ii.Accessors != statecfg.AccessorHashMap {
		return r, fmt.Errorf("%s: inverted index accessors %v are not supported", ii.FilenameBase, ii.Accessors)
	}
	vPath := stagePath(stage, staged, h.vNewFilePath(r.from, r.to))
	viPath := stagePath(stage, staged, h.vAccessorNewFilePath(r.from, r.to))
	efPath := stagePath(stage, staged, ii.efNewFilePath(r.from, r.to))
	efiPath := stagePath(stage, staged, ii.efAccessorNewFilePath(r.from, r.to))
	if err := writePatchedHistory(ctx, h, vFile, efFile, patches, vPath, efPath, logger); err != nil {
		return r, err
	}
	if err := buildHistoryAccessors(ctx, h, vFile.startTxNum, vPath, viPath, efPath, efiPath); err != nil {
		return r, err
	}
	logger.Info("[patch] history file written", "file", filepath.Base(vPath), "patches", len(patches))
	return r, nil
}

func writePatchedHistory(ctx context.Context, h *History, vFile, efFile visibleFile, patches []HistoryPatch, vPath, efPath string, logger log.Logger) error {
	ii := h.InvertedIndex
	vComp, compErr := seg.NewCompressor(ctx, "patch .v", vPath, h.dirs.Tmp, h.CompressorCfg, log.LvlTrace, logger)
	if compErr != nil {
		return compErr
	}
	defer vComp.Close()
	efComp, compErr := seg.NewCompressor(ctx, "patch .ef", efPath, h.dirs.Tmp, ii.CompressorCfg, log.LvlTrace, logger)
	if compErr != nil {
		return compErr
	}
	defer efComp.Close()
	vView, viewErr := vFile.src.decompressor.OpenSequentialView()
	if viewErr != nil {
		return viewErr
	}
	defer vView.Close()
	efView, viewErr := efFile.src.decompressor.OpenSequentialView()
	if viewErr != nil {
		return viewErr
	}
	defer efView.Close()

	w := &historyPatchWriter{
		fromTxNum: vFile.startTxNum,
		patches: slices.SortedFunc(slices.Values(patches), func(a, b HistoryPatch) int {
			return cmp.Or(bytes.Compare(a.Key, b.Key), cmp.Compare(a.TxNum, b.TxNum))
		}),
		v:  h.dataWriter(ctx, vComp),
		ef: ii.dataWriter(efComp, false),
	}
	efReader := seg.NewReader(efView.MakeGetter(), ii.Compression)
	vReader := seg.NewPagedReader(seg.NewReader(vView.MakeGetter(), h.Compression), int(h.valuesPerPage(vFile.src.decompressor)), true)
	var (
		seq            multiencseq.SequenceReader
		it             multiencseq.SequenceIterator
		key, efVal, vb []byte
	)
	for efReader.HasNext() {
		key, _ = efReader.Next(key[:0])
		if !efReader.HasNext() {
			return fmt.Errorf("%s truncated at key %x", efFile.Fullpath(), key)
		}
		efVal, _ = efReader.Next(efVal[:0])
		if err := w.writeNewKeys(key); err != nil {
			return err
		}
		var txNums []uint64
		var values [][]byte
		seq.Reset(vFile.startTxNum, efVal)
		it.Reset(&seq, 0)
		for it.HasNext() {
			txNum, err := it.Next()
			if err != nil {
				return err
			}
			if !vReader.HasNext() {
				return fmt.Errorf("%s has no value for %x at txNum %d", vFile.Fullpath(), key, txNum)
			}
			var v []byte
			_, v, vb, _ = vReader.Next2(vb[:0])
			txNums, values = append(txNums, txNum), append(values, bytes.Clone(v))
		}
		if err := w.writePatched(key, txNums, values); err != nil {
			return err
		}
	}
	if vReader.HasNext() {
		return fmt.Errorf("%s has values past the last .ef entry", vFile.Fullpath())
	}
	if err := w.writeNewKeys(nil); err != nil {
		return err
	}
	if err := w.v.Compress(); err != nil {
		return err
	}
	return w.ef.Compress()
}

type historyPatchWriter struct {
	fromTxNum uint64
	patches   []HistoryPatch // sorted by key and txNum; consumed while writing
	v         *seg.PagedWriter
	ef        *seg.Writer
	builder   multiencseq.SequenceBuilder
	efBuf     []byte
	histKey   []byte
}

// writeNewKeys writes the patched keys that sort before key and that the file does not
// have. A nil key writes all of them.
func (w *historyPatchWriter) writeNewKeys(key []byte) error {
	for len(w.patches) > 0 && (key == nil || bytes.Compare(w.patches[0].Key, key) < 0) {
		if err := w.writePatched(w.patches[0].Key, nil, nil); err != nil {
			return err
		}
	}
	return nil
}

// writePatched writes the changes of one key with the patches of that key applied.
func (w *historyPatchWriter) writePatched(key []byte, txNums []uint64, values [][]byte) error {
	for len(w.patches) > 0 && bytes.Equal(w.patches[0].Key, key) {
		p := w.patches[0]
		w.patches = w.patches[1:]
		i, found := slices.BinarySearch(txNums, p.TxNum)
		if found {
			values[i] = p.Value
			continue
		}
		txNums = slices.Insert(txNums, i, p.TxNum)
		values = slices.Insert(values, i, p.Value)
	}
	w.builder.Reset(w.fromTxNum, uint64(len(txNums)), txNums[len(txNums)-1])
	for i, txNum := range txNums {
		w.builder.AddOffset(txNum)
		w.histKey = historyKey(txNum, key, w.histKey)
		if err := w.v.Add(w.histKey, values[i]); err != nil {
			return err
		}
	}
	w.builder.Build()
	w.efBuf = w.builder.AppendBytes(w.efBuf[:0])
	if _, err := w.ef.Write(key); err != nil {
		return err
	}
	_, err := w.ef.Write(w.efBuf)
	return err
}

func buildHistoryAccessors(ctx context.Context, h *History, fromTxNum uint64, vPath, viPath, efPath, efiPath string) error {
	ps := background.NewProgressSet()
	efDecomp, openErr := seg.NewDecompressor(efPath)
	if openErr != nil {
		return openErr
	}
	defer efDecomp.Close()
	vDecomp, openErr := seg.NewDecompressor(vPath)
	if openErr != nil {
		return openErr
	}
	defer vDecomp.Close()
	if err := h.InvertedIndex.buildMapAccessorAt(ctx, efiPath, efDecomp, ps); err != nil {
		return err
	}
	return h.buildVI(ctx, viPath, vDecomp, efDecomp, fromTxNum, ps)
}

// rewriteDomainFile copies a .kv file with the values of the patched keys replaced, and
// builds its accessors.
func rewriteDomainFile(ctx context.Context, dt *DomainRoTx, f visibleFile, patches []LatestPatch, stage string, staged map[string]string, logger log.Logger) (patchedRange, error) {
	d := dt.d
	r := patchedRange{from: kv.Step(f.startTxNum / dt.stepSize), to: kv.Step(f.endTxNum / dt.stepSize), exts: domainFileExts}
	values := make(map[string][]byte, len(patches))
	for _, p := range patches {
		values[string(p.Key)] = p.Value
	}

	kvPath := d.kvNewFilePathIn(stage, r.from, r.to)
	comp, compErr := seg.NewCompressor(ctx, "patch .kv", kvPath, d.dirs.Tmp, d.CompressCfg, log.LvlTrace, logger)
	if compErr != nil {
		return r, compErr
	}
	w := d.dataWriter(comp, r.to-r.from < DomainMinStepsToCompress)
	reader := d.dataReader(f.src.decompressor)
	reader.Reset(0)
	var k, v []byte
	for reader.HasNext() {
		k, _ = reader.Next(k[:0])
		v, _ = reader.Next(v[:0])
		if pv, ok := values[string(k)]; ok {
			v = pv
			delete(values, string(k))
		}
		if _, err := w.Write(k); err != nil {
			comp.Close()
			return r, err
		}
		if _, err := w.Write(v); err != nil {
			comp.Close()
			return r, err
		}
	}
	if len(values) > 0 {
		comp.Close()
		return r, fmt.Errorf("%d patched keys are not in %s", len(values), f.Fullpath())
	}
	sf, buildErr := d.buildFileRange(ctx, r.from, r.to, Collation{valuesComp: comp, valuesPath: kvPath}, background.NewProgressSet(), stage)
	if buildErr != nil {
		return r, buildErr
	}
	sf.CleanupOnError()
	for _, p := range []string{kvPath, d.kvBtAccessorNewFilePathIn(stage, r.from, r.to), d.kvExistenceIdxNewFilePathIn(stage, r.from, r.to), d.kviAccessorNewFilePathIn(stage, r.from, r.to)} {
		if exists, err := dir.FileExist(p); err != nil {
			return r, err
		} else if exists {
			staged[p] = filepath.Join(d.dirs.SnapDomain, filepath.Base(p))
		}
	}
	logger.Info("[patch] domain file written", "file", filepath.Base(kvPath), "patches", len(patches))
	return r, nil
}

// stagePath returns the stage path of a file that goes to live, and records it in staged.
func stagePath(stage string, staged map[string]string, live string) string {
	p := filepath.Join(stage, filepath.Base(live))
	staged[p] = live
	return p
}

func movePatchedOriginals(dirs datadir.Dirs, backup, base string, r patchedRange) error {
	snapDirs := []string{dirs.SnapDomain, dirs.SnapHistory, dirs.SnapIdx, dirs.SnapAccessors}
	patterns := make([]string, 0, 2*len(r.exts)*len(snapDirs))
	for _, d := range snapDirs {
		for _, ext := range r.exts {
			pattern := filepath.Join(d, fmt.Sprintf("*-%s.%d-%d.%s", base, r.from, r.to, ext))
			patterns = append(patterns, pattern, pattern+".torrent")
		}
	}
	for _, p := range patterns {
		matches, err := dir.Glob(p)
		if err != nil {
			return err
		}
		for _, src := range matches {
			dst := filepath.Join(backup, filepath.Base(filepath.Dir(src)), filepath.Base(src))
			if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
				return err
			}
			if err := os.Rename(src, dst); err != nil {
				return fmt.Errorf("back up %s: %w", src, err)
			}
		}
	}
	return nil
}
