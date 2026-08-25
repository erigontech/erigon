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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

var pbinConvertPairHook func()
var pbinConvertAfterBuildHook func(string)

// A pre-version branch record opens with the high byte of its touchMap, always
// zero; a current one opens with a cell-fields byte, which always carries a kind
// bit. One byte separates the two formats without decoding either.
func pbinRecordIsLegacy(value []byte) bool { return len(value) > 0 && value[0] == 0 }

func pbinStatePayload(value []byte) (payload []byte, wrapped bool, err error) {
	if commitment.IsPBinState(value) {
		return value, false, nil
	}
	if len(value) < 18 || !commitment.IsPBinState(value[18:]) {
		return nil, false, fmt.Errorf("pbin state value has no state blob")
	}
	rootLen := int(binary.BigEndian.Uint16(value[16:18]))
	if rootLen != len(value)-18 {
		return nil, false, fmt.Errorf("pbin state value length %d does not match root length %d", len(value), rootLen)
	}
	return value[18:], true, nil
}

func pbinConvertState(conv *commitment.PBinRecordConverter, value []byte) ([]byte, error) {
	payload, wrapped, err := pbinStatePayload(value)
	if err != nil {
		return nil, err
	}
	if commitment.ValidatePBinStateFormat(payload) == nil {
		return append([]byte(nil), value...), nil
	}
	converted, err := conv.ConvertState(payload)
	if err != nil {
		return nil, err
	}
	if !wrapped {
		return converted, nil
	}
	out := append([]byte(nil), value[:18]...)
	if len(converted) > 1<<16-1 {
		return nil, fmt.Errorf("converted pbin state blob is too large: %d bytes", len(converted))
	}
	binary.BigEndian.PutUint16(out[16:18], uint16(len(converted)))
	return append(out, converted...), nil
}

func pbinCurrentStateRoot(value []byte) ([]byte, error) {
	payload, _, err := pbinStatePayload(value)
	if err != nil {
		return nil, err
	}
	if err := commitment.ValidatePBinStateFormat(payload); err != nil {
		return nil, fmt.Errorf("pbin state is not in current format: %w", err)
	}
	trie := commitment.NewPBinPatriciaHashed(nil)
	defer trie.Release()
	if err := trie.SetState(payload); err != nil {
		return nil, fmt.Errorf("restore pbin state: %w", err)
	}
	root, err := trie.RootHash()
	if err != nil {
		return nil, fmt.Errorf("hash restored pbin state: %w", err)
	}
	return root, nil
}

func pbinVerifyStateConversion(conv *commitment.PBinRecordConverter, source, converted []byte) error {
	sourcePayload, _, err := pbinStatePayload(source)
	if err != nil {
		return err
	}
	var sourceRoot []byte
	if commitment.ValidatePBinStateFormat(sourcePayload) == nil {
		sourceRoot, err = pbinCurrentStateRoot(source)
	} else {
		sourceRoot, err = conv.LegacyStateRoot(sourcePayload)
	}
	if err != nil {
		return fmt.Errorf("read source state root: %w", err)
	}
	convertedRoot, err := pbinCurrentStateRoot(converted)
	if err != nil {
		return fmt.Errorf("read converted state root: %w", err)
	}
	if !bytes.Equal(sourceRoot, convertedRoot) {
		return fmt.Errorf("pbin state root mismatch: source %x, converted %x", sourceRoot, convertedRoot)
	}
	return nil
}

func verifyPBinPairCount(sourcePairs uint64, outputWords int) error {
	if outputWords%2 != 0 {
		return fmt.Errorf("pbin pair count: output has an odd word count %d", outputWords)
	}
	outputPairs := uint64(outputWords / 2)
	if outputPairs != sourcePairs {
		return fmt.Errorf("pbin pair count: source has %d pairs, output has %d pairs", sourcePairs, outputPairs)
	}
	return nil
}

type pbinLegacySample struct {
	pair   uint64
	key    []byte
	legacy []byte
}

func verifyPBinSamples(ctx context.Context, d *Domain, outputPath string, samples []pbinLegacySample) error {
	if len(samples) == 0 {
		return nil
	}

	decompressor, err := seg.NewDecompressor(outputPath)
	if err != nil {
		return fmt.Errorf("open sampled pbin output: %w", err)
	}
	defer decompressor.Close()

	reader := d.dataReader(decompressor)
	reader.Reset(0)
	converter := commitment.NewPBinRecordConverter()
	var key, value []byte
	sampleIdx := 0
	for pair := uint64(0); reader.HasNext(); pair++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		key, _ = reader.Next(key[:0])
		if !reader.HasNext() {
			return fmt.Errorf("pbin sample read-back: output has no value at pair %d", pair)
		}
		value, _ = reader.Next(value[:0])
		if sampleIdx >= len(samples) || samples[sampleIdx].pair != pair {
			continue
		}

		sample := samples[sampleIdx]
		if !bytes.Equal(key, sample.key) {
			return fmt.Errorf("pbin sample read-back: output key at pair %d is %x, want %x", pair, key, sample.key)
		}
		if err := converter.CompareLegacy(sample.key, sample.legacy, value); err != nil {
			return fmt.Errorf("pbin sample read-back at pair %d: %w", pair, err)
		}
		sampleIdx++
	}
	if sampleIdx != len(samples) {
		return fmt.Errorf("pbin sample read-back: output ended before sample at pair %d", samples[sampleIdx].pair)
	}
	return nil
}

func pbinFileHasLegacy(ctx context.Context, d *Domain, file *FilesItem) (bool, error) {
	reader := d.dataReader(file.decompressor)
	reader.Reset(0)
	var key, value []byte
	for reader.HasNext() {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		default:
		}
		key, _ = reader.Next(key[:0])
		if !reader.HasNext() {
			return false, errors.New("truncated commitment file: value missing")
		}
		value, _ = reader.Next(value[:0])
		if bytes.Equal(key, commitmentdb.KeyCommitmentState) {
			payload, _, err := pbinStatePayload(value)
			if err != nil || commitment.ValidatePBinStateFormat(payload) != nil {
				return true, nil
			}
			continue
		}
		if pbinRecordIsLegacy(value) {
			return true, nil
		}
	}
	return false, nil
}

func commitmentOutputPaths(d *Domain, stepFrom, stepTo kv.Step) []string {
	paths := []string{d.kvNewFilePathIn(d.dirs.SnapDomain, stepFrom, stepTo)}
	if d.Accessors.Has(statecfg.AccessorBTree) {
		paths = append(paths, d.kvBtAccessorNewFilePathIn(d.dirs.SnapDomain, stepFrom, stepTo))
	}
	if d.Accessors.Has(statecfg.AccessorHashMap) {
		paths = append(paths, d.kviAccessorNewFilePathIn(d.dirs.SnapDomain, stepFrom, stepTo))
	}
	if d.Accessors.Has(statecfg.AccessorExistence) {
		paths = append(paths, d.kvExistenceIdxNewFilePathIn(d.dirs.SnapDomain, stepFrom, stepTo))
	}
	return paths
}

func removeCommitmentOutputFiles(paths []string) error {
	for _, path := range paths {
		if err := dir.RemoveFile(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove %s: %w", path, err)
		}
	}
	return nil
}

func commitmentFilesForConversion(at *AggregatorRoTx) (VisibleFiles, error) {
	d := at.d[kv.CommitmentDomain].d
	filesByPath := make(map[string]VisibleFile)
	d.dirtyFiles.Scan(func(item *FilesItem) bool {
		if item.decompressor == nil || filepath.Ext(item.decompressor.FilePath()) != ".kv" {
			return true
		}
		file := visibleFile{
			startTxNum: item.startTxNum,
			endTxNum:   item.endTxNum,
			src:        item,
		}
		filesByPath[filepath.Clean(item.decompressor.FilePath())] = file
		return true
	})

	entries, err := os.ReadDir(d.dirs.SnapDomain)
	if err != nil {
		return nil, fmt.Errorf("enumerate commitment files: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".kv" || !strings.Contains(entry.Name(), d.FilenameBase) {
			continue
		}
		path := filepath.Clean(filepath.Join(d.dirs.SnapDomain, entry.Name()))
		if _, ok := filesByPath[path]; !ok {
			return nil, fmt.Errorf("commitment file %q is present on disk but is not readable", path)
		}
	}

	files := make(VisibleFiles, 0, len(filesByPath))
	for _, file := range filesByPath {
		files = append(files, file)
	}
	sort.Slice(files, func(i, j int) bool {
		if files[i].StartRootNum() != files[j].StartRootNum() {
			return files[i].StartRootNum() < files[j].StartRootNum()
		}
		return files[i].Fullpath() < files[j].Fullpath()
	})
	return files, nil
}

func commitmentOutputComplete(paths []string) (bool, error) {
	for _, path := range paths {
		if _, err := os.Stat(path); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return false, nil
			}
			return false, fmt.Errorf("stat %s: %w", path, err)
		}
	}
	return true, nil
}

func convertPBinFile(ctx context.Context, at *AggregatorRoTx, file VisibleFile, logger log.Logger, verifySample uint64) (pairs uint64, err error) {
	vf, ok := file.(visibleFile)
	if !ok {
		return 0, fmt.Errorf("convertPBinFile %q: VisibleFile is not state.visibleFile (got %T)", file.Fullpath(), file)
	}
	if vf.src == nil || vf.src.decompressor == nil {
		return 0, fmt.Errorf("convertPBinFile %q: source has no decompressor", file.Fullpath())
	}

	d := at.d[kv.CommitmentDomain].d
	stepSize := at.StepSize()
	stepFrom, stepTo := kv.Step(file.StartRootNum()/stepSize), kv.Step(file.EndRootNum()/stepSize)
	outputPath := d.kvNewFilePathIn(d.dirs.SnapDomain, stepFrom, stepTo)
	if filepath.Base(outputPath) != filepath.Base(file.Fullpath()) {
		return 0, fmt.Errorf("convertPBinFile %q: output basename %q does not match source basename %q", file.Fullpath(), filepath.Base(outputPath), filepath.Base(file.Fullpath()))
	}
	paths := commitmentOutputPaths(d, stepFrom, stepTo)
	cleanupOutput := true
	defer func() {
		if cleanupOutput {
			if cleanupErr := removeCommitmentOutputFiles(paths); cleanupErr != nil && err == nil {
				err = cleanupErr
			}
		}
	}()

	hasLegacy, err := pbinFileHasLegacy(ctx, d, vf.src)
	if err != nil {
		return 0, fmt.Errorf("convertPBinFile %q: classify: %w", file.Fullpath(), err)
	}
	if !hasLegacy {
		complete, err := commitmentOutputComplete(paths)
		if err != nil {
			return 0, fmt.Errorf("convertPBinFile %q: check output: %w", file.Fullpath(), err)
		}
		if complete {
			cleanupOutput = false
			return 0, errSkip
		}
	}
	sourceWords := vf.src.decompressor.Count()
	if sourceWords%2 != 0 {
		return 0, fmt.Errorf("convertPBinFile %q: source has an odd word count %d", file.Fullpath(), sourceWords)
	}
	sourcePairs := uint64(sourceWords / 2)

	if err := removeCommitmentOutputFiles(paths); err != nil {
		return 0, err
	}

	comp, err := seg.NewCompressor(ctx, "pbin_convert", outputPath, d.dirs.Tmp, d.CompressCfg, log.LvlTrace, logger)
	if err != nil {
		return 0, fmt.Errorf("convertPBinFile %q: create compressor: %w", file.Fullpath(), err)
	}
	compOwned := true
	defer func() {
		if compOwned {
			comp.Close()
		}
	}()
	writer := d.dataWriter(comp, false)
	reader := d.dataReader(vf.src.decompressor)
	reader.Reset(0)
	converter := commitment.NewPBinRecordConverter()
	var legacyBranches uint64
	var samples []pbinLegacySample
	var key, value []byte
	for reader.HasNext() {
		key, _ = reader.Next(key[:0])
		if !reader.HasNext() {
			return pairs, fmt.Errorf("convertPBinFile %q: truncated at pair %d (value missing)", file.Fullpath(), pairs)
		}
		value, _ = reader.Next(value[:0])
		if pbinConvertPairHook != nil {
			pbinConvertPairHook()
		}
		select {
		case <-ctx.Done():
			return pairs, ctx.Err()
		default:
		}
		var outputValue []byte
		switch {
		case bytes.Equal(key, commitmentdb.KeyCommitmentState):
			outputValue, err = pbinConvertState(converter, value)
			if err == nil {
				err = pbinVerifyStateConversion(converter, value, outputValue)
			}
		case pbinRecordIsLegacy(value):
			outputValue, err = converter.ConvertBranch(key, value)
			if err == nil {
				legacyBranches++
				if verifySample > 0 && legacyBranches%verifySample == 0 {
					samples = append(samples, pbinLegacySample{
						pair:   pairs,
						key:    append([]byte(nil), key...),
						legacy: append([]byte(nil), value...),
					})
				}
			}
		default:
			outputValue = append([]byte(nil), value...)
		}
		if err != nil {
			return pairs, fmt.Errorf("convertPBinFile %q: pair %d key=%x: %w", file.Fullpath(), pairs, key, err)
		}
		if _, err = writer.Write(key); err != nil {
			return pairs, fmt.Errorf("convertPBinFile %q: write key at pair %d: %w", file.Fullpath(), pairs, err)
		}
		if _, err = writer.Write(outputValue); err != nil {
			return pairs, fmt.Errorf("convertPBinFile %q: write value at pair %d: %w", file.Fullpath(), pairs, err)
		}
		pairs++
		select {
		case <-ctx.Done():
			return pairs, ctx.Err()
		default:
		}
	}

	coll := Collation{valuesComp: comp, valuesPath: outputPath, valuesCount: comp.Count() / 2}
	if err := verifyPBinPairCount(sourcePairs, coll.valuesComp.Count()); err != nil {
		return pairs, fmt.Errorf("convertPBinFile %q: %w", file.Fullpath(), err)
	}
	static, err := d.buildFileRange(ctx, stepFrom, stepTo, coll, background.NewProgressSet(), d.dirs.SnapDomain)
	compOwned = false
	if err != nil {
		return pairs, fmt.Errorf("convertPBinFile %q: build output: %w", file.Fullpath(), err)
	}
	static.CleanupOnError()
	if pbinConvertAfterBuildHook != nil {
		pbinConvertAfterBuildHook(outputPath)
	}
	if err := verifyPBinSamples(ctx, d, outputPath, samples); err != nil {
		return pairs, fmt.Errorf("convertPBinFile %q: %w", file.Fullpath(), err)
	}
	cleanupOutput = false
	logger.Info("[pbin_convert] converted", "file", filepath.Base(file.Fullpath()), "pairs", pairs)
	return pairs, nil
}

// ConvertPBinRecordFiles rewrites pre-version pbin commitment files in the
// output datadir. Files already in the current format remain hardlinks to the
// source datadir; converted files replace those links before they are written.
func ConvertPBinRecordFiles(ctx context.Context, at *AggregatorRoTx, logger log.Logger, verifySample ...uint64) error {
	if len(verifySample) > 1 {
		return fmt.Errorf("pbin conversion: expected at most one verify sample stride, got %d", len(verifySample))
	}
	var sampleStride uint64
	if len(verifySample) == 1 {
		sampleStride = verifySample[0]
	}
	files, err := commitmentFilesForConversion(at)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		logger.Info("[pbin_convert] no commitment files to convert")
		return nil
	}

	for _, file := range files {
		if _, err := convertPBinFile(ctx, at, file, logger, sampleStride); err != nil {
			if errors.Is(err, errSkip) {
				logger.Info("[pbin_convert] already current", "file", filepath.Base(file.Fullpath()))
				continue
			}
			return err
		}
	}
	return nil
}
