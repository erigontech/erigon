package app

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/config3"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/recsplit"
	"github.com/erigontech/erigon-lib/recsplit/multiencseq"
	"github.com/erigontech/erigon-lib/seg"
	"github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/turbo/debug"
	"github.com/urfave/cli/v2"
)

func historyDedup(cliCtx *cli.Context) error {
	dirs := datadir.New(cliCtx.String(utils.DataDirFlag.Name))
	logger, _, _, _, err := debug.Setup(cliCtx, true /* rootLogger */)
	if err != nil {
		return err
	}
	ctx := cliCtx.Context

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	salt, err := state.GetStateIndicesSalt(dirs, false, logger)
	if err != nil {
		return err
	}

	vfile := cliCtx.String(utils.VFileFlag.Name)
	if err := dedupV(ctx, dirs, vfile, background.NewProgressSet(), logger, salt, ticker); err != nil {
		return err
	}
	return nil
}

func historyDedupAll(cliCtx *cli.Context) error {
	dirs := datadir.New(cliCtx.String(utils.DataDirFlag.Name))
	logger, _, _, _, err := debug.Setup(cliCtx, true /* rootLogger */)
	if err != nil {
		return err
	}
	ctx := cliCtx.Context

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	salt, err := state.GetStateIndicesSalt(dirs, false, logger)
	if err != nil {
		return err
	}

	files, err := fs.ReadDir(os.DirFS(dirs.SnapHistory), ".")
	if err != nil {
		return err
	}

	logger.Info("Deduping .v files...")
	for _, file := range files {
		if file.IsDir() || (!strings.HasPrefix(file.Name(), "v1.0-accounts") && !strings.HasPrefix(file.Name(), "v1.0-storage")) || !strings.HasSuffix(file.Name(), ".v") {
			continue
		}
		if err := dedupV(ctx, dirs, file.Name(), background.NewProgressSet(), logger, salt, ticker); err != nil {
			return err
		}
	}

	return nil
}

func parseVFilename(fileName string) (*vFileInfo, error) {
	partsByDot := strings.Split(fileName, ".")
	partsByDash := strings.Split(partsByDot[0], "-")
	stepParts := strings.Split(partsByDot[2], "-")
	startStep, err := strconv.ParseUint(stepParts[0], 10, 64)
	if err != nil {
		return nil, err
	}
	endStep, err := strconv.ParseUint(stepParts[1], 10, 64)
	if err != nil {
		return nil, err
	}

	return &vFileInfo{
		prefix:    partsByDash[0],
		stepSize:  endStep - startStep,
		startStep: startStep,
		endStep:   endStep,
	}, nil
}

type vFileInfo struct {
	prefix    string
	stepSize  uint64
	startStep uint64
	endStep   uint64
}

// Based on History.dedupV
func dedupV(ctx context.Context, dirs datadir.Dirs, vfile string, ps *background.ProgressSet, logger log.Logger, salt *uint32, ticker *time.Ticker) error {
	parts, err := parseVFilename(vfile)
	if err != nil {
		return err
	}
	if parts.stepSize != config3.StepsInFrozenFile {
		logger.Info("Not a frozen file; skipping", "file", vfile)
		return nil
	}
	efBaseTxNum := parts.startStep * config3.DefaultStepSize

	// Corresponding .ef file for reindexing
	// TODO: need some smarter way to link .v -> .ef versioning aware
	efFilename := "v2.0" + strings.TrimPrefix(strings.TrimSuffix(vfile, ".v")+".ef", "v1.0")
	efDecomp, err := seg.NewDecompressor(filepath.Join(dirs.SnapIdx, efFilename))
	if err != nil {
		return err
	}
	defer efDecomp.Close()

	// Original .v file
	origVDecomp, err := seg.NewDecompressor(filepath.Join(dirs.SnapHistory, vfile))
	if err != nil {
		return err
	}
	defer origVDecomp.Close()

	// Dedupped .v file
	newVFile := filepath.Join(dirs.SnapHistory, vfile+".new")
	newViFile := filepath.Join(dirs.SnapAccessors, vfile+"i.new")

	logger.Info("Deduping history file", "file", origVDecomp.FilePath())

	var histKey []byte

	iiReader := seg.NewReader(efDecomp.MakeGetter(), seg.CompressNone)

	// Dedupped .v
	dedupedVComp, err := seg.NewCompressor(ctx, "compressor", newVFile, dirs.Tmp, seg.DefaultCfg, log.LvlInfo, logger)
	if err != nil {
		return err
	}
	defer dedupedVComp.Close()

	dedupedVWriter := seg.NewWriter(dedupedVComp, seg.CompressNone)
	defer dedupedVWriter.Close()

	// Count expected words to setup new .vi rs
	var keyBuf, valBuf []byte
	vCount := uint64(0)
	for iiReader.HasNext() {
		keyBuf, _ = iiReader.Next(keyBuf[:0]) // skip key
		valBuf, _ = iiReader.Next(valBuf[:0])
		vCount += multiencseq.Count(efBaseTxNum, valBuf)
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}

	// Integrity check: .v word count MUST match number of idx touches in corresponding .ef file, otherwise it
	// means either:
	//
	// - .v file is already deduped
	// - .v/.ef pair is corrupted by some bug
	if vCount != uint64(origVDecomp.Count()) {
		logger.Warn("Value count mismatch: expected .ef %d != .v %d", vCount, origVDecomp.Count())
		logger.Warn("File is either already deduped or corrupted; skipping...", "file", vfile)
		return nil
	}

	_, fName := filepath.Split(newViFile)
	p := ps.AddNew(fName, uint64(efDecomp.Count())/2)
	defer ps.Delete(p)
	rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   int(vCount),
		Enums:      false,
		BucketSize: recsplit.DefaultBucketSize,
		LeafSize:   recsplit.DefaultLeafSize,
		TmpDir:     dirs.Tmp,
		IndexFile:  newViFile,
		Salt:       salt,
		NoFsync:    false,
	}, logger)
	if err != nil {
		return fmt.Errorf("create recsplit: %w", err)
	}
	defer rs.Close()
	rs.LogLvl(log.LvlTrace)

	seq := &multiencseq.SequenceReader{}

	// Write a new .v dedupping the original .v
	logger.Info("Writing tmp deduped .v...")
	origVReader := seg.NewReader(origVDecomp.MakeGetter(), seg.CompressNone)
	origVReader.Reset(0)
	origWordCount := origVReader.Count()

	uniqMap := make(map[string]bool)
	c := uint64(0)
	for origVReader.HasNext() {
		p.Processed.Add(1)
		buf, _ := origVReader.NextUncompressed()
		c++

		// skip dup values
		_, ok := uniqMap[string(buf)]
		if ok {
			continue
		}

		uniqMap[string(buf)] = true
		if err := dedupedVWriter.AddUncompressedWord(buf); err != nil {
			return err
		}

		select {
		case <-ticker.C:
			logger.Info("Deduping", progress("wordCount", c, uint64(origWordCount))...)
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}

	// Flush/write/close dedupped .v
	if err := dedupedVComp.Compress(); err != nil {
		if err != nil {
			return err
		}
	}
	dedupedVComp.Close()
	logger.Info("Deduping finished", "originalWordCount", origWordCount, "dedupedWordCount", c)

	// Reopen/scan the dedupped .v and map content -> offsets
	logger.Info("Mapping deduped offsets...")
	dedupedVDecomp, err := seg.NewDecompressor(newVFile)
	if err != nil {
		return err
	}
	defer dedupedVDecomp.Close()

	dedupedVReader := seg.NewReader(dedupedVDecomp.MakeGetter(), seg.CompressNone)
	offset := uint64(0)
	dedupedVReader.Reset(offset)
	offsetMap := make(map[string]uint64)
	c = 0
	for dedupedVReader.HasNext() {
		buf, newOffset := dedupedVReader.NextUncompressed()
		offsetMap[string(buf)] = offset
		offset = newOffset
		c++

		select {
		case <-ticker.C:
			logger.Info("Mapping", progress("dedupedWordCount", c, uint64(dedupedVReader.Count()))...)
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}

	// Reindex new .vi with dedupped offsets
	logger.Info("Reindexing deduped .vi...")
	for {
		origVReader.Reset(0)
		iiReader.Reset(0)

		c = 0
		for iiReader.HasNext() {
			keyBuf, _ = iiReader.Next(keyBuf[:0])
			valBuf, _ = iiReader.Next(valBuf[:0])
			p.Processed.Add(1)

			seq.Reset(efBaseTxNum, valBuf)
			it := seq.Iterator(0)
			for it.HasNext() {
				txNum, err := it.Next()
				if err != nil {
					return err
				}
				histKey = state.HistoryKey(txNum, keyBuf, histKey[:0])

				buf, _ := origVReader.NextUncompressed()
				valOffset, ok := offsetMap[string(buf)]
				if !ok {
					panic("should not happen")
				}
				if err = rs.AddKey(histKey, valOffset); err != nil {
					return err
				}
				c++
			}

			select {
			case <-ticker.C:
				logger.Info("Reindexing", progress("wordCount", c, uint64(origWordCount))...)
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}

		logger.Info("Building new index...")
		if err = rs.Build(ctx); err != nil {
			if rs.Collision() {
				log.Info("Building recsplit. Collision happened. It's ok. Restarting...")
				rs.ResetNextSalt()
			} else {
				return fmt.Errorf("build idx: %w", err)
			}
		} else {
			break
		}
	}
	rs.Close()

	logger.Info("Finished deduping history file", "file", origVDecomp.FilePath())

	// Verify contents of generated .v/.vi
	logger.Info("Verifying generated history contents...")

	idx, err := recsplit.OpenIndex(newViFile)
	if err != nil {
		return err
	}
	defer idx.Close()
	newViReader := idx.GetReaderFromPool()
	defer newViReader.Close()

	origVReader.Reset(0)
	iiReader.Reset(0)
	c = 0
	for iiReader.HasNext() {
		keyBuf, _ = iiReader.Next(keyBuf[:0])
		valBuf, _ = iiReader.Next(valBuf[:0])

		seq.Reset(efBaseTxNum, valBuf)
		it := seq.Iterator(0)
		for it.HasNext() {
			txNum, err := it.Next()
			if err != nil {
				return err
			}
			histKey = state.HistoryKey(txNum, keyBuf, histKey[:0])
			expectedVal, _ := origVReader.NextUncompressed()

			offset, ok := newViReader.Lookup(histKey)
			if !ok {
				return fmt.Errorf("couldn't find history key in deduped index: %s", hexutil.Encode(histKey))
			}
			dedupedVReader.Reset(offset)
			newVal, _ := dedupedVReader.NextUncompressed()
			if !bytes.Equal(newVal, expectedVal) {
				return fmt.Errorf("deduped value doesn't match the expected one: histKey=%s expected=%s new=%s", hexutil.Encode(histKey), hexutil.Encode(expectedVal), hexutil.Encode(newVal))
			}
			c++

			select {
			case <-ticker.C:
				logger.Info("Verifying", progress("wordCount", c, uint64(origWordCount))...)
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
	}

	// Close everything in order to move files
	newViReader.Close()
	idx.Close()
	dedupedVDecomp.Close()
	origVDecomp.Close()
	efDecomp.Close()

	// Replace the original .v/.vi with the new ones
	logger.Info("Replacing original .v/.vi with the new ones...")

	vFileOrig := filepath.Join(dirs.SnapHistory, vfile)
	vFileBackup, err := backupFile(vFileOrig)
	if err != nil {
		return err
	}
	logger.Info(fmt.Sprintf("Backup %s => %s", vFileOrig, vFileBackup))

	viFileOrig := filepath.Join(dirs.SnapAccessors, vfile+"i")
	viFileBackup, err := backupFile(viFileOrig)
	if err != nil {
		return err
	}
	logger.Info(fmt.Sprintf("Backup %s => %s", viFileOrig, viFileBackup))

	if err := os.Rename(newVFile, vFileOrig); err != nil {
		return err
	}
	if err := os.Rename(newViFile, viFileOrig); err != nil {
		return err
	}
	return nil
}

func backupFile(file string) (string, error) {
	backupFilename := file + ".prededup"
	i := 1
	actualBackupFilename := backupFilename
	for {
		_, err := os.Stat(actualBackupFilename)
		if err == nil {
			actualBackupFilename = backupFilename + "." + strconv.Itoa(i)
			i++
			continue
		}
		if os.IsNotExist(err) {
			break
		}
		return "", fmt.Errorf("failed to stat %s: %w", file, err)
	}
	if err := os.Rename(file, actualBackupFilename); err != nil {
		return "", err
	}
	return actualBackupFilename, nil
}

func progress(k string, p, t uint64) []interface{} {
	r := make([]interface{}, 0, 4)
	r = append(r, k)
	r = append(r, fmt.Sprintf("%d/%d", p, t))
	r = append(r, k+"_completed")
	r = append(r, fmt.Sprintf("%.2f%%", float64(p)/float64(t)*100))
	return r
}
