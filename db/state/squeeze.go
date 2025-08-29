package state

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/erigontech/erigon/db/state/statecfg"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/stream"
	"github.com/erigontech/erigon/db/seg"
	downloadertype "github.com/erigontech/erigon/db/snaptype"
)

//Sqeeze: ForeignKeys-aware compression of file

// Sqeeze - re-compress file
// Breaks squeezed commitment files totally
// TODO: care of ForeignKeys
func (a *Aggregator) Sqeeze(ctx context.Context, domain kv.Domain) error {
	filesToRemove := []string{}
	for _, to := range domainFiles(a.dirs, domain) {
		_, fileName := filepath.Split(to)
		res, _, _ := downloadertype.ParseFileName("", fileName)
		if res.To-res.From < DomainMinStepsToCompress {
			continue
		}

		tempFileCopy := filepath.Join(a.dirs.Tmp, fileName)
		if err := datadir.CopyFile(to, tempFileCopy); err != nil {
			return err
		}

		if err := a.sqeezeDomainFile(ctx, domain, tempFileCopy, to); err != nil {
			return err
		}

		// a.squeezeCommitmentFile(ctx, domain)

		filesToRemove = append(filesToRemove,
			tempFileCopy,
			strings.ReplaceAll(to, ".kv", ".kv.torrent"),
			strings.ReplaceAll(to, ".kv", ".bt"),
			strings.ReplaceAll(to, ".kv", ".bt.torrent"),
			strings.ReplaceAll(to, ".kv", ".kvei"),
			strings.ReplaceAll(to, ".kv", ".kvei.torrent"),
			strings.ReplaceAll(to, ".kv", ".kvi"),
			strings.ReplaceAll(to, ".kv", ".kvi.torrent"))
	}

	for _, f := range filesToRemove {
		if err := dir.RemoveFile(f); err != nil {
			return err
		}
	}
	return nil
}

func (a *Aggregator) sqeezeDomainFile(ctx context.Context, domain kv.Domain, from, to string) error {
	if domain == kv.CommitmentDomain {
		panic("please use SqueezeCommitmentFiles func")
	}

	compression := a.d[domain].Compression
	compressCfg := a.d[domain].CompressCfg

	a.logger.Info("[sqeeze] file", "f", to, "cfg", compressCfg, "c", compression)
	decompressor, err := seg.NewDecompressor(from)
	if err != nil {
		return err
	}
	defer decompressor.Close()
	defer decompressor.MadvSequential().DisableReadAhead()

	c, err := seg.NewCompressor(ctx, "sqeeze", to, a.dirs.Tmp, compressCfg, log.LvlInfo, a.logger)
	if err != nil {
		return err
	}
	defer c.Close()
	r := a.d[domain].dataReader(decompressor)
	w := a.d[domain].dataWriter(c, false)
	if err := w.ReadFrom(r); err != nil {
		return err
	}
	if err := c.Compress(); err != nil {
		return err
	}

	return nil
}

// SqueezeCommitmentFiles should be called only when NO EXECUTION is running.
// Removes commitment files and suppose following aggregator shutdown and restart  (to integrate new files and rebuild indexes)
func SqueezeCommitmentFiles(ctx context.Context, at *AggregatorRoTx, logger log.Logger) error {
	commitmentUseReferencedBranches := at.a.d[kv.CommitmentDomain].ReplaceKeysInValues
	if !commitmentUseReferencedBranches {
		return nil
	}

	rng := &Ranges{
		domain: [kv.DomainLen]DomainRanges{
			kv.AccountsDomain: {
				name:    kv.AccountsDomain,
				values:  MergeRange{"", true, 0, math.MaxUint64},
				history: HistoryRanges{},
				aggStep: at.StepSize(),
			},
			kv.StorageDomain: {
				name:    kv.StorageDomain,
				values:  MergeRange{"", true, 0, math.MaxUint64},
				history: HistoryRanges{},
				aggStep: at.StepSize(),
			},
			kv.CommitmentDomain: {
				name:    kv.CommitmentDomain,
				values:  MergeRange{"", true, 0, math.MaxUint64},
				history: HistoryRanges{},
				aggStep: at.StepSize(),
			},
		},
	}
	sf, err := at.FilesInRange(rng)
	if err != nil {
		return err
	}
	getSizeDelta := func(a, b string) (datasize.ByteSize, float32, error) {
		ai, err := os.Stat(a)
		if err != nil {
			return 0, 0, err
		}
		bi, err := os.Stat(b)
		if err != nil {
			return 0, 0, err
		}
		return datasize.ByteSize(ai.Size()) - datasize.ByteSize(bi.Size()), 100.0 * (float32(ai.Size()-bi.Size()) / float32(ai.Size())), nil
	}

	ranges := make([]MergeRange, 0)
	for fi, f := range sf.d[kv.AccountsDomain] {
		ranges = append(ranges, MergeRange{
			from: f.startTxNum,
			to:   f.endTxNum,
		})
		if sf.d[kv.StorageDomain][fi].startTxNum != f.startTxNum || sf.d[kv.StorageDomain][fi].endTxNum != f.endTxNum {
			return errors.New("account and storage file ranges are not matching")
		}
		if sf.d[kv.CommitmentDomain][fi].startTxNum != f.startTxNum || sf.d[kv.CommitmentDomain][fi].endTxNum != f.endTxNum {
			return errors.New("account and commitment file ranges are not matching")
		}
	}

	log.Info("[squeeze_migration] see target files", "count", len(ranges))
	if len(ranges) == 0 {
		return errors.New("no account files found")
	}

	var (
		temporalFiles  []string
		processedFiles int
		sizeDelta      = datasize.B
		sqExt          = ".squeezed"
		commitment     = at.d[kv.CommitmentDomain]
		accounts       = at.d[kv.AccountsDomain]
		storage        = at.d[kv.StorageDomain]
		logEvery       = time.NewTicker(30 * time.Second)
	)
	defer logEvery.Stop()

	for ri, r := range ranges {
		af, err := accounts.rawLookupFileByRange(r.from, r.to)
		if err != nil {
			return err
		}
		sf, err := storage.rawLookupFileByRange(r.from, r.to)
		if err != nil {
			return err
		}
		cf, err := commitment.rawLookupFileByRange(r.from, r.to)
		if err != nil {
			return err
		}

		af.decompressor.MadvNormal()
		sf.decompressor.MadvNormal()
		cf.decompressor.MadvNormal()

		err = func() error {
			steps := cf.endTxNum/at.a.stepSize - cf.startTxNum/at.a.stepSize
			compression := commitment.d.Compression
			if steps < DomainMinStepsToCompress {
				compression = seg.CompressNone
			}
			at.a.logger.Info("[squeeze_migration] file start", "original", cf.decompressor.FileName(),
				"progress", fmt.Sprintf("%d/%d", ri+1, len(ranges)), "compress_cfg", commitment.d.CompressCfg, "compress", compression)

			originalPath := cf.decompressor.FilePath()
			squeezedTmpPath := originalPath + sqExt + ".tmp"

			squeezedCompr, err := seg.NewCompressor(ctx, "squeeze", squeezedTmpPath, at.a.dirs.Tmp,
				commitment.d.CompressCfg, log.LvlInfo, commitment.d.logger)
			if err != nil {
				return err
			}
			defer squeezedCompr.Close()

			writer := commitment.d.dataWriter(squeezedCompr, false)
			reader := seg.NewReader(cf.decompressor.MakeGetter(), compression)
			reader.Reset(0)

			rng := MergeRange{needMerge: true, from: af.startTxNum, to: af.endTxNum}
			vt, err := commitment.commitmentValTransformDomain(rng, accounts, storage, af, sf)
			if err != nil {
				return fmt.Errorf("failed to create commitment value transformer: %w", err)
			}

			ki := 0
			var k, v []byte
			for reader.HasNext() {
				k, _ = reader.Next(k[:0])
				v, _ = reader.Next(v[:0])
				ki += 2

				if k == nil {
					// nil keys are not supported for domains
					continue
				}

				if !bytes.Equal(k, keyCommitmentState) {
					v, err = vt(v, af.startTxNum, af.endTxNum)
					if err != nil {
						return fmt.Errorf("failed to transform commitment value: %w", err)
					}
				}
				if _, err = writer.Write(k); err != nil {
					return fmt.Errorf("write key word: %w", err)
				}
				if _, err = writer.Write(v); err != nil {
					return fmt.Errorf("write value word: %w", err)
				}

				select {
				case <-logEvery.C:
					logger.Info("[squeeze_migration]", "file", cf.decompressor.FileName(), "k", hex.EncodeToString(k),
						"progress", fmt.Sprintf("%s/%s", common.PrettyCounter(ki), common.PrettyCounter(cf.decompressor.Count())))
				default:
				}
			}

			if err = writer.Compress(); err != nil {
				return err
			}
			writer.Close()

			delta, deltaP, err := getSizeDelta(originalPath, squeezedTmpPath)
			if err != nil {
				return err
			}
			sizeDelta += delta
			cf.frozen = false
			cf.closeFilesAndRemove()

			squeezedPath := originalPath + sqExt
			if err = os.Rename(squeezedTmpPath, squeezedPath); err != nil {
				return err
			}
			temporalFiles = append(temporalFiles, squeezedPath)

			at.a.logger.Info("[sqeeze_migration] file done", "original", filepath.Base(originalPath),
				"sizeDelta", fmt.Sprintf("%s (%.1f%%)", delta.HR(), deltaP))

			processedFiles++
			return nil
		}()
		if err != nil {
			return fmt.Errorf("failed to squeeze commitment file %q: %w", cf.decompressor.FileName(), err)
		}
		af.decompressor.DisableReadAhead()
		sf.decompressor.DisableReadAhead()
	}

	for _, path := range temporalFiles {
		if err := os.Rename(path, strings.TrimSuffix(path, sqExt)); err != nil {
			return err
		}
		at.a.logger.Debug("[squeeze_migration] temporal file renaming", "path", path)
	}
	at.a.logger.Info("[squeeze_migration] done", "sizeDelta", sizeDelta.HR(), "files", len(ranges))

	return nil
}

func CheckCommitmentForPrint(ctx context.Context, rwDb kv.TemporalRwDB) (string, error) {
	a := rwDb.(HasAgg).Agg().(*Aggregator)

	rwTx, err := rwDb.BeginTemporalRw(ctx)
	if err != nil {
		return "", err
	}
	defer rwTx.Rollback()

	domains, err := NewSharedDomains(rwTx, log.New())
	if err != nil {
		return "", err
	}
	rootHash, err := domains.sdCtx.Trie().RootHash()
	if err != nil {
		return "", err
	}
	s := fmt.Sprintf("[commitment] Latest: blockNum: %d txNum: %d latestRootHash: %x\n", domains.BlockNum(), domains.TxNum(), rootHash)
	s += fmt.Sprintf("[commitment] stepSize %d, ReplaceKeysInValues enabled %t\n", a.StepSize(), a.d[kv.CommitmentDomain].ReplaceKeysInValues)
	return s, nil
}

// RebuildCommitmentFiles recreates commitment files from existing accounts and storage kv files
// If some commitment exists, they will be accepted as correct and next kv range will be processed.
// DB expected to be empty, committed into db keys will be not processed.
func RebuildCommitmentFiles(ctx context.Context, rwDb kv.TemporalRwDB, txNumsReader *rawdbv3.TxNumsReader, logger log.Logger, squeeze bool) (latestRoot []byte, err error) {
	a := rwDb.(HasAgg).Agg().(*Aggregator)

	// disable hard alignment; allowing commitment and storage/account to have
	// different visibleFiles
	a.DisableAllDependencies()

	acRo := a.BeginFilesRo() // this tx is used to read existing domain files and closed in the end
	defer acRo.Close()
	defer acRo.MadvNormal().DisableReadAhead()

	rng := &Ranges{
		domain: [kv.DomainLen]DomainRanges{
			kv.AccountsDomain: {
				name:    kv.AccountsDomain,
				values:  MergeRange{"", true, 0, math.MaxUint64},
				history: HistoryRanges{},
				aggStep: a.StepSize(),
			},
		},
	}
	sf, err := acRo.FilesInRange(rng)
	if err != nil {
		return nil, err
	}

	ranges := make([]MergeRange, 0)
	for fi, f := range sf.d[kv.AccountsDomain] {
		logger.Info(fmt.Sprintf("[commitment_rebuild] shard to build #%d: steps %d-%d (based on %s)", fi, f.startTxNum/a.StepSize(), f.endTxNum/a.StepSize(), f.decompressor.FileName()))
		ranges = append(ranges, MergeRange{
			from: f.startTxNum,
			to:   f.endTxNum,
		})
	}
	if len(ranges) == 0 {
		return nil, errors.New("no account files found")
	}

	logger.Info("[commitment_rebuild] collected shards to build", "count", len(sf.d[kv.AccountsDomain]))
	start := time.Now()

	var totalKeysCommitted uint64

	for i, r := range ranges {
		rangeFromTxNum, rangeToTxNum := r.FromTo() // start-end txnum of found range
		lastTxnumInShard := rangeToTxNum
		if acRo.TxNumsInFiles(kv.CommitmentDomain) >= rangeToTxNum {
			logger.Info("[commitment_rebuild] skipping existing range", "range", r.String("", a.StepSize()))
			continue
		}

		roTx, err := a.db.BeginRo(ctx)
		if err != nil {
			return nil, err
		}
		defer roTx.Rollback()

		// count keys in accounts and storage domains
		accKeys := acRo.KeyCountInFiles(kv.AccountsDomain, rangeFromTxNum, rangeToTxNum)
		stoKeys := acRo.KeyCountInFiles(kv.StorageDomain, rangeFromTxNum, rangeToTxNum)
		totalKeys := accKeys + stoKeys

		shardFrom, shardTo := kv.Step(rangeFromTxNum/a.StepSize()), kv.Step(rangeToTxNum/a.StepSize()) // define steps from-to for this range

		lastShard := shardTo // this is the last shard in this range, in case we lower shardTo to process big range in several steps

		stepsInShard := uint64(shardTo - shardFrom)
		keysPerStep := totalKeys / stepsInShard // how many keys in just one step?

		//shardStepsSize := kv.Step(2)
		shardStepsSize := kv.Step(min(uint64(math.Pow(2, math.Log2(float64(stepsInShard)))), 128))
		if uint64(shardStepsSize) != stepsInShard { // processing shard in several smaller steps
			shardTo = shardFrom + shardStepsSize // if shard is quite big, we will process it in several steps
		}

		// rangeToTxNum = uint64(shardTo) * a.StepSize()

		// Range is original file steps; like 0-1024.
		// Shard is smaller part of same file. By its own it does not make sense or match to state as of e.g. 0-128. Its just 1/8 shard of range 0-1024
		//
		logger.Info("[commitment_rebuild] starting", "range", r.String("", a.StepSize()), "range", fmt.Sprintf("%d/%d", i+1, len(ranges)),
			"shardSteps", fmt.Sprintf("%d-%d", shardFrom, shardTo),
			"keysPerStep", keysPerStep, "keysInRange", common.PrettyCounter(totalKeys))

		// fmt.Printf("txRangeFrom %d, txRangeTo %d, totalKeys %d (%d + %d)\n", rangeFromTxNum, rangeToTxNum, totalKeys, accKeys, stoKeys)
		// fmt.Printf("keysPerStep %d, shardStepsSize %d, shardFrom %d, shardTo %d, lastShard %d\n", keysPerStep, shardStepsSize, shardFrom, shardTo, lastShard)

		var rebuiltCommit *rebuiltCommitment
		var processed uint64

		streamAcc, err := acRo.FileStream(kv.AccountsDomain, rangeFromTxNum, rangeToTxNum)
		if err != nil {
			return nil, err
		}
		streamSto, err := acRo.FileStream(kv.StorageDomain, rangeFromTxNum, rangeToTxNum)
		if err != nil {
			return nil, err
		}
		keyIter := stream.UnionKV(streamAcc, streamSto, -1)
		//blockNum, ok, err := txNumsReader.FindBlockNum(roTx, rangeToTxNum-1)
		blockNum, ok, err := txNumsReader.FindBlockNum(roTx, rangeToTxNum-1)
		if err != nil {
			return nil, fmt.Errorf("CommitmentRebuild: FindBlockNum(%d) %w", rangeToTxNum, err)
		}
		if !ok {
			//var txnum uint64
			blockNum, _, err = txNumsReader.Last(roTx)
			if err != nil {
				return nil, fmt.Errorf("CommitmentRebuild: Last() %w", err)
			}
		}
		roTx.Rollback()

		for shardFrom < lastShard { // recreate this file range 1+ steps
			nextKey := func() (ok bool, k []byte) {
				if !keyIter.HasNext() {
					return false, nil
				}
				k, _, err := keyIter.Next()
				if err != nil {
					err = fmt.Errorf("CommitmentRebuild: keyIter.Next() %w", err)
					panic(err)
				}
				processed++
				if processed%(keysPerStep*uint64(shardStepsSize)) == 0 && shardTo != lastShard {
					return false, k
				}
				return true, k
			}

			rwTx, err := rwDb.BeginTemporalRw(ctx)
			if err != nil {
				return nil, err
			}
			defer rwTx.Rollback()

			domains, err := NewSharedDomains(rwTx, log.New())
			if err != nil {
				return nil, err
			}

			domains.SetBlockNum(blockNum)
			domains.SetTxNum(lastTxnumInShard - 1)
			domains.sdCtx.SetLimitReadAsOfTxNum(lastTxnumInShard, true) // this helps to read state from correct file during commitment

			rebuiltCommit, err = rebuildCommitmentShard(ctx, domains, rwTx, nextKey, &rebuiltCommitment{
				StepFrom: shardFrom,
				StepTo:   shardTo,
				TxnFrom:  rangeFromTxNum,
				TxnTo:    rangeToTxNum,
				Keys:     totalKeys,

				BlockNumber: domains.BlockNum(),
				TxnNumber:   domains.TxNum(),
				LogPrefix:   fmt.Sprintf("[commitment_rebuild] range %s shard %d-%d", r.String("", a.StepSize()), shardFrom, shardTo),
			})
			if err != nil {
				return nil, err
			}
			domains.Close()

			// make new file visible for all aggregator transactions
			a.dirtyFilesLock.Lock()
			a.recalcVisibleFiles(a.dirtyFilesEndTxNumMinimax())
			a.dirtyFilesLock.Unlock()
			rwTx.Rollback()

			if shardTo+shardStepsSize > lastShard && shardStepsSize > 1 {
				shardStepsSize /= 2
			}
			shardFrom = shardTo
			shardTo += shardStepsSize
			rangeFromTxNum = rangeToTxNum
			rangeToTxNum += uint64(shardStepsSize) * a.StepSize()
		}

		keyIter.Close()

		totalKeysCommitted += processed

		rhx := ""
		if rebuiltCommit != nil {
			rhx = hex.EncodeToString(rebuiltCommit.RootHash)
			latestRoot = rebuiltCommit.RootHash
		}

		var m runtime.MemStats
		dbg.ReadMemStats(&m)
		logger.Info("[rebuild_commitment] finished range", "stateRoot", rhx, "range", r.String("", a.StepSize()),
			"block", blockNum, "totalKeysProcessed", common.PrettyCounter(totalKeysCommitted), "alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))

		for {
			smthDone, err := a.mergeLoopStep(ctx, rangeToTxNum)
			if err != nil {
				return nil, err
			}
			if !smthDone {
				break
			}
		}

	}

	var m runtime.MemStats
	dbg.ReadMemStats(&m)
	logger.Info("[rebuild_commitment] done", "duration", time.Since(start), "totalKeysProcessed", common.PrettyCounter(totalKeysCommitted), "alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))

	acRo.Close()

	if !squeeze && !statecfg.Schema.CommitmentDomain.ReplaceKeysInValues {
		return latestRoot, nil
	}
	logger.Info("[squeeze] starting")
	a.recalcVisibleFiles(a.dirtyFilesEndTxNumMinimax())

	logger.Info(fmt.Sprintf("[squeeze] latest root %x", latestRoot))

	actx := a.BeginFilesRo()
	defer actx.Close()

	if err = SqueezeCommitmentFiles(ctx, actx, logger); err != nil {
		logger.Warn("[squeeze] failed", "err", err)
		logger.Info("[squeeze] rebuilt commitment files still available. Instead of re-run, you have to run 'erigon snapshots sqeeze' to finish squeezing")
		return nil, err
	}
	actx.Close()
	if err = a.OpenFolder(); err != nil {
		logger.Warn("[squeeze] failed to open folder after sqeeze", "err", err)
	}

	if err = a.BuildMissedAccessors(ctx, 4); err != nil {
		logger.Warn("[squeeze] failed to build missed accessors", "err", err)
		return nil, err
	}

	return latestRoot, nil
}

// discardWrites disables updates collection for further flushing into db.
// Instead, it keeps them temporarily available until .ClearRam/.Close will make them unavailable.
func (sd *SharedDomains) discardWrites(d kv.Domain) {
	if d >= kv.DomainLen {
		return
	}
	sd.domainWriters[d].discard = true
	sd.domainWriters[d].h.discard = true
}

func rebuildCommitmentShard(ctx context.Context, sd *SharedDomains, tx kv.TemporalTx, next func() (bool, []byte), cfg *rebuiltCommitment) (*rebuiltCommitment, error) {
	aggTx := AggTx(tx)
	sd.discardWrites(kv.AccountsDomain)
	sd.discardWrites(kv.StorageDomain)
	sd.discardWrites(kv.CodeDomain)

	logger := sd.logger

	visComFiles := tx.(kv.WithFreezeInfo).FreezeInfo().Files(kv.CommitmentDomain)
	logger.Info(cfg.LogPrefix+" started", "totalKeys", common.PrettyCounter(cfg.Keys), "block", cfg.BlockNumber, "txn", cfg.TxnNumber,
		"files", fmt.Sprintf("%d %v", len(visComFiles), visComFiles.Fullpaths()))

	sf := time.Now()
	var processed uint64
	for ok, key := next(); ; ok, key = next() {
		sd.sdCtx.TouchKey(kv.AccountsDomain, string(key), nil)
		processed++
		if !ok {
			break
		}
	}

	collectionSpent := time.Since(sf)
	rh, err := sd.sdCtx.ComputeCommitment(ctx, true, cfg.BlockNumber, cfg.TxnNumber, fmt.Sprintf("%d-%d", cfg.StepFrom, cfg.StepTo))
	if err != nil {
		return nil, err
	}
	logger.Info(cfg.LogPrefix+" now sealing (dumping on disk)", "root", hex.EncodeToString(rh),
		"keysInShard", common.PrettyCounter(processed), "keysInRange", common.PrettyCounter(cfg.Keys))

	sb := time.Now()
	err = aggTx.d[kv.CommitmentDomain].d.dumpStepRangeOnDisk(ctx, cfg.StepFrom, cfg.StepTo, cfg.TxnFrom, cfg.TxnTo, sd.domainWriters[kv.CommitmentDomain], nil)
	if err != nil {
		return nil, err
	}

	logger.Info(cfg.LogPrefix+" finished", "block", cfg.BlockNumber, "txn", cfg.TxnNumber, "root", hex.EncodeToString(rh),
		"keysInShard", common.PrettyCounter(processed), "keysInRange", common.PrettyCounter(cfg.Keys),
		"spentCollecting", collectionSpent.String(), "spentComputing", time.Since(sf).String(), "spentDumpingOnDisk", time.Since(sb).String())

	return &rebuiltCommitment{
		RootHash: rh,
		StepFrom: cfg.StepFrom,
		StepTo:   cfg.StepTo,
		TxnFrom:  cfg.TxnFrom,
		TxnTo:    cfg.TxnTo,
		Keys:     processed,
	}, nil
}

type rebuiltCommitment struct {
	RootHash    []byte // root hash of this commitment. set once commit is finished
	StepFrom    kv.Step
	StepTo      kv.Step
	TxnFrom     uint64
	TxnTo       uint64
	Keys        uint64 // amount of keys in this range
	BlockNumber uint64 // block number for this commitment
	TxnNumber   uint64 // tx number for this commitment
	LogPrefix   string
}

func domainFiles(dirs datadir.Dirs, domain kv.Domain) []string {
	files, err := dir.ListFiles(dirs.SnapDomain, ".kv")
	if err != nil {
		panic(err)
	}
	res := make([]string, 0, len(files))
	for _, f := range files {
		if !strings.Contains(f, domain.String()) {
			continue
		}
		res = append(res, f)
	}
	return res
}
