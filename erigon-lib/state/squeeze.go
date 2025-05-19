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
	"strings"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/kv/stream"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/seg"
)

//Sqeeze: ForeignKeys-aware compression of file

// Sqeeze - re-compress file
// Breaks squeezed commitment files totally
// TODO: care of ForeignKeys
func (a *Aggregator) Sqeeze(ctx context.Context, domain kv.Domain) error {
	filesToRemove := []string{}
	for _, to := range domainFiles(a.dirs, domain) {
		_, fileName := filepath.Split(to)
		fromStep, toStep, err := ParseStepsFromFileName(fileName)
		if err != nil {
			return err
		}
		if toStep-fromStep < DomainMinStepsToCompress {
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
		if err := os.Remove(f); err != nil {
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
	r := seg.NewReader(decompressor.MakeGetter(), seg.DetectCompressType(decompressor.MakeGetter()))

	c, err := seg.NewCompressor(ctx, "sqeeze", to, a.dirs.Tmp, compressCfg, log.LvlInfo, a.logger)
	if err != nil {
		return err
	}
	defer c.Close()
	w := seg.NewWriter(c, compression)
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
func (at *AggregatorRoTx) SqueezeCommitmentFiles() error {
	if !at.a.commitmentValuesTransform {
		return nil
	}

	rng := &Ranges{
		domain: [kv.DomainLen]DomainRanges{
			kv.AccountsDomain: {
				name:    kv.AccountsDomain,
				values:  MergeRange{"", true, 0, math.MaxUint64},
				history: HistoryRanges{},
				aggStep: at.a.StepSize(),
			},
			kv.StorageDomain: {
				name:    kv.StorageDomain,
				values:  MergeRange{"", true, 0, math.MaxUint64},
				history: HistoryRanges{},
				aggStep: at.a.StepSize(),
			},
			kv.CommitmentDomain: {
				name:    kv.CommitmentDomain,
				values:  MergeRange{"", true, 0, math.MaxUint64},
				history: HistoryRanges{},
				aggStep: at.a.StepSize(),
			},
		},
	}
	sf, err := at.staticFilesInRange(rng)
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
			steps := cf.endTxNum/at.a.aggregationStep - cf.startTxNum/at.a.aggregationStep
			compression := commitment.d.Compression
			if steps < DomainMinStepsToCompress {
				compression = seg.CompressNone
			}
			at.a.logger.Info("[squeeze_migration] file start", "original", cf.decompressor.FileName(),
				"progress", fmt.Sprintf("%d/%d", ri+1, len(ranges)), "compress_cfg", commitment.d.CompressCfg, "compress", compression)

			originalPath := cf.decompressor.FilePath()
			squeezedTmpPath := originalPath + sqExt + ".tmp"

			squeezedCompr, err := seg.NewCompressor(context.Background(), "squeeze", squeezedTmpPath, at.a.dirs.Tmp,
				commitment.d.CompressCfg, log.LvlInfo, commitment.d.logger)
			if err != nil {
				return err
			}
			defer squeezedCompr.Close()

			writer := seg.NewWriter(squeezedCompr, commitment.d.Compression)
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
					at.a.logger.Info("[squeeze_migration]", "file", cf.decompressor.FileName(), "k", fmt.Sprintf("%x", k),
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

// wraps tx and aggregator for tests. (when temporal db is unavailable)
type wrappedTxWithCtx struct {
	kv.Tx
	ac *AggregatorRoTx
}

func (w *wrappedTxWithCtx) AggTx() any { return w.ac }

func wrapTxWithCtxForTest(tx kv.Tx, ctx *AggregatorRoTx) *wrappedTxWithCtx {
	return &wrappedTxWithCtx{Tx: tx, ac: ctx}
}

// RebuildCommitmentFiles recreates commitment files from existing accounts and storage kv files
// If some commitment exists, they will be accepted as correct and next kv range will be processed.
// DB expected to be empty, committed into db keys will be not processed.
func (a *Aggregator) RebuildCommitmentFiles(ctx context.Context, rwDb kv.RwDB, txNumsReader *rawdbv3.TxNumsReader) (latestRoot []byte, err error) {
	acRo := a.BeginFilesRo() // this tx is used to read existing domain files and closed in the end
	defer acRo.Close()

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
	sf, err := acRo.staticFilesInRange(rng)
	if err != nil {
		return nil, err
	}
	ranges := make([]MergeRange, 0)
	for fi, f := range sf.d[kv.AccountsDomain] {
		fmt.Printf("shard %d - %d-%d %s\n", fi, f.startTxNum, f.endTxNum, f.decompressor.FileName())
		ranges = append(ranges, MergeRange{
			from: f.startTxNum,
			to:   f.endTxNum,
		})
	}
	if len(ranges) == 0 {
		return nil, errors.New("no account files found")
	}

	acRo.MadvNormal()
	defer acRo.DisableReadAhead()

	start := time.Now()

	acRo.RestrictSubsetFileDeletions(true)
	a.commitmentValuesTransform = false

	var totalKeysCommitted uint64

	for i, r := range ranges {
		a.logger.Info("scanning keys", "range", r.String("", a.StepSize()), "shards", fmt.Sprintf("%d/%d", i+1, len(ranges))) //

		fromTxNumRange, toTxNumRange := r.FromTo()
		lastTxnumInShard := toTxNumRange
		if acRo.TxNumsInFiles(kv.StateDomains...) >= toTxNumRange {
			a.logger.Info("skipping existing range", "range", r.String("", a.StepSize()))
			continue
		}

		roTx, err := a.db.BeginRo(ctx)
		if err != nil {
			return nil, err
		}
		defer roTx.Rollback()

		_, blockNum, err := txNumsReader.FindBlockNum(roTx, toTxNumRange-1)
		if err != nil {
			a.logger.Warn("failed to find block number for txNum", "txNum", toTxNumRange, "err", err)
			return nil, err
		}

		txnRangeTo := int(toTxNumRange)
		txnRangeFrom := int(fromTxNumRange)

		accReader, err := acRo.nastyFileRead(kv.AccountsDomain, fromTxNumRange, toTxNumRange)
		if err != nil {
			return nil, err
		}
		stoReader, err := acRo.nastyFileRead(kv.StorageDomain, fromTxNumRange, toTxNumRange)
		if err != nil {
			return nil, err
		}

		streamAcc := NewSegStreamReader(accReader, -1)
		streamSto := NewSegStreamReader(stoReader, -1)
		keyIter := stream.UnionKV(streamAcc, streamSto, -1)

		totalKeys := acRo.KeyCountInDomainRange(kv.AccountsDomain, uint64(txnRangeFrom), uint64(txnRangeTo)) +
			acRo.KeyCountInDomainRange(kv.StorageDomain, uint64(txnRangeFrom), uint64(txnRangeTo))

		shardFrom, shardTo := fromTxNumRange/a.StepSize(), toTxNumRange/a.StepSize()
		batchSize := totalKeys / (shardTo - shardFrom)
		lastShard := shardTo

		shardSize := min(uint64(math.Pow(2, math.Log2(float64(totalKeys/batchSize)))), 128)
		shardTo = shardFrom + shardSize
		toTxNumRange = shardTo * a.StepSize()

		a.logger.Info("beginning commitment", "range", r.String("", a.StepSize()), "shardSize", shardSize, "batch", batchSize)

		var rebuiltCommit *RebuiltCommitment
		var processed uint64

		for shardFrom < lastShard {
			nextKey := func() (ok bool, k []byte) {
				if !keyIter.HasNext() {
					return false, nil
				}
				if processed%1000 == 0 {
					fmt.Printf("processed %12d/%d (%2.f%%) %x\r", processed, totalKeys, float64(processed)/float64(totalKeys)*100, k)
				}
				k, _, err := keyIter.Next()
				if err != nil {
					a.logger.Warn("nextKey", "err", err)
					return false, nil
				}
				processed++
				if processed%(batchSize*shardSize) == 0 && shardTo != lastShard {
					fmt.Println()
					return false, k
				}
				return true, k
			}

			var rwTx kv.RwTx
			var domains *SharedDomains
			var ac *AggregatorRoTx

			if rwDb != nil {
				// regular case
				rwTx, err = rwDb.BeginRw(ctx)
				if err != nil {
					return nil, err
				}
				defer rwTx.Rollback()

				domains, err = NewSharedDomains(rwTx, log.New())
				if err != nil {
					return nil, err
				}

				var ok bool
				ac, ok = domains.AggTx().(*AggregatorRoTx)
				if !ok {
					return nil, errors.New("failed to get state aggregatorTx")
				}

			} else {
				// case when we do testing and temporal db with aggtx is not available
				ac = a.BeginFilesRo()

				domains, err = NewSharedDomains(wrapTxWithCtxForTest(roTx, ac), log.New())
				if err != nil {
					ac.Close()
					return nil, err
				}
			}
			defer ac.Close()

			////domains, err := NewSharedDomains(wrapTxWithCtx(roTx, ac), log.New())
			//if err != nil {
			//	return nil, err
			//}

			//ac, ok := domains.AggTx().(*AggregatorRoTx)
			//if !ok {
			//	return nil, errors.New("failed to get state aggregatorTx")
			//}
			//defer ac.Close()

			domains.SetBlockNum(blockNum)
			domains.SetTxNum(lastTxnumInShard - 1)
			domains.sdCtx.SetLimitReadAsOfTxNum(domains.TxNum()+1, true) // this helps to read state from correct file during commitment

			rebuiltCommit, err = domains.RebuildCommitmentShard(ctx, nextKey, &RebuiltCommitment{
				StepFrom: shardFrom,
				StepTo:   shardTo,
				TxnFrom:  fromTxNumRange,
				TxnTo:    toTxNumRange,
				Keys:     totalKeys,
			})
			if err != nil {
				return nil, err
			}
			a.logger.Info(fmt.Sprintf("shard %d-%d of range %s finished (%d%%)", shardFrom, shardTo, r.String("", a.StepSize()), processed*100/totalKeys),
				"keys", fmt.Sprintf("%s/%s", common.PrettyCounter(processed), common.PrettyCounter(totalKeys)))

			ac.Close()
			domains.Close()

			a.dirtyFilesLock.Lock()
			a.recalcVisibleFiles(a.dirtyFilesEndTxNumMinimax())
			a.dirtyFilesLock.Unlock()
			if rwTx != nil {
				rwTx.Rollback()
				rwTx = nil
			}

			if shardTo+shardSize > lastShard && shardSize > 1 {
				shardSize /= 2
			}
			shardFrom = shardTo
			shardTo += shardSize
			fromTxNumRange = toTxNumRange
			toTxNumRange += shardSize * a.StepSize()
		}

		roTx.Rollback()
		totalKeysCommitted += processed

		rhx := ""
		if rebuiltCommit != nil {
			rhx = hex.EncodeToString(rebuiltCommit.RootHash)
			latestRoot = rebuiltCommit.RootHash
		}
		a.logger.Info("finished commitment range", "stateRoot", rhx, "range", r.String("", a.StepSize()),
			"block", blockNum, "totalKeysProcessed", common.PrettyCounter(totalKeysCommitted))

		for {
			smthDone, err := a.mergeLoopStep(ctx, toTxNumRange)
			if err != nil {
				return nil, err
			}
			if !smthDone {
				break
			}
		}

		keyIter.Close()
	}
	a.logger.Info("Commitment rebuild", "duration", time.Since(start), "totalKeysProcessed", common.PrettyCounter(totalKeysCommitted))

	a.logger.Info("Squeezing commitment files")
	a.commitmentValuesTransform = true

	acRo.Close()

	a.recalcVisibleFiles(a.dirtyFilesEndTxNumMinimax())

	fmt.Printf("latest root %x\n", latestRoot)

	actx := a.BeginFilesRo()
	defer actx.Close()
	if err = actx.SqueezeCommitmentFiles(); err != nil {
		a.logger.Warn("squeezeCommitmentFiles failed", "err", err)
		fmt.Printf("rebuilt commitment files still available. Instead of re-run, you have to run 'erigon snapshots sqeeze' to finish squeezing")
		return nil, err
	}
	return latestRoot, nil
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
