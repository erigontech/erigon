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
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/v3/pbt"
)

type PBinConvertOptions struct {
	SourceAggregator  *Aggregator
	SourceTx          kv.TemporalTx
	TargetAggregator  *Aggregator
	TargetTx          kv.TemporalRwTx
	TargetDB          kv.TemporalRoDB
	TargetDomain      kv.Domain
	BlockNum          uint64
	EndTxNum          uint64
	Hash              eip8297.HashFn
	RangeWriterLimits *PBinRangeWriterLimits
	Workers           int
	Resume            *PBinResumeOptions
}

type pbinConversionResumeStateReader struct {
	at         *AggregatorRoTx
	state      []byte
	baseTxNum  uint64
	limitTxNum uint64
	stepSize   uint64
}

func (r *pbinConversionResumeStateReader) WithHistory() bool { return false }

func (r *pbinConversionResumeStateReader) CheckDataAvailable(kv.Domain, kv.Step) error { return nil }

func (r *pbinConversionResumeStateReader) Read(domain kv.Domain, key []byte, stepSize uint64) ([]byte, kv.Step, error) {
	if domain == kv.CommitmentDomain || domain == kv.CommitmentBinDomain {
		if bytes.Equal(key, commitment.KeyCommitmentState) {
			if len(r.state) != 0 {
				return bytes.Clone(r.state), kv.Step((r.baseTxNum + 1) / stepSize), nil
			}
		}
		if bytes.Equal(key, pbt.GlobalRootKey()) {
			state := r.state
			if len(state) != 0 {
				if !commitment.IsPBinState(state) && len(state) >= 18 {
					state = state[18:]
				}
				if len(state) >= 5 {
					return bytes.Clone(state[5:]), kv.Step((r.baseTxNum + 1) / stepSize), nil
				}
			}
		}
	}
	value, found, _, endTxNum, err := r.at.DebugGetLatestFromFiles(domain, key, r.limitTxNum)
	if err != nil {
		return nil, 0, err
	}
	if !found {
		return nil, 0, nil
	}
	return bytes.Clone(value), kv.Step(endTxNum / r.stepSize), nil
}

func (r *pbinConversionResumeStateReader) Clone(kv.TemporalTx) commitmentdb.StateReader { return r }

func (r *pbinConversionResumeStateReader) CloneForWorker(context.Context, kv.TemporalTx) commitmentdb.StateReader {
	return r
}

func (r *pbinConversionResumeStateReader) BindsWorkerTx() bool { return false }

func PBinRootFromStream(hash eip8297.HashFn, stream func(func(PBinLeaf) error) error) (common.Hash, error) {
	builder, err := eip8297.NewStreamRootBuilder(hash)
	if err != nil {
		return common.Hash{}, err
	}
	if err := stream(func(leaf PBinLeaf) error { return builder.Add(leaf.Key, leaf.Value) }); err != nil {
		return common.Hash{}, err
	}
	return builder.RootHash()
}

func PBinConvertWorkers(workers int) int {
	if workers <= 0 {
		workers = dbg.PBTWorkers
	}
	return max(workers, 1)
}

func ConvertPBin(ctx context.Context, opts PBinConvertOptions) (common.Hash, error) {
	if opts.SourceAggregator == nil || opts.SourceTx == nil || opts.TargetAggregator == nil || opts.TargetTx == nil {
		return common.Hash{}, fmt.Errorf("pbin conversion: missing database input")
	}
	if opts.TargetDomain != kv.CommitmentDomain && opts.TargetDomain != kv.CommitmentBinDomain {
		return common.Hash{}, fmt.Errorf("pbin conversion: invalid target domain %s", opts.TargetDomain)
	}
	if opts.Hash == nil {
		return common.Hash{}, fmt.Errorf("pbin conversion: nil hash function")
	}
	workers := PBinConvertWorkers(opts.Workers)
	limits := PBinRangeWriterLimits{MaxOps: pbinRangeWriterMaxOps, MaxBytes: pbinRangeWriterMaxBytes}
	if opts.RangeWriterLimits != nil {
		limits = *opts.RangeWriterLimits
	}
	cfg := pbinConversionTrieConfig()
	cfg.WarmupNumWorkers = workers
	options := []execctx.SharedDomainOption{execctx.WithTrieConfig(cfg), execctx.WithCommitmentDomainOnly(opts.TargetDomain), execctx.WithoutCommitmentSeek()}
	if opts.TargetDB != nil {
		options = append(options, execctx.WithParaTrieDB(opts.TargetDB))
	}
	domains, err := execctx.NewSharedDomains(ctx, opts.TargetTx, log.Root(), options...)
	if err != nil {
		return common.Hash{}, err
	}
	defer domains.Close()
	if opts.Resume != nil {
		baseTxNum := opts.Resume.BaseTxNum
		baseFiles := opts.TargetAggregator.BeginFilesRo()
		defer baseFiles.Close()
		limitTxNum := max(baseTxNum, opts.Resume.BaseStateEnd)
		commitmentReader := commitmentdb.StateReader(&pbinConversionResumeStateReader{at: baseFiles, state: opts.Resume.BaseState, baseTxNum: baseTxNum, limitTxNum: limitTxNum, stepSize: opts.TargetAggregator.StepSize()})
		plainReader := commitmentdb.NewLatestStateReader(opts.TargetTx, domains, commitmentdb.LatestStateReaderOptions{})
		domains.GetCommitmentCtxForDomain(opts.TargetDomain).SetStateReader(commitmentdb.NewCommitmentSplitStateReader(
			commitmentReader,
			plainReader,
			opts.TargetDomain,
			false,
		))
		gotTxNum, _, seekErr := domains.SeekCommitment(ctx, opts.TargetTx)
		if seekErr != nil {
			return common.Hash{}, fmt.Errorf("pbin conversion: resume base seek: %w", seekErr)
		}
		if gotTxNum != baseTxNum {
			return common.Hash{}, fmt.Errorf("pbin conversion: resume base state is at txNum %d, want %d", gotTxNum, baseTxNum)
		}
	}

	sourceFiles := opts.SourceAggregator.BeginFilesRo()
	defer sourceFiles.Close()
	var streamRoot common.Hash
	var streamErr error
	leaves := func(emit func(PBinLeaf) error) error {
		if opts.Resume != nil {
			resume := *opts.Resume
			if resume.SourceTxNum == 0 {
				resume.SourceTxNum = opts.EndTxNum
			}
			if resume.Workers <= 0 {
				resume.Workers = workers
			}
			if resume.SourceTxNum != opts.EndTxNum {
				return fmt.Errorf("pbin conversion: resume source txNum %d differs from end txNum %d", resume.SourceTxNum, opts.EndTxNum)
			}
			return PBinResumeLeaves(ctx, sourceFiles, resume, emit)
		}
		rootBuilder, rootErr := eip8297.NewParallelStreamRoot(opts.Hash, workers)
		if rootErr != nil {
			return rootErr
		}
		returnErr := ForEachPBinLeafWithWorkers(ctx, sourceFiles, opts.SourceTx, true, workers, func(leaf PBinLeaf) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			if addErr := rootBuilder.AddContext(ctx, leaf.Key, leaf.Value); addErr != nil {
				return addErr
			}
			return emit(leaf)
		})
		streamRoot, streamErr = rootBuilder.RootHash()
		if returnErr != nil {
			return returnErr
		}
		return streamErr
	}
	var writer *PBinRangeWriter
	limits.Workers = workers
	if opts.Resume != nil {
		resume := *opts.Resume
		writer, err = newPBinRangeWriterForResume(opts.TargetAggregator, opts.TargetDomain, resume.BaseTxNum, opts.EndTxNum, resume.BaseState, resume.BaseStateEnd, resume.SourceRanges)
		if writer != nil {
			writer.workers = workers
		}
	} else {
		writer, err = newPBinRangeWriter(opts.TargetAggregator, opts.TargetDomain, opts.EndTxNum, limits)
	}
	if err != nil {
		return common.Hash{}, err
	}
	writer.absentProbe = limits.AbsentProbe
	root, err := writer.WriteAtBlock(ctx, opts.TargetTx, domains, leaves, opts.BlockNum)
	if err != nil {
		return common.Hash{}, err
	}
	if opts.Resume == nil && root != streamRoot {
		return common.Hash{}, fmt.Errorf("pbin conversion: engine root %x differs from stream root %x", root, streamRoot)
	}
	return root, nil
}

func VerifyPBinDomainRoot(ctx context.Context, tx kv.TemporalTx, aggregator *Aggregator, domain kv.Domain) (common.Hash, error) {
	return verifyPBinDomainRoot(ctx, tx, aggregator, domain, nil)
}

func verifyPBinDomainRoot(ctx context.Context, tx kv.TemporalTx, aggregator *Aggregator, domain kv.Domain, stats *pbt.VerifyStats) (common.Hash, error) {
	if tx == nil || aggregator == nil {
		return common.Hash{}, fmt.Errorf("pbin verification: missing database input")
	}
	cfg := pbinConversionTrieConfig()
	at := aggregator.BeginFilesRo()
	defer at.Close()
	progress := NewPBTProgress(PBTProgressConfig{Message: "PBT verification progress", Phase: "domain root verify " + domain.String(), TempDir: aggregator.Dirs().Tmp})
	defer progress.Done()
	if len(at.Files(domain)) == 0 {
		return eip8297.EmptyTreeHash, nil
	}
	verifyStats := stats
	if verifyStats == nil {
		verifyStats = new(pbt.VerifyStats)
	}
	verifyStats.SetLiveProgress(func(rows, bytes uint64) {
		progress.SetFields("rows_live", rows, "bytes_live", bytes)
	})
	defer verifyStats.SetLiveProgress(nil)
	log.Root().Info("PBT verification started", "phase", "pbt verify", "domain", domain.String())
	domains, err := execctx.NewSharedDomains(ctx, tx, log.Root(), execctx.WithTrieConfig(cfg), execctx.WithCommitmentDomainOnly(domain), execctx.WithoutCommitmentSeek())
	if err != nil {
		return common.Hash{}, err
	}
	defer domains.Close()
	commitmentCtx := domains.GetCommitmentCtxForDomain(domain)
	if commitmentCtx == nil {
		return common.Hash{}, fmt.Errorf("pbin verification: commitment domain %s is unavailable", domain)
	}
	commitmentCtx.SetVerifyPointLookupCounter(&verifyStats.PointLookups)
	defer commitmentCtx.SetVerifyPointLookupCounter(nil)
	err = commitmentCtx.PrepareForVerification(tx)
	if err != nil {
		return common.Hash{}, fmt.Errorf("pbin verification: prepare: %w", err)
	}
	trie := commitmentCtx.Trie()
	verifier, ok := trie.(interface {
		VerifyContext(context.Context, int, commitment.TrieContextFactory) error
	})
	if !ok {
		return common.Hash{}, fmt.Errorf("pbin verification: trie does not support verification")
	}
	if progressTrie, ok := trie.(interface{ SetVerifyProgress(func([]byte)) }); ok {
		progressTrie.SetVerifyProgress(func([]byte) { progress.Add(1) })
		defer progressTrie.SetVerifyProgress(nil)
	}
	if statsTrie, ok := trie.(interface{ SetVerifyStats(*pbt.VerifyStats) }); ok {
		statsTrie.SetVerifyStats(verifyStats)
		defer statsTrie.SetVerifyStats(nil)
	}
	factory, release := commitmentCtx.VerificationContextFactory(tx)
	defer release()
	if verifyErr := verifier.VerifyContext(ctx, cfg.PBinWorkersOrDefault(), factory); verifyErr != nil {
		return common.Hash{}, verifyErr
	}
	root, err := trie.RootHash()
	if err != nil {
		return common.Hash{}, err
	}
	result := common.BytesToHash(root)
	log.Root().Info("PBT verification finished", "phase", "pbt verify", "domain", domain.String(), "root", result)
	return result, nil
}

func pbinConversionTrieConfig() commitment.TrieConfig {
	cfg := commitment.DefaultTrieConfig()
	cfg.Variant = commitment.VariantBinPatriciaTrie
	cfg.EnableTrieWarmup = false
	return cfg
}
