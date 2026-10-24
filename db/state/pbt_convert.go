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
	"sync/atomic"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
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
	WorkerViews       *atomic.Int32
	Workers           int
	SourceWorkerRuns  *atomic.Int32
	Resume            *PBinResumeOptions
}

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

func PBinStateRoot(at *AggregatorRoTx, tx kv.Tx, filesOnly bool, hash eip8297.HashFn) (common.Hash, error) {
	return PBinRootFromStream(hash, func(emit func(PBinLeaf) error) error {
		return ForEachPBinLeaf(at, tx, filesOnly, emit)
	})
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
	workers := opts.Workers
	if workers <= 0 {
		workers = dbg.PBTWorkers
	}
	workers = max(workers, 1)
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
		domains.GetCommitmentCtx().SetStateReader(commitmentdb.NewCommitmentSplitStateReader(
			commitmentdb.NewFilesOnlyStateReader(opts.TargetTx, baseTxNum),
			commitmentdb.NewLatestStateReader(opts.TargetTx, domains, commitmentdb.LatestStateReaderOptions{}),
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
		rootInput := make(chan PBinLeaf, 1024)
		rootDone := make(chan struct{})
		go func() {
			defer close(rootDone)
			streamRoot, streamErr = PBinRootFromStream(opts.Hash, func(add func(PBinLeaf) error) error {
				for leaf := range rootInput {
					if addErr := add(leaf); addErr != nil {
						return addErr
					}
				}
				return nil
			})
		}()
		defer func() {
			close(rootInput)
			<-rootDone
		}()
		returnErr := ForEachPBinLeafWithWorkers(sourceFiles, opts.SourceTx, true, workers, opts.SourceWorkerRuns, func(leaf PBinLeaf) error {
			rootLeaf := PBinLeaf{Key: bytes.Clone(leaf.Key), Value: bytes.Clone(leaf.Value), Stamp: leaf.Stamp}
			select {
			case rootInput <- rootLeaf:
			case <-ctx.Done():
				return ctx.Err()
			case <-rootDone:
				return streamErr
			}
			return emit(leaf)
		})
		if returnErr != nil {
			return returnErr
		}
		return streamErr
	}
	var writer *PBinRangeWriter
	limits := PBinRangeWriterLimits{MaxOps: pbinRangeWriterMaxOps, MaxBytes: pbinRangeWriterMaxBytes}
	if opts.RangeWriterLimits != nil {
		limits = *opts.RangeWriterLimits
	}
	limits.Workers = workers
	if opts.Resume != nil {
		resume := *opts.Resume
		if resume.SourceTxNum == 0 {
			resume.SourceTxNum = opts.EndTxNum
		}
		writer, err = NewPBinRangeWriterForResume(opts.TargetAggregator, opts.TargetDomain, resume.BaseTxNum, opts.EndTxNum)
		if writer != nil {
			writer.workers = workers
		}
	} else {
		writer, err = newPBinRangeWriter(opts.TargetAggregator, opts.TargetDomain, opts.EndTxNum, limits)
	}
	if err != nil {
		return common.Hash{}, err
	}
	root, err := writer.WriteAtBlock(ctx, opts.TargetTx, domains, leaves, opts.BlockNum)
	if opts.WorkerViews != nil {
		opts.WorkerViews.Add(writer.workerViews.Load())
	}
	if err != nil {
		return common.Hash{}, err
	}
	if opts.Resume == nil && root != streamRoot {
		return common.Hash{}, fmt.Errorf("pbin conversion: engine root %x differs from stream root %x", root, streamRoot)
	}
	return root, nil
}

func VerifyPBinDomainRoot(ctx context.Context, tx kv.TemporalTx, aggregator *Aggregator, domain kv.Domain) (common.Hash, error) {
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
	commitmentCtx.PrepareForVerification(tx)
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
