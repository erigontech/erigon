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
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

type PBinConvertOptions struct {
	SourceAggregator  *Aggregator
	SourceTx          kv.TemporalTx
	TargetAggregator  *Aggregator
	TargetTx          kv.TemporalRwTx
	TargetDomain      kv.Domain
	BlockNum          uint64
	EndTxNum          uint64
	Hash              eip8297.HashFn
	RangeWriterLimits *PBinRangeWriterLimits
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
	cfg := pbinConversionTrieConfig()
	domains, err := execctx.NewSharedDomains(ctx, opts.TargetTx, log.Root(), execctx.WithTrieConfig(cfg), execctx.WithCommitmentDomainOnly(opts.TargetDomain), execctx.WithoutCommitmentSeek())
	if err != nil {
		return common.Hash{}, err
	}
	defer domains.Close()

	sourceFiles := opts.SourceAggregator.BeginFilesRo()
	defer sourceFiles.Close()
	var streamRoot common.Hash
	var streamErr error
	leaves := func(emit func(PBinLeaf) error) error {
		streamRoot, streamErr = PBinRootFromStream(opts.Hash, func(add func(PBinLeaf) error) error {
			return ForEachPBinLeaf(sourceFiles, opts.SourceTx, true, func(leaf PBinLeaf) error {
				if addErr := add(leaf); addErr != nil {
					return addErr
				}
				return emit(leaf)
			})
		})
		return streamErr
	}
	var writer *PBinRangeWriter
	if opts.RangeWriterLimits == nil {
		writer, err = NewPBinRangeWriter(opts.TargetAggregator, opts.TargetDomain, opts.EndTxNum)
	} else {
		writer, err = newPBinRangeWriter(opts.TargetAggregator, opts.TargetDomain, opts.EndTxNum, *opts.RangeWriterLimits)
	}
	if err != nil {
		return common.Hash{}, err
	}
	root, err := writer.WriteAtBlock(ctx, opts.TargetTx, domains, leaves, opts.BlockNum)
	if err != nil {
		return common.Hash{}, err
	}
	if root != streamRoot {
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
	verifier, ok := trie.(interface{ Verify() error })
	if !ok {
		return common.Hash{}, fmt.Errorf("pbin verification: trie does not support verification")
	}
	if progressTrie, ok := trie.(interface{ SetVerifyProgress(func([]byte)) }); ok {
		var checked uint64
		nextProgress := time.Now()
		progressTrie.SetVerifyProgress(func(key []byte) {
			checked++
			if checked&1023 != 0 {
				return
			}
			now := time.Now()
			if now.Before(nextProgress) {
				return
			}
			nextProgress = now.Add(30 * time.Second)
			log.Root().Info("PBT verification progress", "phase", "pbt verify", "domain", domain.String(), "records", checked, "key_prefix", hex.EncodeToString(key[:min(len(key), 8)]))
		})
		defer progressTrie.SetVerifyProgress(nil)
	}
	if verifyErr := verifier.Verify(); verifyErr != nil {
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
