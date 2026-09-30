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
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

type PBinConvertOptions struct {
	SourceAggregator *Aggregator
	SourceTx         kv.TemporalTx
	TargetAggregator *Aggregator
	TargetTx         kv.TemporalRwTx
	TargetDomain     kv.Domain
	BlockNum         uint64
	EndTxNum         uint64
	Hash             eip8297.HashFn
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
	cfg := commitment.DefaultTrieConfig()
	cfg.Variant = commitment.VariantBinPatriciaTrie
	if len(opts.TargetAggregator.CommitmentDomains()) > 1 {
		cfg.Variant = commitment.VariantCommitmentV3
	}
	cfg.EnableTrieWarmup = false
	domains, err := execctx.NewSharedDomains(ctx, opts.TargetTx, log.Root(), execctx.WithTrieConfig(cfg), execctx.WithCommitmentDomain(opts.TargetDomain), execctx.WithoutCommitmentSeek())
	if err != nil {
		return common.Hash{}, err
	}
	defer domains.Close()

	sourceFiles := opts.SourceAggregator.BeginFilesRo()
	defer sourceFiles.Close()
	rootBuilder, err := eip8297.NewStreamRootBuilder(opts.Hash)
	if err != nil {
		return common.Hash{}, err
	}
	streamSeen := false
	leaves := func(emit func(PBinLeaf) error) error {
		return ForEachPBinLeaf(sourceFiles, opts.SourceTx, false, func(leaf PBinLeaf) error {
			streamSeen = true
			if addErr := rootBuilder.Add(leaf.Key, leaf.Value); addErr != nil {
				return addErr
			}
			return emit(leaf)
		})
	}
	writer, err := NewPBinRangeWriter(opts.TargetAggregator, opts.TargetDomain, opts.EndTxNum)
	if err != nil {
		return common.Hash{}, err
	}
	root, err := writer.WriteAtBlock(ctx, opts.TargetTx, domains, leaves, opts.BlockNum)
	if err != nil {
		return common.Hash{}, err
	}
	if streamSeen {
		trie := domains.GetCommitmentCtx().Trie()
		verifier, ok := trie.(interface{ Verify() error })
		if !ok {
			return common.Hash{}, fmt.Errorf("pbin conversion: trie does not support verification")
		}
		if verifyErr := verifier.Verify(); verifyErr != nil {
			return common.Hash{}, fmt.Errorf("pbin conversion: verify: %w", verifyErr)
		}
	}
	streamRoot, err := rootBuilder.RootHash()
	if err != nil {
		return common.Hash{}, err
	}
	if !bytes.Equal(root[:], streamRoot[:]) {
		return common.Hash{}, fmt.Errorf("pbin conversion: engine root %x differs from stream root %x", root, streamRoot)
	}
	return root, nil
}

func VerifyPBinDomain(ctx context.Context, tx kv.TemporalTx, aggregator *Aggregator, domain kv.Domain) error {
	if tx == nil || aggregator == nil {
		return fmt.Errorf("pbin verification: missing database input")
	}
	cfg := commitment.DefaultTrieConfig()
	cfg.Variant = commitment.VariantBinPatriciaTrie
	if len(aggregator.CommitmentDomains()) > 1 && domain != kv.CommitmentBinDomain {
		cfg.Variant = commitment.VariantCommitmentV3
	}
	cfg.EnableTrieWarmup = false
	at := aggregator.BeginFilesRo()
	defer at.Close()
	if len(at.Files(domain)) == 0 {
		return nil
	}
	domains, err := execctx.NewSharedDomains(ctx, tx, log.Root(), execctx.WithTrieConfig(cfg), execctx.WithCommitmentDomain(domain), execctx.WithPBinOnly())
	if err != nil {
		return err
	}
	defer domains.Close()
	trie := domains.GetCommitmentCtx().Trie()
	verifier, ok := trie.(interface{ Verify() error })
	if !ok {
		return fmt.Errorf("pbin verification: trie does not support verification")
	}
	return verifier.Verify()
}
