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

package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"slices"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
)

type exportPin struct {
	Block   uint64
	TxNum   uint64
	Domain  kv.Domain
	Variant commitment.TrieVariant
	Root    common.Hash
}

func sharedExportPin(ctx context.Context, tx kv.TemporalTx, headerAt func(uint64) (*types.Header, error), logger log.Logger) (exportPin, error) {
	return sharedExportPinWithTxNumReader(ctx, tx, headerAt, rawdbv3.TxNums, logger)
}

func sharedExportPinWithTxNumReader(ctx context.Context, tx kv.TemporalTx, headerAt func(uint64) (*types.Header, error), txNums rawdbv3.TxNumsReader, logger log.Logger) (exportPin, error) {
	head, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return exportPin{}, err
	}
	headHeader, err := headerAt(head)
	if err != nil {
		return exportPin{}, fmt.Errorf("read canonical header for block %d: %w", head, err)
	}
	if headHeader == nil {
		return exportPin{}, fmt.Errorf("canonical header for block %d not found", head)
	}
	chainConfig, err := exportChainConfig(tx)
	if err != nil {
		return exportPin{}, err
	}
	settings, err := state.ReadErigonDBSettings(tx.Debug().Dirs())
	if errors.Is(err, os.ErrNotExist) {
		settings = nil
	} else if err != nil {
		return exportPin{}, err
	}
	variant := state.TrieVariantHex
	if settings != nil {
		variant = settings.TrieVariantName()
	}
	domains := state.AggTx(tx).CommitmentDomains()
	binRegistered := slices.Contains(domains, kv.CommitmentBinDomain)
	domain, err := exportPinDomain(variant, binRegistered, chainConfig.IsBinaryTrie(headHeader.Time))
	if err != nil {
		return exportPin{}, err
	}
	checkpointBlock, checkpointTx, checkpointFound, err := exportCheckpoint(tx, domain)
	if err != nil {
		return exportPin{}, err
	}
	if checkpointFound {
		if err := checkExportPinTxNum(ctx, tx, txNums, checkpointBlock, checkpointTx); err != nil {
			return exportPin{}, err
		}
	}
	domainsView, err := execctx.NewSharedDomains(ctx, tx, logger, execctx.WithCommitmentDomainOnly(domain))
	if err != nil {
		return exportPin{}, err
	}
	defer domainsView.Close()
	commitmentCtx := domainsView.GetCommitmentCtxForDomain(domain)
	if commitmentCtx == nil {
		return exportPin{}, fmt.Errorf("export pin domain %s is not registered", domain)
	}
	txNum, blockNum, err := commitmentCtx.SeekCommitment(ctx, tx)
	if err != nil {
		return exportPin{}, err
	}
	if checkpointFound && (blockNum != checkpointBlock || txNum != checkpointTx) {
		return exportPin{}, fmt.Errorf("export pin checkpoint (%d, %d) differs from state record (%d, %d)", blockNum, txNum, checkpointBlock, checkpointTx)
	}
	if err := checkExportPinTxNum(ctx, tx, txNums, blockNum, txNum); err != nil {
		return exportPin{}, err
	}
	header, err := headerAt(blockNum)
	if err != nil {
		return exportPin{}, fmt.Errorf("read canonical header for block %d: %w", blockNum, err)
	}
	if header == nil {
		return exportPin{}, fmt.Errorf("canonical header for block %d not found", blockNum)
	}
	if binRegistered && chainConfig.IsBinaryTrie(header.Time) != (domain == kv.CommitmentBinDomain) {
		return exportPin{}, fmt.Errorf("export pin domain %s is not canonical at block %d", domain, blockNum)
	}
	root, err := commitmentCtx.Trie().RootHash()
	if err != nil {
		return exportPin{}, err
	}
	rootHash := common.BytesToHash(root)
	if err := checkRootPin(rootHash, header, blockNum); err != nil {
		return exportPin{}, err
	}
	return exportPin{Block: blockNum, TxNum: txNum, Domain: domain, Variant: commitmentCtx.Trie().Variant(), Root: rootHash}, nil
}

func exportCheckpoint(tx kv.TemporalTx, domain kv.Domain) (blockNum, txNum uint64, found bool, err error) {
	keys := [][]byte{commitment.KeyCommitmentV3State, commitment.KeyCommitmentState}
	for _, key := range keys {
		value, _, getErr := tx.GetLatest(domain, key, kv.GetLatestOptions{})
		if getErr != nil {
			return 0, 0, false, getErr
		}
		if len(value) == 0 {
			continue
		}
		if bytes.HasPrefix(value, []byte{commitment.CommitmentV3StateMarker}) || bytes.Equal(key, commitment.KeyCommitmentV3State) {
			blockNum, txNum, _, err = commitment.DecodeCommitmentV3State(value)
			return blockNum, txNum, true, err
		}
		if len(value) < 16 {
			return 0, 0, false, fmt.Errorf("export pin: commitment state for %s is too short", domain)
		}
		txNum, blockNum = commitmentdb.DecodeTxBlockNums(value)
		return blockNum, txNum, true, nil
	}
	return 0, 0, false, nil
}

func checkExportPinTxNum(ctx context.Context, tx kv.TemporalTx, txNums rawdbv3.TxNumsReader, blockNum, txNum uint64) error {
	maxTxNum, found, err := txNums.MaxExact(ctx, tx, blockNum)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("export pin block %d has no txNum mapping", blockNum)
	}
	if maxTxNum != txNum {
		return fmt.Errorf("export pin checkpoint (%d, %d) does not match block %d last txNum %d", blockNum, txNum, blockNum, maxTxNum)
	}
	return nil
}

func exportPinDomain(variant string, binRegistered, forked bool) (kv.Domain, error) {
	switch variant {
	case state.TrieVariantBin:
		if !forked {
			return 0, errors.New("export pin refuses a bin-only datadir before the binary trie fork")
		}
		return kv.CommitmentDomain, nil
	case state.TrieVariantHex:
		if forked {
			return 0, errors.New("export pin refuses a hex-only datadir after the binary trie fork")
		}
		return kv.CommitmentDomain, nil
	case state.TrieVariantHexBin:
		if forked {
			if !binRegistered {
				return 0, errors.New("export pin: hex+bin datadir has no binary commitment domain")
			}
			return kv.CommitmentBinDomain, nil
		}
		return kv.CommitmentDomain, nil
	default:
		return 0, fmt.Errorf("export pin: unknown trie variant %q", variant)
	}
}

func exportChainConfig(tx kv.TemporalTx) (*chain.Config, error) {
	genesisHash, err := rawdb.ReadCanonicalHash(tx, 0)
	if err != nil {
		return nil, err
	}
	config, err := rawdb.ReadChainConfig(tx, genesisHash)
	if err != nil {
		return nil, err
	}
	if config == nil {
		return nil, errors.New("export pin: chain config is missing")
	}
	return config, nil
}
