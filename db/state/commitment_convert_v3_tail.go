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
	"path/filepath"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

type tailFold struct {
	cur, prev map[string][]byte
}

func foldCommitmentTails(ctx context.Context, at *AggregatorRoTx, files VisibleFiles, db kv.TemporalRoDB, logger log.Logger) (map[string]tailFold, error) {
	folds := make(map[string]tailFold, len(files))
	var prev map[string][]byte
	for _, f := range files {
		cur, err := foldCommitmentTail(ctx, at, f, db, logger)
		if err != nil {
			return nil, fmt.Errorf("[commitment_convert] %s: %w", filepath.Base(f.Fullpath()), err)
		}
		folds[f.Fullpath()] = tailFold{cur: cur, prev: prev}
		prev = cur
	}
	return folds, nil
}

func foldCommitmentTail(ctx context.Context, at *AggregatorRoTx, f VisibleFile, db kv.TemporalRoDB, logger log.Logger) (map[string][]byte, error) {
	dt := at.d[kv.CommitmentDomain]
	end := f.EndRootNum()
	state, found, _, fileEnd, err := dt.getLatestFromFiles(commitment.KeyCommitmentState, nil, kv.Step(end/at.StepSize())-1)
	if err != nil || !found || fileEnd != end || len(state) < 8 {
		return nil, err
	}
	stateTxNum := binary.BigEndian.Uint64(state)
	if stateTxNum == end-1 {
		return nil, nil
	}
	differs := fmt.Errorf("commitment state txNum %d differs from file end txNum %d", stateTxNum, end-1)
	if len(dt.ht.files) > 0 {
		return nil, fmt.Errorf("%w; the tail after it is not folded while commitment history is present", differs)
	}
	st, err := detectFileState(at, f, 48)
	if err != nil && !errors.Is(err, errNoNonStateSamples) {
		return nil, err
	}
	if st.keysV2 {
		return nil, fmt.Errorf("%w; the tail after it is not folded into nibbles v2 keys", differs)
	}
	for _, d := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain} {
		if from := at.d[d].HistoryCoverageStart(); from > stateTxNum+1 {
			return nil, fmt.Errorf("%w; folding the tail needs %s history from txNum %d, it starts at %d", differs, d, stateTxNum+1, from)
		}
	}
	if db == nil {
		return nil, fmt.Errorf("%w; folding the tail needs the database", differs)
	}

	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	cfg := commitment.DefaultTrieConfig()
	cfg.Variant = commitment.VariantHexPatriciaTrie
	cfg.DeferBranchUpdates = false
	sd, err := execctx.NewSharedDomains(ctx, tx, logger, execctx.WithTrieConfig(cfg), execctx.WithoutSharedBranchCache())
	if err != nil {
		return nil, err
	}
	defer sd.Close()
	sd.GetCommitmentCtx().SetStateReader(commitmentdb.NewFilesOnlyStateReader(tx, end-1))
	seekTxNum, _, err := sd.SeekCommitment(ctx, tx)
	if err != nil {
		return nil, err
	}
	if seekTxNum != stateTxNum {
		return nil, fmt.Errorf("%w; the trie seeks to txNum %d", differs, seekTxNum)
	}
	if _, _, err = sd.TouchChangedKeysFromHistory(tx, stateTxNum+1, end); err != nil {
		return nil, err
	}
	blockNum, ok, err := rawdbv3.TxNums.FindBlockNum(ctx, tx, end-1)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("%w; no block holds txNum %d", differs, end-1)
	}
	if _, err = sd.ComputeCommitment(ctx, tx, true, blockNum, end-1, "", nil); err != nil {
		return nil, err
	}
	cur := map[string][]byte{}
	shards := sd.GetMemBatch().(*TemporalMemBatch).shards(kv.CommitmentDomain)
	for i := range shards {
		for k, vals := range shards[i].m {
			cur[k] = bytes.Clone(vals[len(vals)-1].data)
		}
	}
	return cur, nil
}
