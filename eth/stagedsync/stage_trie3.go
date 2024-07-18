// Copyright 2024 The Erigon Authors
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

package stagedsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv/dbutils"
	"github.com/erigontech/erigon-lib/kv/temporal"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/turbo/stages/headerdownload"

	"github.com/erigontech/erigon-lib/commitment"
	"github.com/erigontech/erigon-lib/kv/order"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/turbo/services"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/etl"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/types/accounts"
	"github.com/erigontech/erigon/turbo/trie"
)

func collectAndComputeCommitment(ctx context.Context, tx kv.RwTx, tmpDir string, toTxNum uint64) ([]byte, error) {
	domains, err := state.NewSharedDomains(tx, log.New())
	if err != nil {
		return nil, err
	}
	defer domains.Close()
	ac := domains.AggTx().(*state.AggregatorRoTx)

	// has to set this value because it will be used during domain.Commit() call.
	// If we do not, txNum of block beginning will be used, which will cause invalid txNum on restart following commitment rebuilding
	domains.SetTxNum(toTxNum)

	logger := log.New("stage", "patricia_trie", "block", domains.BlockNum())
	logger.Info("Collecting account/storage keys")
	collector := etl.NewCollector("collect_keys", tmpDir, etl.NewSortableBuffer(etl.BufferOptimalSize/2), logger)
	defer collector.Close()

	var totalKeys atomic.Uint64
	it, err := ac.DomainRangeLatest(tx, kv.AccountsDomain, nil, nil, -1)
	if err != nil {
		return nil, err
	}
	for it.HasNext() {
		k, _, err := it.Next()
		if err != nil {
			return nil, err
		}
		if err := collector.Collect(k, nil); err != nil {
			return nil, err
		}
		totalKeys.Add(1)
	}

	it, err = ac.DomainRangeLatest(tx, kv.CodeDomain, nil, nil, -1)
	if err != nil {
		return nil, err
	}
	for it.HasNext() {
		k, _, err := it.Next()
		if err != nil {
			return nil, err
		}
		if err := collector.Collect(k, nil); err != nil {
			return nil, err
		}
		totalKeys.Add(1)
	}

	it, err = ac.DomainRangeLatest(tx, kv.StorageDomain, nil, nil, -1)
	if err != nil {
		return nil, err
	}
	for it.HasNext() {
		k, _, err := it.Next()
		if err != nil {
			return nil, err
		}
		if err := collector.Collect(k, nil); err != nil {
			return nil, err
		}
		totalKeys.Add(1)
	}

	var (
		batchSize = uint64(10_000_000)
		processed atomic.Uint64
	)

	sdCtx := state.NewSharedDomainsCommitmentContext(domains, commitment.ModeDirect, commitment.VariantHexPatriciaTrie)

	loadKeys := func(k, v []byte, table etl.CurrentTableReader, next etl.LoadNextFunc) error {
		if sdCtx.KeysCount() >= batchSize {
			rh, err := sdCtx.ComputeCommitment(ctx, true, domains.BlockNum(), "")
			if err != nil {
				return err
			}
			logger.Info("Committing batch",
				"processed", fmt.Sprintf("%s/%s (%.2f%%)", libcommon.PrettyCounter(processed.Load()), libcommon.PrettyCounter(totalKeys.Load()),
					float64(processed.Load())/float64(totalKeys.Load())*100),
				"intermediate root", hex.EncodeToString(rh))
		}
		processed.Add(1)
		sdCtx.TouchKey(kv.AccountsDomain, string(k), nil)

		return nil
	}
	err = collector.Load(nil, "", loadKeys, etl.TransformArgs{Quit: ctx.Done()})
	if err != nil {
		return nil, err
	}
	collector.Close()

	rh, err := sdCtx.ComputeCommitment(ctx, true, domains.BlockNum(), "")
	if err != nil {
		return nil, err
	}
	logger.Info("Commitment has been reevaluated",
		"tx", domains.TxNum(),
		"root", hex.EncodeToString(rh),
		"processed", processed.Load(),
		"total", totalKeys.Load())

	if err := domains.Flush(ctx, tx); err != nil {
		return nil, err
	}

	return rh, nil
}

type blockBorders struct {
	Number    uint64
	FirstTx   uint64
	CurrentTx uint64
	LastTx    uint64
}

func (b blockBorders) Offset() uint64 {
	if b.CurrentTx > b.FirstTx && b.CurrentTx < b.LastTx {
		return b.CurrentTx - b.FirstTx
	}
	return 0
}

func countBlockByTxnum(ctx context.Context, tx kv.Tx, blockReader services.FullBlockReader, txNum uint64) (bb blockBorders, err error) {
	var txCounter uint64 = 0

	for i := uint64(0); i < math.MaxUint64; i++ {
		if i%1000000 == 0 {
			fmt.Printf("\r [%s] Counting block for txn %d: cur block %s cur txn %d\n", "restoreCommit", txNum, libcommon.PrettyCounter(i), txCounter)
		}

		h, err := blockReader.HeaderByNumber(ctx, tx, i)
		if err != nil {
			return blockBorders{}, err
		}

		bb.Number = i
		bb.FirstTx = txCounter
		txCounter++
		b, err := blockReader.BodyWithTransactions(ctx, tx, h.Hash(), i)
		if err != nil {
			return blockBorders{}, err
		}
		txCounter += uint64(len(b.Transactions))
		txCounter++
		bb.LastTx = txCounter

		if txCounter >= txNum {
			bb.CurrentTx = txNum
			return bb, nil
		}
	}
	return blockBorders{}, fmt.Errorf("block with txn %x not found", txNum)
}

type TrieCfg struct {
	db                kv.RwDB
	checkRoot         bool
	badBlockHalt      bool
	tmpDir            string
	saveNewHashesToDB bool // no reason to save changes when calculating root for mining
	blockReader       services.FullBlockReader
	hd                *headerdownload.HeaderDownload

	historyV3 bool
	agg       *state.Aggregator
}

func StageTrieCfg(db kv.RwDB, checkRoot, saveNewHashesToDB, badBlockHalt bool, tmpDir string, blockReader services.FullBlockReader, hd *headerdownload.HeaderDownload, historyV3 bool, agg *state.Aggregator) TrieCfg {
	return TrieCfg{
		db:                db,
		checkRoot:         checkRoot,
		tmpDir:            tmpDir,
		saveNewHashesToDB: saveNewHashesToDB,
		badBlockHalt:      badBlockHalt,
		blockReader:       blockReader,
		hd:                hd,

		historyV3: historyV3,
		agg:       agg,
	}
}

type HashStateCfg struct {
	db   kv.RwDB
	dirs datadir.Dirs
}

func StageHashStateCfg(db kv.RwDB, dirs datadir.Dirs) HashStateCfg {
	return HashStateCfg{
		db:   db,
		dirs: dirs,
	}
}

var ErrInvalidStateRootHash = errors.New("invalid state root hash")

func RebuildPatriciaTrieBasedOnFiles(rwTx kv.RwTx, cfg TrieCfg, ctx context.Context, logger log.Logger) (libcommon.Hash, error) {
	useExternalTx := rwTx != nil
	if !useExternalTx {
		var err error
		rwTx, err = cfg.db.BeginRw(context.Background())
		if err != nil {
			return trie.EmptyRoot, err
		}
		defer rwTx.Rollback()
	}
	txNumsReader := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.ReadTxNumFuncFromBlockReader(ctx, cfg.blockReader))
	var foundHash bool
	toTxNum := rwTx.(*temporal.Tx).AggTx().(*state.AggregatorRoTx).EndTxNumNoCommitment()
	ok, blockNum, err := txNumsReader.FindBlockNum(rwTx, toTxNum)
	if err != nil {
		return libcommon.Hash{}, err
	}
	if !ok {
		bb, err := countBlockByTxnum(ctx, rwTx, cfg.blockReader, toTxNum)
		if err != nil {
			return libcommon.Hash{}, err
		}
		blockNum = bb.Number
		foundHash = bb.Offset() != 0
	} else {
		firstTxInBlock, err := txNumsReader.Min(rwTx, blockNum)
		if err != nil {
			return libcommon.Hash{}, fmt.Errorf("failed to find first txNum in block %d : %w", blockNum, err)
		}
		lastTxInBlock, err := txNumsReader.Max(rwTx, blockNum)
		if err != nil {
			return libcommon.Hash{}, fmt.Errorf("failed to find last txNum in block %d : %w", blockNum, err)
		}
		if firstTxInBlock == toTxNum || lastTxInBlock == toTxNum {
			foundHash = true // state is in the beginning or end of block
		}
	}

	var expectedRootHash libcommon.Hash
	var headerHash libcommon.Hash
	var syncHeadHeader *types.Header
	if foundHash && cfg.checkRoot {
		syncHeadHeader, err = cfg.blockReader.HeaderByNumber(ctx, rwTx, blockNum)
		if err != nil {
			return trie.EmptyRoot, err
		}
		if syncHeadHeader == nil {
			return trie.EmptyRoot, fmt.Errorf("no header found with number %d", blockNum)
		}
		expectedRootHash = syncHeadHeader.Root
		headerHash = syncHeadHeader.Hash()
	}

	rh, err := collectAndComputeCommitment(ctx, rwTx, cfg.tmpDir, toTxNum)
	if err != nil {
		return trie.EmptyRoot, err
	}

	if foundHash && cfg.checkRoot && !bytes.Equal(rh, expectedRootHash[:]) {
		logger.Error(fmt.Sprintf("[RebuildCommitment] Wrong trie root of block %d: %x, expected (from header): %x. Block hash: %x", blockNum, rh, expectedRootHash, headerHash))
		rwTx.Rollback()

		return trie.EmptyRoot, errors.New("wrong trie root")
	}
	logger.Info(fmt.Sprintf("[RebuildCommitment] Trie root of block %d txNum %d: %x. Could not verify with block hash because txnum of state is in the middle of the block.", blockNum, toTxNum, rh))

	if !useExternalTx {
		if err := rwTx.Commit(); err != nil {
			return trie.EmptyRoot, err
		}
	}
	return libcommon.BytesToHash(rh), err
}

func NewPromoter(db kv.RwTx, dirs datadir.Dirs, ctx context.Context, logger log.Logger) *Promoter {
	return &Promoter{
		tx:               db,
		ChangeSetBufSize: 256 * 1024 * 1024,
		dirs:             dirs,
		ctx:              ctx,
		logger:           logger,
	}
}

type Promoter struct {
	tx               kv.RwTx
	ChangeSetBufSize uint64
	dirs             datadir.Dirs
	ctx              context.Context
	logger           log.Logger
}

func (p *Promoter) PromoteOnHistoryV3(logPrefix string, from, to uint64, storage bool) error {
	if to > from+16 {
		p.logger.Info(fmt.Sprintf("[%s] Incremental promotion", logPrefix), "from", from, "to", to, "storage", storage)
	}

	txnFrom, err := rawdbv3.TxNums.Min(p.tx, from+1)
	if err != nil {
		return err
	}
	txnTo := uint64(math.MaxUint64)
	collector := etl.NewCollector(logPrefix, p.dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize), p.logger)
	defer collector.Close()

	if storage {
		it, err := p.tx.(kv.TemporalTx).HistoryRange(kv.StorageHistory, int(txnFrom), int(txnTo), order.Asc, kv.Unlim)
		if err != nil {
			return err
		}
		for it.HasNext() {
			k, _, err := it.Next()
			if err != nil {
				return err
			}

			accBytes, err := p.tx.GetOne(kv.PlainState, k[:20])
			if err != nil {
				return err
			}
			incarnation := uint64(1)
			if len(accBytes) != 0 {
				incarnation, err = accounts.DecodeIncarnationFromStorage(accBytes)
				if err != nil {
					return err
				}
				if incarnation == 0 {
					continue
				}
			}
			plainKey := dbutils.PlainGenerateCompositeStorageKey(k[:20], incarnation, k[20:])
			newV, err := p.tx.GetOne(kv.PlainState, plainKey)
			if err != nil {
				return err
			}
			newK, err := transformPlainStateKey(plainKey)
			if err != nil {
				return err
			}
			if err := collector.Collect(newK, newV); err != nil {
				return err
			}
		}
		if err := collector.Load(p.tx, kv.HashedStorage, etl.IdentityLoadFunc, etl.TransformArgs{Quit: p.ctx.Done()}); err != nil {
			return err
		}
		return nil
	}

	codeCollector := etl.NewCollector(logPrefix, p.dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize), p.logger)
	defer codeCollector.Close()

	it, err := p.tx.(kv.TemporalTx).HistoryRange(kv.AccountsHistory, int(txnFrom), int(txnTo), order.Asc, kv.Unlim)
	if err != nil {
		return err
	}
	for it.HasNext() {
		k, _, err := it.Next()
		if err != nil {
			return err
		}
		newV, err := p.tx.GetOne(kv.PlainState, k)
		if err != nil {
			return err
		}
		newK, err := transformPlainStateKey(k)
		if err != nil {
			return err
		}

		if err := collector.Collect(newK, newV); err != nil {
			return err
		}

		//code
		if len(newV) == 0 {
			continue
		}
		incarnation, err := accounts.DecodeIncarnationFromStorage(newV)
		if err != nil {
			return err
		}
		if incarnation == 0 {
			continue
		}
		plainKey := dbutils.PlainGenerateStoragePrefix(k, incarnation)
		var codeHash []byte
		codeHash, err = p.tx.GetOne(kv.PlainContractCode, plainKey)
		if err != nil {
			return fmt.Errorf("getFromPlainCodesAndLoad for %x, inc %d: %w", plainKey, incarnation, err)
		}
		if codeHash == nil {
			continue
		}
		newCodeK, err := transformContractCodeKey(plainKey)
		if err != nil {
			return err
		}
		if err = codeCollector.Collect(newCodeK, newV); err != nil {
			return err
		}
	}
	if err := collector.Load(p.tx, kv.HashedAccounts, etl.IdentityLoadFunc, etl.TransformArgs{Quit: p.ctx.Done()}); err != nil {
		return err
	}
	if err := codeCollector.Load(p.tx, kv.ContractCode, etl.IdentityLoadFunc, etl.TransformArgs{Quit: p.ctx.Done()}); err != nil {
		return err
	}
	return nil
}

func (p *Promoter) UnwindOnHistoryV3(logPrefix string, unwindFrom, unwindTo uint64, storage, codes bool) error {
	p.logger.Info(fmt.Sprintf("[%s] Unwinding started", logPrefix), "from", unwindFrom, "to", unwindTo, "storage", storage, "codes", codes)

	txnFrom, err := rawdbv3.TxNums.Min(p.tx, unwindTo+1)
	if err != nil {
		return err
	}
	txnTo := uint64(math.MaxUint64)
	collector := etl.NewCollector(logPrefix, p.dirs.Tmp, etl.NewOldestEntryBuffer(etl.BufferOptimalSize), p.logger)
	defer collector.Close()

	acc := accounts.NewAccount()
	if codes {
		it, err := p.tx.(kv.TemporalTx).HistoryRange(kv.AccountsHistory, int(txnFrom), int(txnTo), order.Asc, kv.Unlim)
		if err != nil {
			return err
		}
		for it.HasNext() {
			k, v, err := it.Next()
			if err != nil {
				return err
			}

			if len(v) == 0 {
				continue
			}
			if err := accounts.DeserialiseV3(&acc, v); err != nil {
				return err
			}

			incarnation := acc.Incarnation
			if incarnation == 0 {
				continue
			}
			plainKey := dbutils.PlainGenerateStoragePrefix(k, incarnation)
			codeHash, err := p.tx.GetOne(kv.PlainContractCode, plainKey)
			if err != nil {
				return fmt.Errorf("getCodeUnwindExtractFunc: %w, key=%x", err, plainKey)
			}
			newK, err := transformContractCodeKey(plainKey)
			if err != nil {
				return err
			}
			if err = collector.Collect(newK, codeHash); err != nil {
				return err
			}
		}

		return collector.Load(p.tx, kv.ContractCode, etl.IdentityLoadFunc, etl.TransformArgs{Quit: p.ctx.Done()})
	}

	if storage {
		it, err := p.tx.(kv.TemporalTx).HistoryRange(kv.StorageHistory, int(txnFrom), int(txnTo), order.Asc, kv.Unlim)
		if err != nil {
			return err
		}
		for it.HasNext() {
			k, v, err := it.Next()
			if err != nil {
				return err
			}
			val, err := p.tx.GetOne(kv.PlainState, k[:20])
			if err != nil {
				return err
			}
			incarnation := uint64(1)
			if len(val) != 0 {
				oldInc, _ := accounts.DecodeIncarnationFromStorage(val)
				incarnation = oldInc
			}
			plainKey := dbutils.PlainGenerateCompositeStorageKey(k[:20], incarnation, k[20:])
			newK, err := transformPlainStateKey(plainKey)
			if err != nil {
				return err
			}
			if err := collector.Collect(newK, v); err != nil {
				return err
			}
		}
		return collector.Load(p.tx, kv.HashedStorage, etl.IdentityLoadFunc, etl.TransformArgs{Quit: p.ctx.Done()})
	}

	it, err := p.tx.(kv.TemporalTx).HistoryRange(kv.AccountsHistory, int(txnFrom), int(txnTo), order.Asc, kv.Unlim)
	if err != nil {
		return err
	}
	for it.HasNext() {
		k, v, err := it.Next()
		if err != nil {
			return err
		}
		newK, err := transformPlainStateKey(k)
		if err != nil {
			return err
		}

		if len(v) == 0 {
			if err = collector.Collect(newK, nil); err != nil {
				return err
			}
			continue
		}
		if err := accounts.DeserialiseV3(&acc, v); err != nil {
			return err
		}
		if acc.Incarnation > 0 && acc.IsEmptyCodeHash() {
			if codeHash, err := p.tx.GetOne(kv.ContractCode, dbutils.GenerateStoragePrefix(newK, acc.Incarnation)); err == nil {
				copy(acc.CodeHash[:], codeHash)
			} else {
				return fmt.Errorf("adjusting codeHash for ks %x, inc %d: %w", newK, acc.Incarnation, err)
			}
		}

		value := make([]byte, acc.EncodingLengthForStorage())
		acc.EncodeForStorage(value)
		if err := collector.Collect(newK, value); err != nil {
			return err
		}
	}
	return collector.Load(p.tx, kv.HashedAccounts, etl.IdentityLoadFunc, etl.TransformArgs{Quit: p.ctx.Done()})
}

func promoteHashedStateIncrementally(logPrefix string, from, to uint64, tx kv.RwTx, cfg HashStateCfg, ctx context.Context, logger log.Logger) error {
	prom := NewPromoter(tx, cfg.dirs, ctx, logger)
	if err := prom.PromoteOnHistoryV3(logPrefix, from, to, false); err != nil {
		return err
	}
	if err := prom.PromoteOnHistoryV3(logPrefix, from, to, true); err != nil {
		return err
	}
	return nil
}

func PruneHashStateStage(s *PruneState, tx kv.RwTx, cfg HashStateCfg, ctx context.Context) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func transformPlainStateKey(key []byte) ([]byte, error) {
	switch len(key) {
	case length.Addr:
		// account
		hash, err := libcommon.HashData(key)
		return hash[:], err
	case length.Addr + length.Incarnation + length.Hash:
		// storage
		addrHash, err := libcommon.HashData(key[:length.Addr])
		if err != nil {
			return nil, err
		}
		inc := binary.BigEndian.Uint64(key[length.Addr:])
		secKey, err := libcommon.HashData(key[length.Addr+length.Incarnation:])
		if err != nil {
			return nil, err
		}
		compositeKey := dbutils.GenerateCompositeStorageKey(addrHash, inc, secKey)
		return compositeKey, nil
	default:
		// no other keys are supported
		return nil, fmt.Errorf("could not convert key from plain to hashed, unexpected len: %d", len(key))
	}
}

func transformContractCodeKey(key []byte) ([]byte, error) {
	if len(key) != length.Addr+length.Incarnation {
		return nil, fmt.Errorf("could not convert code key from plain to hashed, unexpected len: %d", len(key))
	}
	address, incarnation := dbutils.PlainParseStoragePrefix(key)

	addrHash, err := libcommon.HashData(address[:])
	if err != nil {
		return nil, err
	}

	compositeKey := dbutils.GenerateStoragePrefix(addrHash[:], incarnation)
	return compositeKey, nil
}

type HashPromoter struct {
	tx               kv.RwTx
	ChangeSetBufSize uint64
	TempDir          string
	logPrefix        string
	quitCh           <-chan struct{}
	logger           log.Logger
}

func NewHashPromoter(db kv.RwTx, tempDir string, quitCh <-chan struct{}, logPrefix string, logger log.Logger) *HashPromoter {
	return &HashPromoter{
		tx:               db,
		ChangeSetBufSize: 256 * 1024 * 1024,
		TempDir:          tempDir,
		quitCh:           quitCh,
		logPrefix:        logPrefix,
		logger:           logger,
	}
}

func (p *HashPromoter) PromoteOnHistoryV3(logPrefix string, from, to uint64, storage bool, load func(k []byte, v []byte) error) error {
	nonEmptyMarker := []byte{1}

	txnFrom, err := rawdbv3.TxNums.Min(p.tx, from+1)
	if err != nil {
		return err
	}
	txnTo := uint64(math.MaxUint64)

	if storage {
		compositeKey := make([]byte, length.Hash+length.Hash)
		it, err := p.tx.(kv.TemporalTx).HistoryRange(kv.StorageHistory, int(txnFrom), int(txnTo), order.Asc, kv.Unlim)
		if err != nil {
			return err
		}
		for it.HasNext() {
			k, v, err := it.Next()
			if err != nil {
				return err
			}
			addrHash, err := libcommon.HashData(k[:length.Addr])
			if err != nil {
				return err
			}
			secKey, err := libcommon.HashData(k[length.Addr:])
			if err != nil {
				return err
			}
			copy(compositeKey, addrHash[:])
			copy(compositeKey[length.Hash:], secKey[:])
			if len(v) != 0 {
				v = nonEmptyMarker
			}
			if err := load(compositeKey, v); err != nil {
				return err
			}
		}
		return nil
	}

	it, err := p.tx.(kv.TemporalTx).HistoryRange(kv.AccountsHistory, int(txnFrom), int(txnTo), order.Asc, kv.Unlim)
	if err != nil {
		return err
	}
	for it.HasNext() {
		k, v, err := it.Next()
		if err != nil {
			return err
		}
		newK, err := transformPlainStateKey(k)
		if err != nil {
			return err
		}
		if len(v) != 0 {
			v = nonEmptyMarker
		}
		if err := load(newK, v); err != nil {
			return err
		}
	}
	return nil
}
