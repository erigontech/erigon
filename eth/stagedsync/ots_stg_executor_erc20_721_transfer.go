package stagedsync

import (
	"bytes"
	"context"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/bitmapdb"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/log/v3"
)

func ERC20And721TransferIndexerExecutor(ctx context.Context, db kv.RoDB, tx kv.RwTx, isInternalTx bool, tmpDir string, chainConfig *chain.Config, blockReader services.FullBlockReader, engine consensus.Engine, startBlock, endBlock uint64, isShortInterval bool, logEvery *time.Ticker, s *StageState, logger log.Logger) (uint64, error) {
	analyzer, err := NewTransferLogAnalyzer()
	if err != nil {
		return startBlock, err
	}

	aggrHandler := NewMultiIndexerHandler[TransferAnalysisResult](
		NewTransferLogIndexerHandler(tmpDir, s, false, kv.OtsERC20TransferIndex, kv.OtsERC20TransferCounter, logger),
		NewTransferLogIndexerHandler(tmpDir, s, true, kv.OtsERC721TransferIndex, kv.OtsERC721TransferCounter, logger),
	)
	defer aggrHandler.Close()

	if startBlock == 0 && isInternalTx {
		return runConcurrentLogIndexerExecutor[TransferAnalysisResult](db, tx, blockReader, startBlock, endBlock, isShortInterval, logEvery, ctx, s, analyzer, aggrHandler)
	}
	return runIncrementalLogIndexerExecutor[TransferAnalysisResult](db, tx, blockReader, startBlock, endBlock, isShortInterval, logEvery, ctx, s, analyzer, aggrHandler)
}

// Topic hash for Transfer(address,address,uint256) event
var TRANSFER_TOPIC = hexutil.MustDecode("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")

type TransferAnalysisResult struct {
	// ERC20 == false, ERC721 == true
	nft bool

	// token address == log emitter address
	token libcommon.Address

	// topic1
	from libcommon.Address

	// topic2
	to libcommon.Address
}

func (r *TransferAnalysisResult) Unwind(tx kv.RwTx, nft bool, indexer LogIndexerUnwinder, ethTx uint64) error {
	if r.nft != nft {
		return nil
	}

	if err := indexer.UnwindAddress(tx, r.from, ethTx); err != nil {
		return err
	}
	if err := indexer.UnwindAddress(tx, r.to, ethTx); err != nil {
		return err
	}

	return nil
}

func (r *TransferAnalysisResult) UnwindHolding(tx kv.RwTx, nft bool, indexer LogIndexerUnwinder, ethTx uint64) error {
	if r.nft != nft {
		return nil
	}

	if err := indexer.UnwindAddressHolding(tx, r.from, r.token, ethTx); err != nil {
		return err
	}
	if err := indexer.UnwindAddressHolding(tx, r.to, r.token, ethTx); err != nil {
		return err
	}

	return nil
}

// This is an implementation of LogAnalyzer that detects Transfer events.
type TransferLogAnalyzer struct {
	// Caches positive/negative checks of address -> token? avoiding repeatedly DB checks
	// for popular tokens.
	erc20Cache *lru.ARCCache

	// Caches positive/negative checks of address -> token? avoiding repeatedly DB checks
	// for popular tokens.
	erc721Cache *lru.ARCCache
}

func NewTransferLogAnalyzer() (*TransferLogAnalyzer, error) {
	isERC20, err := lru.NewARC(1_000_000)
	if err != nil {
		return nil, err
	}

	isERC721, err := lru.NewARC(1_000_000)
	if err != nil {
		return nil, err
	}

	return &TransferLogAnalyzer{isERC20, isERC721}, nil
}

// Checks if a log entry is a standard Transfer event.
//
// If so, the returned payload says if it is an ERC20 or ERC721 (both uses the same topic0),
// the from and to addresses.
func (a *TransferLogAnalyzer) Inspect(tx kv.Tx, l *types.Log) (*TransferAnalysisResult, error) {
	// Transfer logs have 3 topics (ERC20: topic + from + to) or
	// 4 (ERC721: topic + from + to + id)
	if len(l.Topics) < 3 {
		return nil, nil
	}

	// Topic0 must match the standard Transfer topic sig
	if !bytes.Equal(TRANSFER_TOPIC, l.Topics[0].Bytes()) {
		return nil, nil
	}

	// It is a Transfer(address, address)
	tokenAddr := l.Address.Bytes()

	// Check token types
	isERC20, isERC721, err := a.checkTokenType(tx, tokenAddr)
	if err != nil {
		return nil, err
	}
	if !isERC20 && !isERC721 {
		return nil, nil
	}
	if isERC20 && isERC721 {
		// Faulty token which identifies itself as both ERC20 and ERC721
		// log.Info("XXXXX BOTH", "tokenAddr", hexutility.Encode(tokenAddr))
	}

	// Confirmed that tokenAddr IS an ERC20 or ERC721
	fromAddr := libcommon.BytesToAddress(l.Topics[1].Bytes()[length.Hash-length.Addr:])
	toAddr := libcommon.BytesToAddress(l.Topics[2].Bytes()[length.Hash-length.Addr:])
	return &TransferAnalysisResult{isERC721, l.Address, fromAddr, toAddr}, nil
}

func (a *TransferLogAnalyzer) checkTokenType(tx kv.Tx, tokenAddr []byte) (isERC20, isERC721 bool, err error) {
	// Check caches
	cachedERC20, okERC20 := a.erc20Cache.Get(string(tokenAddr))
	if okERC20 {
		isERC20 = cachedERC20.(bool)
	}
	cachedERC721, okERC721 := a.erc721Cache.Get(string(tokenAddr))
	if okERC721 {
		isERC721 = cachedERC721.(bool)
	}
	if okERC20 && okERC721 {
		return isERC20, isERC721, nil
	}

	// no entry == addr is not expect token type
	attr, err := tx.GetOne(kv.OtsAddrAttributes, tokenAddr)
	if err != nil {
		return false, false, err
	}
	if attr == nil {
		a.erc20Cache.Add(string(tokenAddr), false)
		a.erc721Cache.Add(string(tokenAddr), false)
		return false, false, nil
	}

	// decode flag
	bm := bitmapdb.NewBitmap64()
	defer bitmapdb.ReturnToPool64(bm)

	if _, err := bm.ReadFrom(bytes.NewReader(attr)); err != nil {
		return false, false, err
	}

	isERC20 = bm.Contains(kv.ADDR_ATTR_ERC20)
	a.erc20Cache.Add(string(tokenAddr), isERC20)

	isERC721 = bm.Contains(kv.ADDR_ATTR_ERC721)
	a.erc721Cache.Add(string(tokenAddr), isERC721)

	return isERC20, isERC721, nil
}
