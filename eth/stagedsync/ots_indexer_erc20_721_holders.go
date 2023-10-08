package stagedsync

import (
	"bytes"
	"context"
	"encoding/binary"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/log/v3"
)

// Implements LogIndexerHandler interface in order to index token transfers
// (ERC20/ERC721)
type TransferLogHolderHandler struct {
	nft                bool
	indexBucket        string
	transfersCollector *etl.Collector
}

func NewTransferLogHolderHandler(tmpDir string, s *StageState, nft bool, indexBucket string, logger log.Logger) LogIndexerHandler[TransferAnalysisResult] {
	transfersCollector := etl.NewCollector(s.LogPrefix(), tmpDir, etl.NewOldestEntryBuffer(etl.BufferOptimalSize), logger)

	return &TransferLogHolderHandler{nft, indexBucket, transfersCollector}
}

// Add log's ethTx index to from/to addresses indexes
func (h *TransferLogHolderHandler) HandleMatch(match *TxMatchedLogs[TransferAnalysisResult]) {
	for _, res := range match.matchResults {
		if res.nft != h.nft {
			continue
		}

		// Register this ethTx into from/to transfer addresses indexes
		h.touchIndex(res.from, res.token, match.ethTx)
		h.touchIndex(res.to, res.token, match.ethTx)
	}
}

func (h *TransferLogHolderHandler) touchIndex(addr, token common.Address, ethTx uint64) {
	// Collect k as holder + token addr in order to get a free dedup from collector;
	// it'll be split into v during load phase
	k := make([]byte, length.Addr*2)
	copy(k, addr.Bytes())
	copy(k[length.Addr:], token.Bytes())

	// This is the first appearance of address as holder of tokenAddr (in this sync cycle; we don't
	// know about the DB, that'll be checked later during load phase); save it (and feed the cache).
	//
	// Even if cache is evicted, buffer implementation will ensure the key is added only once (saving
	// us temp space).
	v := make([]byte, length.BlockNum)
	binary.BigEndian.PutUint64(v, ethTx)
	if err := h.transfersCollector.Collect(k, v); err != nil {
		// TODO: change interface to return err
		// return startBlock, err
		log.Warn("unexpected error", "err", err)
	}
}

func (h *TransferLogHolderHandler) Flush(force bool) error {
	return nil
}

var expectedHolder = hexutil.MustDecode("0xF4445E7218889A91c63d6f95296459Fee6cabC30")
var expectedToken = hexutil.MustDecode("0x44Ce562D8296179630F71680dFb3cA773B4FF5DE")

func (h *TransferLogHolderHandler) Load(ctx context.Context, tx kv.RwTx) error {
	index, err := tx.CursorDupSort(h.indexBucket)
	if err != nil {
		return err
	}
	defer index.Close()

	loadFunc := func(k []byte, value []byte, _ etl.CurrentTableReader, next etl.LoadNextFunc) error {
		// Buffer will guarantee there is only 1 occurrence of key per provider file; it may appear in
		// multiple files or in a previous sync cycle though, so safeguard here
		holder := k[:length.Addr]
		token := k[length.Addr:]

		v, err := index.SeekBothRange(holder, token)
		if err != nil {
			return err
		}
		if v != nil && bytes.HasPrefix(v, token) {
			existingEthTx := binary.BigEndian.Uint64(v[length.Addr:])
			newEthTx := binary.BigEndian.Uint64(value)
			if newEthTx >= existingEthTx {
				return nil
			}
		}

		newK := k[:length.Addr]
		newV := make([]byte, length.Addr+length.BlockNum)
		copy(newV, k[length.Addr:])
		copy(newV[length.Addr:], value)

		// smol hack: avoid next(...) bc we rely on dupsort here
		return tx.Put(h.indexBucket, newK, newV)
	}
	if err := h.transfersCollector.Load(tx, h.indexBucket, loadFunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return err
	}

	return nil
}

func (h *TransferLogHolderHandler) Close() {
	h.transfersCollector.Close()
}
