package jsonrpc

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/log/v3"
)

func (api *Otterscan2APIImpl) TransferIntegrityChecker(ctx context.Context) error {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := api.checkTransferIntegrity(tx, kv.OtsERC20TransferIndex, kv.OtsERC20TransferCounter); err != nil {
		return err
	}
	if err := api.checkTransferIntegrity(tx, kv.OtsERC721TransferIndex, kv.OtsERC721TransferCounter); err != nil {
		return err
	}
	return nil
}

func (api *Otterscan2APIImpl) checkTransferIntegrity(tx kv.Tx, indexBucket, counterBucket string) error {
	index, err := tx.Cursor(indexBucket)
	if err != nil {
		return err
	}
	defer index.Close()

	counter, err := tx.CursorDupSort(counterBucket)
	if err != nil {
		return err
	}
	defer counter.Close()

	k, v, err := index.First()
	if err != nil {
		return err
	}
	ck, cv, err := counter.First()
	if err != nil {
		return err
	}

	var prevAddr []byte
	addrCount := uint64(0)
	expectedAddrCount := uint64(0)
	accCount := uint64(0)
	maxPrev := uint64(0)

	for k != nil {
		addr := k[:length.Addr]
		chunk := k[length.Addr:]
		newCount := uint64(0)
		counterChunk := []byte(nil)

		shouldBeSingleChunk := false

		// Address must match on index and counter
		if !bytes.Equal(addr, ck) {
			log.Warn("Integrity checker: counter bucket has different address", "index", indexBucket, "counter", counterBucket, "k", hexutility.Encode(k), "v", hexutility.Encode(v), "ck", hexutility.Encode(ck), "cv", hexutility.Encode(cv))
			return nil
		}

		changedAddress := !bytes.Equal(prevAddr, addr)
		if changedAddress {
			expectedAddrCount, err = counter.CountDuplicates()
			if err != nil {
				return err
			}
			shouldBeSingleChunk = expectedAddrCount == 1
			addrCount = 0
			accCount = 0
			maxPrev = 0
		}
		addrCount++

		// Chunk must match on index and chunk
		if len(cv) != length.Counter+length.Chunk {
			// Optimization is only allowed on 1st and unique counter of each address
			if !changedAddress || !shouldBeSingleChunk {
				log.Warn("Integrity checker: counter is optimized, but has multiple duplicates", "index", indexBucket, "counter", counterBucket, "k", hexutility.Encode(k), "v", hexutility.Encode(v), "ck", hexutility.Encode(ck), "cv", hexutility.Encode(cv), "c", expectedAddrCount)
				return nil
			}
			if len(cv) != 1 {
				log.Warn("Integrity checker: invalid optimized counter format", "index", indexBucket, "counter", counterBucket, "k", hexutility.Encode(k), "v", hexutility.Encode(v), "ck", hexutility.Encode(ck), "cv", hexutility.Encode(cv))
				return nil
			}

			newCount = uint64(cv[0]) + 1
			counterChunk = make([]byte, length.Chunk)
			binary.BigEndian.PutUint64(counterChunk, ^uint64(0))
		} else {
			newCount = binary.BigEndian.Uint64(cv[:length.Counter])
			counterChunk = cv[length.Counter:]
		}
		if !bytes.Equal(chunk, counterChunk) {
			log.Warn("Integrity checker: chunks don't match", "index", indexBucket, "counter", counterBucket, "k", hexutility.Encode(k), "v", hexutility.Encode(v), "ck", hexutility.Encode(ck), "cv", hexutility.Encode(cv))
			return nil
		}

		// Chunk data must obey standard format
		// Multiple of 8 bytes
		if uint64(len(v))%8 != 0 {
			log.Warn("Integrity checker: chunk bucket has multiple of 8 data", "index", indexBucket, "counter", counterBucket, "k", hexutility.Encode(k), "v", hexutility.Encode(v), "ck", hexutility.Encode(ck), "cv", hexutility.Encode(cv))
			return nil
		}
		// Last id must be equal to chunk (unless it is 0xffff...)
		if binary.BigEndian.Uint64(chunk) != ^uint64(0) && !bytes.Equal(chunk, v[len(v)-length.Chunk:]) {
			log.Warn("Integrity checker: last value in chunk doesn't match", "index", indexBucket, "counter", counterBucket, "k", hexutility.Encode(k), "v", hexutility.Encode(v), "ck", hexutility.Encode(ck), "cv", hexutility.Encode(cv))
			return nil
		}
		// Last chunk must be 0xffff...
		if addrCount == expectedAddrCount && binary.BigEndian.Uint64(chunk) != ^uint64(0) {
			log.Warn("Integrity checker: last chunk is not 0xffff...", "index", indexBucket, "counter", counterBucket, "k", hexutility.Encode(k), "v", hexutility.Encode(v), "ck", hexutility.Encode(ck), "cv", hexutility.Encode(cv))
			return nil
		}
		// Mid-chunk can't be 0xffff....
		if addrCount != expectedAddrCount && binary.BigEndian.Uint64(chunk) == ^uint64(0) {
			log.Warn("Integrity checker: mid chunk can't be 0xffff...", "index", indexBucket, "counter", counterBucket, "k", hexutility.Encode(k), "v", hexutility.Encode(v), "ck", hexutility.Encode(ck), "cv", hexutility.Encode(cv))
			return nil
		}

		// Prev count + card must be == newCount
		card := uint64(len(v)) / 8
		if accCount+card != newCount {
			log.Warn("Integrity checker: new count doesn't match", "index", indexBucket, "counter", counterBucket, "k", hexutility.Encode(k), "v", hexutility.Encode(v), "ck", hexutility.Encode(ck), "cv", hexutility.Encode(cv), "card", card, "prev", accCount)
			return nil
		}
		accCount = newCount

		// Examine data inside chunk, must be sorted asc
		for i := 0; i < len(v); i += 8 {
			val := binary.BigEndian.Uint64(v[i : i+8])
			if val <= maxPrev {
				log.Warn("Integrity checker: chunk data is not sorted asc", "index", indexBucket, "counter", counterBucket, "k", hexutility.Encode(k), "v", hexutility.Encode(v), "ck", hexutility.Encode(ck), "cv", hexutility.Encode(cv))
				return nil
			}
			maxPrev = val
		}

		// Next index
		prevAddr = addr
		k, v, err = index.Next()
		if err != nil {
			return err
		}

		// Next counter
		ck, cv, err = counter.NextDup()
		if err != nil {
			return err
		}
		if ck == nil {
			ck, cv, err = counter.NextNoDup()
			if err != nil {
				return err
			}
		}
	}
	if ck != nil {
		log.Warn("Integrity checker: index bucket EOF, counter bucket still has data", "index", indexBucket, "counter", counterBucket, "ck", hexutility.Encode(ck), "cv", hexutility.Encode(cv))
	}
	log.Info("Integrity checker finished")

	return nil
}

func (api *Otterscan2APIImpl) HoldingsIntegrityChecker(ctx context.Context) error {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := api.checkHoldingsIntegrity(ctx, tx, kv.OtsERC20Holdings); err != nil {
		return err
	}
	if err := api.checkHoldingsIntegrity(ctx, tx, kv.OtsERC721Holdings); err != nil {
		return err
	}
	return nil
}

func (api *Otterscan2APIImpl) checkHoldingsIntegrity(ctx context.Context, tx kv.Tx, indexBucket string) error {
	index, err := tx.CursorDupSort(indexBucket)
	if err != nil {
		return err
	}
	defer index.Close()

	blockNum, err := stages.GetStageProgress(tx, stages.OtsERC20And721Holdings)
	if err != nil {
		return err
	}

	baseTxId, err := api._blockReader.BaseTxIdForBlock(ctx, tx, blockNum)
	if err != nil {
		return err
	}
	block, err := api._blockReader.BlockByNumber(ctx, tx, blockNum)
	if err != nil {
		return err
	}
	txCount := block.Transactions().Len()
	baseTxId += uint64(txCount + 2)
	log.Info("xxx", "blockNum", blockNum, "txCount", txCount, "baseTxIdPlusOne", baseTxId)

	k, v, err := index.First()
	if err != nil {
		return err
	}

	for k != nil {
		ethTx := binary.BigEndian.Uint64(v[length.Addr:])
		if ethTx >= baseTxId {
			return fmt.Errorf("integrity checker: stageBlockNum=%d bucket=%s holder=%s token=%s ethTx=%d maxEthTx=%d", blockNum, indexBucket, hexutility.Encode(k), hexutility.Encode(v[:length.Addr]), ethTx, baseTxId)
		}

		k, v, err = index.NextDup()
		if err != nil {
			return err
		}
		if k == nil {
			k, v, err = index.NextNoDup()
			if err != nil {
				return err
			}
		}

		select {
		default:
		case <-ctx.Done():
			return common.ErrStopped
		}
	}

	log.Info("Integrity checker finished")
	return nil
}
