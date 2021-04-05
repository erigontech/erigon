package stagedsync

import (
	"fmt"
	"math/big"
	mrand "math/rand"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/metrics"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

var stageHeadersGauge = metrics.NewRegisteredGauge("stage/headers", nil)

func SpawnHeaderDownloadStage(s *StageState, u Unwinder, d DownloaderGlue, headersFetchers []func() error) error {
	err := d.SpawnHeaderDownloadStage(headersFetchers, s, u)
	if err == nil {
		s.Done()
	}
	return err
}

// Implements consensus.ChainReader
type ChainReader struct {
	config *params.ChainConfig
	db     ethdb.Getter
}

// Config retrieves the blockchain's chain configuration.
func (cr ChainReader) Config() *params.ChainConfig {
	return cr.config
}

// CurrentHeader retrieves the current header from the local chain.
func (cr ChainReader) CurrentHeader() *types.Header {
	hash := rawdb.ReadHeadHeaderHash(cr.db)
	number := rawdb.ReadHeaderNumber(cr.db, hash)
	return rawdb.ReadHeader(cr.db, hash, *number)
}

// GetHeader retrieves a block header from the database by hash and number.
func (cr ChainReader) GetHeader(hash common.Hash, number uint64) *types.Header {
	return rawdb.ReadHeader(cr.db, hash, number)
}

// GetHeaderByNumber retrieves a block header from the database by number.
func (cr ChainReader) GetHeaderByNumber(number uint64) *types.Header {
	hash, err := rawdb.ReadCanonicalHash(cr.db, number)
	if err != nil {
		log.Error("ReadCanonicalHash failed", "err", err)
		return nil
	}
	return rawdb.ReadHeader(cr.db, hash, number)
}

// GetHeaderByHash retrieves a block header from the database by its hash.
func (cr ChainReader) GetHeaderByHash(hash common.Hash) *types.Header {
	number := rawdb.ReadHeaderNumber(cr.db, hash)
	return rawdb.ReadHeader(cr.db, hash, *number)
}

// GetBlock retrieves a block from the database by hash and number.
func (cr ChainReader) GetBlock(hash common.Hash, number uint64) *types.Block {
	return rawdb.ReadBlock(cr.db, hash, number)
}

func VerifyHeaders(db ethdb.Getter, headers []*types.Header, config *params.ChainConfig, engine consensus.Engine, checkFreq int) error {
	// Generate the list of seal verification requests, and start the parallel verifier
	seals := make([]bool, len(headers))
	if checkFreq != 0 {
		// In case of checkFreq == 0 all seals are left false.
		for i := 0; i < len(seals)/checkFreq; i++ {
			index := i * checkFreq
			if index >= len(seals) {
				index = len(seals) - 1
			}
			seals[index] = true
		}
		// Last should always be verified to avoid junk.
		seals[len(seals)-1] = true
	}

	cancel, results := engine.VerifyHeaders(ChainReader{config, db}, headers, seals)
	defer cancel()

	// Iterate over the headers and ensure they all check out
	for i := 0; i < len(headers); i++ {
		// Otherwise wait for headers checks and ensure they pass
		if err := <-results; err != nil {
			return err
		}
	}
	return nil
}

func InsertHeaderChain(logPrefix string, db ethdb.Database, headers []*types.Header) (bool, bool, uint64, error) {
	start := time.Now()

	// ignore headers that we already have
	alreadyCanonicalIndex := 0
	for _, h := range headers {
		number := h.Number.Uint64()
		ch, err := rawdb.ReadCanonicalHash(db, number)
		if err != nil {
			return false, false, 0, fmt.Errorf("[%s] %w", logPrefix, err)
		}
		if h.Hash() == ch {
			alreadyCanonicalIndex++
		} else {
			break
		}
		// If the header is a banned one, straight out abort
		if core.BadHashes[h.Hash()] {
			log.Error(fmt.Sprintf(`[%s]
########## BAD BLOCK #########

Number: %v
Hash: 0x%x

Error: %v
##############################
`, logPrefix, h.Number, h.Hash(), core.ErrBlacklistedHash))
			return false, false, 0, core.ErrBlacklistedHash
		}
	}
	headers = headers[alreadyCanonicalIndex:]
	if len(headers) < 1 {
		return false, false, 0, nil
	}

	if rawdb.ReadHeader(db, headers[0].ParentHash, headers[0].Number.Uint64()-1) == nil {
		return false, false, 0, fmt.Errorf("%s: unknown parent %x", logPrefix, headers[0].ParentHash)
	}
	parentTd, pErr := rawdb.ReadTd(db, headers[0].ParentHash, headers[0].Number.Uint64()-1)
	if pErr != nil {
		return false, false, 0, fmt.Errorf("[%s] %w", logPrefix, pErr)
	}
	externTd := new(big.Int).Set(parentTd)
	for i, header := range headers {
		if i > 0 {
			if header.ParentHash != headers[i-1].Hash() {
				return false, false, 0, fmt.Errorf("%s: broken chain", logPrefix)
			}
		}
		externTd = externTd.Add(externTd, header.Difficulty)
	}
	headHash := rawdb.ReadHeadHeaderHash(db)
	headNumber := rawdb.ReadHeaderNumber(db, headHash)
	localTd, tdErr := rawdb.ReadTd(db, headHash, *headNumber)
	if tdErr != nil {
		return false, false, 0, tdErr
	}
	lastHeader := headers[len(headers)-1]
	// If the total difficulty is higher than our known, add it to the canonical chain
	// Second clause in the if statement reduces the vulnerability to selfish mining.
	// Please refer to http://www.cs.cornell.edu/~ie53/publications/btcProcFC.pdf
	newCanonical := externTd.Cmp(localTd) > 0

	if !newCanonical && externTd.Cmp(localTd) == 0 {
		if lastHeader.Number.Uint64() < *headNumber {
			newCanonical = true
		} else if lastHeader.Number.Uint64() == *headNumber {
			//nolint:gosec
			newCanonical = mrand.Float64() < 0.5
		}
	}

	var deepFork bool // Whether the forkBlock is outside this header chain segment
	ch, chErr := rawdb.ReadCanonicalHash(db, headers[0].Number.Uint64()-1)
	if chErr != nil {
		return false, false, 0, fmt.Errorf("[%s] %w", logPrefix, chErr)
	}
	if newCanonical && headers[0].ParentHash != ch {
		deepFork = true
	}
	var forkBlockNumber uint64
	var fork bool // Set to true if forkBlockNumber is initialised
	ignored := 0
	batch := db.NewBatch()
	// Do a full insert if pre-checks passed
	td := new(big.Int).Set(parentTd)
	for _, header := range headers {
		// we always add header difficulty to TD, because next blocks might
		// be inserted and we need the right value for them
		td = td.Add(td, header.Difficulty)
		if !newCanonical && rawdb.ReadHeaderNumber(batch, header.Hash()) != nil {
			// We cannot ignore blocks if they cause reorg
			ignored++
			continue
		}
		number := header.Number.Uint64()
		ch, chErr := rawdb.ReadCanonicalHash(batch, number)
		if chErr != nil {
			return false, false, 0, fmt.Errorf("[%s] %w", logPrefix, chErr)
		}
		hashesMatch := header.Hash() == ch
		if newCanonical && !deepFork && !fork && !hashesMatch {
			forkBlockNumber = number - 1
			fork = true
		} else if newCanonical && hashesMatch {
			forkBlockNumber = number
			fork = true
		}
		if newCanonical {
			if err := rawdb.WriteCanonicalHash(batch, header.Hash(), header.Number.Uint64()); err != nil {
				return false, false, 0, fmt.Errorf("[%s] %w", logPrefix, err)
			}
		}
		data, rlpErr := rlp.EncodeToBytes(header)
		if rlpErr != nil {
			return false, false, 0, fmt.Errorf("[%s] Failed to RLP encode header: %w", logPrefix, rlpErr)
		}
		if err := rawdb.WriteTd(batch, header.Hash(), header.Number.Uint64(), td); err != nil {
			return false, false, 0, fmt.Errorf("[%s] Failed to WriteTd: %w", logPrefix, err)
		}
		if err := batch.Put(dbutils.HeadersBucket, dbutils.HeaderKey(number, header.Hash()), data); err != nil {
			return false, false, 0, fmt.Errorf("[%s] Failed to store header: %w", logPrefix, err)
		}
		stageHeadersGauge.Update(int64(lastHeader.Number.Uint64()))
	}
	if deepFork {
		forkHeader := rawdb.ReadHeader(batch, headers[0].ParentHash, headers[0].Number.Uint64()-1)
		forkBlockNumber = forkHeader.Number.Uint64() - 1
		forkHash := forkHeader.ParentHash
		for {
			ch, err := rawdb.ReadCanonicalHash(batch, forkBlockNumber)
			if err != nil {
				return false, false, 0, fmt.Errorf("[%s] %w", logPrefix, err)
			}
			if forkHash == ch {
				break
			}

			if err = rawdb.WriteCanonicalHash(batch, forkHash, forkBlockNumber); err != nil {
				return false, false, 0, err
			}
			forkHeader = rawdb.ReadHeader(batch, forkHash, forkBlockNumber)
			forkBlockNumber = forkHeader.Number.Uint64() - 1
			forkHash = forkHeader.ParentHash
		}
		if err := rawdb.WriteCanonicalHash(batch, headers[0].ParentHash, headers[0].Number.Uint64()-1); err != nil {
			return false, false, 0, err
		}
	}
	reorg := newCanonical && forkBlockNumber < *headNumber
	if reorg {
		// Delete any canonical number assignments above the new head
		for i := lastHeader.Number.Uint64() + 1; i <= *headNumber; i++ {
			if err := rawdb.DeleteCanonicalHash(batch, i); err != nil {
				return false, false, 0, fmt.Errorf("[%s] %w", logPrefix, err)
			}
		}
	}
	if newCanonical {
		encoded := dbutils.EncodeBlockNumber(lastHeader.Number.Uint64())
		if err := batch.Put(dbutils.HeaderNumberBucket, lastHeader.Hash().Bytes(), encoded); err != nil {
			return false, false, 0, fmt.Errorf("[%s] failed to store hash to number mapping: %w", logPrefix, err)
		}
		if err := rawdb.WriteHeadHeaderHash(batch, lastHeader.Hash()); err != nil {
			return false, false, 0, fmt.Errorf("[%s] failed to write head header hash: %w", logPrefix, err)
		}
	}
	if err := batch.Commit(); err != nil {
		return false, false, 0, fmt.Errorf("%s: write header markers into disk: %w", logPrefix, err)
	}
	// Report some public statistics so the user has a clue what's going on
	ctx := []interface{}{
		"count", len(headers), "elapsed", common.PrettyDuration(time.Since(start)),
		"number", lastHeader.Number, "hash", lastHeader.Hash(),
	}
	if timestamp := time.Unix(int64(lastHeader.Time), 0); time.Since(timestamp) > time.Minute {
		ctx = append(ctx, []interface{}{"age", common.PrettyAge(timestamp)}...)
	}
	if ignored > 0 {
		ctx = append(ctx, []interface{}{"ignored", ignored}...)
	}
	if reorg {
		ctx = append(ctx, []interface{}{"reorg", reorg, "forkBlockNumber", forkBlockNumber}...)
	}

	log.Info(fmt.Sprintf("[%s] Imported new block headers", logPrefix), ctx...)
	return newCanonical, reorg, forkBlockNumber, nil
}
