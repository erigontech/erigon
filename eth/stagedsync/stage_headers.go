package stagedsync

import (
	"errors"
	"fmt"
	"math/big"
	"math/rand"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

var ErrUnknownParent = errors.New("unknown parent")

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
	db     ethdb.Database
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

func VerifyHeaders(db ethdb.Database, headers []*types.Header, engine consensus.EngineAPI, checkFreq int) error {
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

	return verifyHeaders(db, engine, headers, seals)
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
		if h.HashCache() == ch {
			alreadyCanonicalIndex++
		} else {
			break
		}
		// If the header is a banned one, straight out abort
		if core.BadHashes[h.HashCache()] {
			log.Error(fmt.Sprintf(`[%s]
########## BAD BLOCK #########

Number: %v
Hash: 0x%x

Error: %v
##############################
`, logPrefix, h.Number, h.HashCache(), core.ErrBlacklistedHash))
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
	parentTd, err := rawdb.ReadTd(db, headers[0].ParentHash, headers[0].Number.Uint64()-1)
	if err != nil {
		return false, false, 0, fmt.Errorf("[%s] %w", logPrefix, err)
	}
	externTd := new(big.Int).Set(parentTd)
	for i, header := range headers {
		if i > 0 {
			if header.ParentHash != headers[i-1].HashCache() {
				return false, false, 0, fmt.Errorf("%s: broken chain", logPrefix)
			}
		}
		externTd = externTd.Add(externTd, header.Difficulty)
	}
	headHash := rawdb.ReadHeadHeaderHash(db)
	headNumber := rawdb.ReadHeaderNumber(db, headHash)
	localTd, err := rawdb.ReadTd(db, headHash, *headNumber)
	if err != nil {
		return false, false, 0, err
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
			newCanonical = rand.Float64() < 0.5
		}
	}

	var deepFork bool // Whether the forkBlock is outside this header chain segment
	ch, err := rawdb.ReadCanonicalHash(db, headers[0].Number.Uint64()-1)
	if err != nil {
		return false, false, 0, fmt.Errorf("[%s] %w", logPrefix, err)
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
		if !newCanonical && rawdb.ReadHeaderNumber(batch, header.HashCache()) != nil {
			// We cannot ignore blocks if they cause reorg
			ignored++
			continue
		}
		number := header.Number.Uint64()
		ch, err := rawdb.ReadCanonicalHash(batch, number)
		if err != nil {
			return false, false, 0, fmt.Errorf("[%s] %w", logPrefix, err)
		}
		hashesMatch := header.HashCache() == ch
		if newCanonical && !deepFork && !fork && !hashesMatch {
			forkBlockNumber = number - 1
			fork = true
		} else if newCanonical && hashesMatch {
			forkBlockNumber = number
			fork = true
		}
		if newCanonical {
			if err = rawdb.WriteCanonicalHash(batch, header.HashCache(), header.Number.Uint64()); err != nil {
				return false, false, 0, fmt.Errorf("[%s] %w", logPrefix, err)
			}
		}
		data, err := rlp.EncodeToBytes(header)
		if err != nil {
			return false, false, 0, fmt.Errorf("[%s] Failed to RLP encode header: %w", logPrefix, err)
		}
		if err := rawdb.WriteTd(batch, header.HashCache(), header.Number.Uint64(), td); err != nil {
			return false, false, 0, fmt.Errorf("[%s] Failed to WriteTd: %w", logPrefix, err)
		}
		if err := batch.Put(dbutils.HeaderPrefix, dbutils.HeaderKey(number, header.HashCache()), data); err != nil {
			return false, false, 0, fmt.Errorf("[%s] Failed to store header: %w", logPrefix, err)
		}
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
		err = rawdb.WriteCanonicalHash(batch, headers[0].ParentHash, headers[0].Number.Uint64()-1)
		if err != nil {
			return false, false, 0, err
		}
	}
	reorg := newCanonical && forkBlockNumber < *headNumber
	if reorg {
		// Delete any canonical number assignments above the new head
		for i := lastHeader.Number.Uint64() + 1; i <= *headNumber; i++ {
			err = rawdb.DeleteCanonicalHash(batch, i)
			if err != nil {
				return false, false, 0, fmt.Errorf("[%s] %w", logPrefix, err)
			}
		}
	}
	if newCanonical {
		encoded := dbutils.EncodeBlockNumber(lastHeader.Number.Uint64())

		if err := batch.Put(dbutils.HeaderNumberPrefix, lastHeader.HashCache().Bytes(), encoded); err != nil {
			return false, false, 0, fmt.Errorf("[%s] failed to store hash to number mapping: %w", logPrefix, err)
		}
		if err := rawdb.WriteHeadHeaderHash(batch, lastHeader.HashCache()); err != nil {
			return false, false, 0, fmt.Errorf("[%s] failed to write head header hash: %w", logPrefix, err)
		}
	}
	if _, err := batch.Commit(); err != nil {
		return false, false, 0, fmt.Errorf("%s: write header markers into disk: %w", logPrefix, err)
	}

	// Report some public statistics so the user has a clue what's going on
	since := time.Since(start)
	ctx := []interface{}{
		"count", len(headers), "elapsed", common.PrettyDuration(since),
		"number", lastHeader.Number, "hash", lastHeader.HashCache(),
		"blk/sec", float64(len(headers)) / since.Seconds(),
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

func verifyHeaders(db ethdb.Database, engine consensus.EngineAPI, headers []*types.Header, seals []bool) error {
	toVerify := len(headers)
	if toVerify == 0 {
		return nil
	}

	id := rand.Uint64() //nolint
	engine.HeaderVerification() <- consensus.VerifyHeaderRequest{id, headers, seals, nil}

	reqResponses := make(map[common.Hash]struct{}, len(headers))

	for {
		select {
		case result := <-engine.VerifyResults():
			if result.Err != nil {
				return result.Err
			}

			if _, ok := reqResponses[result.Hash]; ok {
				continue
			}

			reqResponses[result.Hash] = struct{}{}
			if len(reqResponses) == toVerify {
				return nil
			}
		case result := <-engine.HeaderRequest():
			var err error

			length := 1
			if result.Number > 0 {
				length = int(result.Number)
			}

			headers := make([]*types.Header, 0, length)

			if result.HighestBlockNumber+1 < result.Number {
				result.Number = result.HighestBlockNumber + 1
			}

			parentHash := result.HighestHash

			var parentNumber int
			for parentNumber = int(result.HighestBlockNumber); parentNumber >= int(result.HighestBlockNumber+1)-int(result.Number); parentNumber-- {
				h := rawdb.ReadHeader(db, parentHash, uint64(parentNumber))
				if h == nil {
					err = fmt.Errorf("%w: block %d %s", ErrUnknownParent, parentNumber, parentHash.String())
					break
				}

				parentHash = h.ParentHash
				headers = append(headers, h)
			}

			resp := consensus.HeaderResponse{
				ID:      result.ID,
				Headers: headers,
			}

			if err != nil {
				resp.Headers = nil
				resp.BlockError = consensus.BlockError{
					parentHash,
					uint64(parentNumber + 1),
					err,
				}
			}

			engine.HeaderResponse() <- resp
		}
	}
}
