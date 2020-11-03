package stagedsync

import (
	"errors"
	"fmt"
	"math/big"
	"math/rand"
	"os"
	"runtime/pprof"
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
	if prof {
		f, err := os.Create("cpu-headers.prof")
		if err != nil {
			log.Error("could not create CPU profile", "error", err)
			return err
		}
		defer f.Close()
		if err = pprof.StartCPUProfile(f); err != nil {
			log.Error("could not start CPU profile", "error", err)
			return err
		}
		defer pprof.StopCPUProfile()
	}

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

func NewChainReader(config *params.ChainConfig, db ethdb.Database) ChainReader {
	return ChainReader{config, db}
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

func InsertHeaderChain(db ethdb.Database, headers []*types.Header, engine consensus.EngineProcess, checkFreq int) (bool, uint64, error) {
	start := time.Now()

	// ignore headers that we already have
	alreadyCanonicalIndex := 0
	for _, h := range headers {
		number := h.Number.Uint64()
		ch, err := rawdb.ReadCanonicalHash(db, number)
		if err != nil {
			return false, 0, err
		}
		if h.Hash() == ch {
			alreadyCanonicalIndex++
		} else {
			break
		}
		// If the header is a banned one, straight out abort
		if core.BadHashes[h.Hash()] {
			log.Error(fmt.Sprintf(`
########## BAD BLOCK #########

Number: %v
Hash: 0x%x

Error: %v
##############################
`, h.Number, h.Hash(), core.ErrBlacklistedHash))
			return false, 0, core.ErrBlacklistedHash
		}
	}
	headers = headers[alreadyCanonicalIndex:]
	if len(headers) < 1 {
		return false, 0, nil
	}

	if rawdb.ReadHeader(db, headers[0].ParentHash, headers[0].Number.Uint64()-1) == nil {
		return false, 0, ErrUnknownParent
	}
	parentTd := rawdb.ReadTd(db, headers[0].ParentHash, headers[0].Number.Uint64()-1)
	externTd := new(big.Int).Set(parentTd)
	for i, header := range headers {
		if i > 0 {
			if header.ParentHash != headers[i-1].Hash() {
				return false, 0, ErrUnknownParent
			}
		}
		externTd = externTd.Add(externTd, header.Difficulty)
	}
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

	if err := VerifyHeaders(db, headers, engine, seals); err != nil {
		return false, 0, err
	}

	headHash := rawdb.ReadHeadHeaderHash(db)
	headNumber := rawdb.ReadHeaderNumber(db, headHash)
	localTd := rawdb.ReadTd(db, headHash, *headNumber)
	lastHeader := headers[len(headers)-1]
	// If the total difficulty is higher than our known, add it to the canonical chain
	// Second clause in the if statement reduces the vulnerability to selfish mining.
	// Please refer to http://www.cs.cornell.edu/~ie53/publications/btcProcFC.pdf
	newCanonical := externTd.Cmp(localTd) > 0

	if !newCanonical && externTd.Cmp(localTd) == 0 {
		if lastHeader.Number.Uint64() < *headNumber {
			newCanonical = true
		} else if lastHeader.Number.Uint64() == *headNumber {
			newCanonical = rand.Float64() < 0.5
		}
	}

	var deepFork bool // Whether the forkBlock is outside this header chain segment
	ch, err := rawdb.ReadCanonicalHash(db, headers[0].Number.Uint64()-1)
	if err != nil {
		return false, 0, err
	}
	if newCanonical && headers[0].ParentHash != ch {
		deepFork = true
	}
	var forkBlockNumber uint64
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
		ch, err := rawdb.ReadCanonicalHash(batch, number)
		if err != nil {
			return false, 0, err
		}
		hashesMatch := header.Hash() == ch
		if newCanonical && !deepFork && forkBlockNumber == 0 && !hashesMatch {
			forkBlockNumber = number - 1
		} else if newCanonical && hashesMatch {
			forkBlockNumber = number
		}
		if newCanonical {
			rawdb.WriteCanonicalHash(batch, header.Hash(), header.Number.Uint64())
		}
		data, err := rlp.EncodeToBytes(header)
		if err != nil {
			log.Crit("Failed to RLP encode header", "err", err)
		}
		rawdb.WriteTd(batch, header.Hash(), header.Number.Uint64(), td)
		if err := batch.Put(dbutils.HeaderPrefix, dbutils.HeaderKey(number, header.Hash()), data); err != nil {
			log.Crit("Failed to store header", "err", err)
		}
	}
	if deepFork {
		forkHeader := rawdb.ReadHeader(batch, headers[0].ParentHash, headers[0].Number.Uint64()-1)
		forkBlockNumber = forkHeader.Number.Uint64() - 1
		forkHash := forkHeader.ParentHash
		for {
			ch, err := rawdb.ReadCanonicalHash(batch, forkBlockNumber)
			if err != nil {
				return false, 0, err
			}
			if forkHash == ch {
				break
			}

			rawdb.WriteCanonicalHash(batch, forkHash, forkBlockNumber)
			forkHeader = rawdb.ReadHeader(batch, forkHash, forkBlockNumber)
			forkBlockNumber = forkHeader.Number.Uint64() - 1
			forkHash = forkHeader.ParentHash
		}
		rawdb.WriteCanonicalHash(batch, headers[0].ParentHash, headers[0].Number.Uint64()-1)
	}
	reorg := newCanonical && forkBlockNumber < *headNumber
	if reorg {
		// Delete any canonical number assignments above the new head
		for i := lastHeader.Number.Uint64() + 1; i <= *headNumber; i++ {
			rawdb.DeleteCanonicalHash(batch, i)
		}
	}
	if newCanonical {
		encoded := dbutils.EncodeBlockNumber(lastHeader.Number.Uint64())

		if err := batch.Put(dbutils.HeaderNumberPrefix, lastHeader.Hash().Bytes(), encoded); err != nil {
			log.Crit("Failed to store hash to number mapping", "err", err)
		}
		rawdb.WriteHeadHeaderHash(batch, lastHeader.Hash())
	}
	if _, err := batch.Commit(); err != nil {
		return false, 0, fmt.Errorf("write header markers into disk: %w", err)
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
	log.Info("Imported new block headers", ctx...)
	return reorg, forkBlockNumber, nil
}

func VerifyHeaders(db rawdb.DatabaseReader, headers []*types.Header, engine consensus.EngineProcess, seals []bool) error {
	toVerify := len(headers)
	if toVerify == 0 {
		return nil
	}

	requests := make(chan consensus.VerifyHeaderRequest, 1)
	requests <- consensus.VerifyHeaderRequest{rand.Uint64(), headers, seals, nil}

	reqResponses := make(map[common.Hash]struct{}, len(headers))

	for {
		select {
		case req := <-requests:
			engine.HeaderVerification() <- req
		case result := <-engine.VerifyResults():
			if result.Err != nil {
				return result.Err
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

			parentHash := result.HighestHash
			parentNumber := result.HighestBlockNumber

			for i := 0; i < int(result.Number); i++ {
				h := rawdb.ReadHeaderByHash(db, parentHash)
				parentNumber = result.HighestBlockNumber - uint64(i)

				if h == nil {
					err = ErrUnknownParent
					break
				}

				parentHash = h.ParentHash
				headers = append(headers, h)
			}

			if err != nil {
				engine.HeaderResponse() <- consensus.HeaderResponse{
					result.ID,
					nil,
					consensus.BlockError{
						parentHash,
						parentNumber,
						err,
					},
				}
			} else {
				engine.HeaderResponse() <- consensus.HeaderResponse{result.ID, headers, consensus.BlockError{}}
			}
		}
	}
}
