package downloader

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"github.com/ledgerwatch/turbo-geth/turbo/stages/bodydownload"
)

// externsions for downloader needed for staged sync
func (d *Downloader) SpawnBodyDownloadStage(
	logPrefix string,
	id string,
	s *stagedsync.StageState,
	u stagedsync.Unwinder,
	prefetchedBlocks *bodydownload.PrefetchedBlocks,
) (bool, error) {
	d.bodiesState = s
	d.bodiesUnwinder = u
	defer func() {
		d.bodiesState = nil
		d.bodiesUnwinder = nil
	}()

	origin := s.BlockNumber
	// Create cancel channel for aborting mid-flight and mark the master peer
	d.cancelLock.Lock()
	d.cancelCh = make(chan struct{})
	d.cancelPeer = id
	d.cancelLock.Unlock()

	defer d.Cancel() // No matter what, we can't leave the cancel channel open
	// Figure out how many headers we have
	currentNumber := origin + 1
	var missingHeader uint64
	// Go over canonical headers and insert them into the queue
	const N = 65536
	var hashes [N]common.Hash                         // Canonical hashes of the blocks
	var headers = make(map[common.Hash]*types.Header) // We use map because there might be more than one header by block number
	var hashCount = 0
	startKey := dbutils.EncodeBlockNumber(currentNumber)
	if err := d.stateDB.Walk(dbutils.HeaderCanonicalBucket, startKey, 0, func(k, v []byte) (bool, error) {
		// This is how we learn about canonical chain
		blockNumber := binary.BigEndian.Uint64(k[:8])
		if blockNumber != currentNumber {
			log.Warn(fmt.Sprintf("[%s] Canonical hash is missing", logPrefix), "number", currentNumber, "got", blockNumber)
			missingHeader = currentNumber
			return false, nil
		}
		currentNumber++
		if hashCount < len(hashes) {
			copy(hashes[hashCount][:], v)
			hashCount++
		} else {
			return false, nil
		}
		return true, nil

	}); err != nil {
		return false, fmt.Errorf("%s: walking over canonical hashes: %w", logPrefix, err)
	}

	if err := d.stateDB.Walk(dbutils.HeadersBucket, startKey, 0, func(k, v []byte) (bool, error) {
		if err := common.Stopped(d.quitCh); err != nil {
			return false, err
		}

		header := new(types.Header)
		if err1 := rlp.Decode(bytes.NewReader(v), header); err1 != nil {
			log.Error(fmt.Sprintf("[%s] Invalid block header RLP", logPrefix), "hash", k[8:], "err", err1)
			return false, err1
		}
		headers[common.BytesToHash(k[8:])] = header
		return currentNumber > binary.BigEndian.Uint64(k[:8]), nil
	}); err != nil {
		return false, fmt.Errorf("%s: walking over headers: %w", logPrefix, err)
	}
	if missingHeader != 0 {
		if err1 := u.UnwindTo(missingHeader, d.stateDB); err1 != nil {
			return false, fmt.Errorf("%s: resetting SyncStage Headers to missing header: %w", logPrefix, err1)
		}
		// This will cause the sync return to the header stage
		return false, nil
	}
	d.queue.Reset(blockCacheMaxItems, blockCacheInitialItems)
	if hashCount == 0 {
		// No more bodies to download
		return false, nil
	}

	prefetchedHashes := 0
	for prefetchedHashes < hashCount {
		h := hashes[prefetchedHashes]
		if block := prefetchedBlocks.Pop(h); block != nil {
			fr := fetchResultFromBlock(block)
			_, err := d.importBlockResults(logPrefix, []*fetchResult{fr})
			if err != nil {
				return false, err
			}
			prefetchedHashes++
		} else {
			break
		}
	}
	if prefetchedHashes > 0 {
		log.Debug(fmt.Sprintf("[%s] Used prefetched bodies", logPrefix), "count", prefetchedHashes, "to", origin+uint64(prefetchedHashes))
		return true, nil
	}
	log.Info(fmt.Sprintf("[%s] Downloading block bodies", logPrefix), "count", hashCount)
	from := origin + 1
	d.queue.Prepare(from)
	d.queue.ScheduleBodies(from, hashes[:hashCount], headers)
	to := from + uint64(hashCount)

	select {
	case d.bodyWakeCh <- true:
	case <-d.cancelCh:
	case <-d.quitCh:
		return false, errCanceled
	}

	// Now fetch all the bodies
	fetchers := []func() error{
		func() error { return d.fetchBodies(from) },
		func() error { return d.processBodiesStage(logPrefix, to) },
	}

	if err := d.spawnSync(fetchers); err != nil {
		return false, err
	}

	return true, nil
}

func fetchResultFromBlock(b *types.Block) *fetchResult {
	return &fetchResult{
		Header:       b.Header(),
		Uncles:       b.Uncles(),
		Transactions: b.Transactions(),
	}
}

// processBodiesStage takes fetch results from the queue and imports them into the chain.
// it doesn't execute blocks
func (d *Downloader) processBodiesStage(logPrefix string, to uint64) error {
	for {
		if err := common.Stopped(d.quitCh); err != nil {
			return err
		}

		results := d.queue.Results(logPrefix, true)
		if len(results) == 0 {
			return nil
		}
		lastNumber, err := d.importBlockResults(logPrefix, results)
		if err != nil {
			return err
		}
		if lastNumber == to {
			select {
			case d.bodyWakeCh <- false:
			case <-d.quitCh:
			case <-d.cancelCh:
			}
			return nil
		}
	}
}

func (d *Downloader) SpawnHeaderDownloadStage(
	fetchers []func() error,
	s *stagedsync.StageState,
	u stagedsync.Unwinder,
) error {
	d.headersState = s
	d.headersUnwinder = u
	d.bodiesState = s
	d.bodiesUnwinder = u
	defer func() {
		d.headersState = nil
		d.headersUnwinder = nil
		d.bodiesState = nil
		d.bodiesUnwinder = nil
	}()
	d.cancelLock.Lock()
	d.cancelCh = make(chan struct{})
	d.cancelLock.Unlock()
	defer d.Cancel() // No matter what, we can't leave the cancel channel open
	return d.spawnSync(fetchers)
}
