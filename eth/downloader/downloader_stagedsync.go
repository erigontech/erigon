package downloader

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

// externsions for downloader needed for staged sync
func (d *Downloader) SpawnBodyDownloadStage(id string, origin uint64) (bool, error) {
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
	err := d.stateDB.Walk(dbutils.HeaderPrefix, dbutils.EncodeBlockNumber(currentNumber), 0, func(k, v []byte) (bool, error) {
		if err := common.Stopped(d.quitCh); err != nil {
			return false, err
		}

		// Skip non relevant records
		if len(k) == 8+len(dbutils.HeaderHashSuffix) && bytes.Equal(k[8:], dbutils.HeaderHashSuffix) {
			// This is how we learn about canonical chain
			blockNumber := binary.BigEndian.Uint64(k[:8])
			if blockNumber != currentNumber {
				log.Warn("Canonical hash is missing", "number", currentNumber, "got", blockNumber)
				missingHeader = currentNumber
				return false, nil
			}
			currentNumber++
			if hashCount < len(hashes) {
				copy(hashes[hashCount][:], v)
			}
			hashCount++
			if hashCount > len(hashes) { // We allow hashCount to go +1 over what it should be, to let headers to be read
				return false, nil
			}
			return true, nil
		}
		if len(k) != 8+common.HashLength {
			return true, nil
		}
		header := new(types.Header)
		if err1 := rlp.Decode(bytes.NewReader(v), header); err1 != nil {
			log.Error("Invalid block header RLP", "hash", k[8:], "err", err1)
			return false, err1
		}
		headers[common.BytesToHash(k[8:])] = header
		return true, nil
	})
	if err != nil {
		return false, fmt.Errorf("walking over canonical hashes: %w", err)
	}
	if missingHeader != 0 {
		if err1 := stages.SaveStageProgress(d.stateDB, stages.Headers, missingHeader); err1 != nil {
			return false, fmt.Errorf("resetting SyncStage Headers to missing header: %w", err1)
		}
		// This will cause the sync return to the header stage
		return false, nil
	}
	d.queue.Reset()
	if hashCount <= 1 {
		// No more bodies to download
		return false, nil
	}
	from := origin + 1
	d.queue.Prepare(from, d.mode)
	d.queue.ScheduleBodies(from, hashes[:hashCount-1], headers)
	to := from + uint64(hashCount-1)

	select {
	case d.bodyWakeCh <- true:
	case <-d.cancelCh:
	case <-d.quitCh:
		return false, errCanceled
	}

	// Now fetch all the bodies
	fetchers := []func() error{
		func() error { return d.fetchBodies(from) },
		func() error { return d.processBodiesStage(to) },
	}

	return true, d.spawnSync(fetchers)
}

// processBodiesStage takes fetch results from the queue and imports them into the chain.
// it doesn't execute blocks
func (d *Downloader) processBodiesStage(to uint64) error {
	for {
		if err := common.Stopped(d.quitCh); err != nil {
			return err
		}

		results := d.queue.Results(true)
		if len(results) == 0 {
			return nil
		}
		lastNumber, err := d.importBlockResults(results, false /* execute */)
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

func (d *Downloader) SpawnSync(fetchers []func() error) error {
	return d.spawnSync(fetchers)
}
