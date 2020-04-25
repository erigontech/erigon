package downloader

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

func (d *Downloader) spawnBodyDownloadStage(id string) (bool, error) {
	// Create cancel channel for aborting mid-flight and mark the master peer
	d.cancelLock.Lock()
	d.cancelCh = make(chan struct{})
	d.cancelPeer = id
	d.cancelLock.Unlock()

	defer d.Cancel() // No matter what, we can't leave the cancel channel open
	// Figure out how many blocks have already been downloaded
	origin, err := GetStageProgress(d.stateDB, Bodies)
	if err != nil {
		return false, err
	}
	// Figure out how many headers we have
	currentNumber := origin + 1
	var missingHeader uint64
	// Go over canonical headers and insert them into the queue
	const N = 65536
	var hashes [N]common.Hash
	var headers [N]*types.Header
	var hashCount = 0
	err = d.stateDB.Walk(dbutils.HeaderPrefix, dbutils.EncodeBlockNumber(currentNumber), 0, func(k, v []byte) (bool, error) {
		// Skip non relevant records
		if len(k) != 8+common.HashLength {
			return true, nil
		}
		blockNumber := binary.BigEndian.Uint64(k[:8])
		if blockNumber != currentNumber {
			log.Warn("Canonical hash is missing", "number", currentNumber, "got", blockNumber)
			missingHeader = currentNumber
			return false, nil
		}
		currentNumber++
		copy(hashes[hashCount][:], k[8:])
		header := new(types.Header)
		if err1 := rlp.Decode(bytes.NewReader(v), header); err1 != nil {
			log.Error("Invalid block header RLP", "hash", k[8:], "err", err1)
			return false, err1
		}
		headers[hashCount] = header
		hashCount++
		if hashCount == len(hashes) {
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		return false, fmt.Errorf("walking over canonical hashes: %v", err)
	}
	if missingHeader != 0 {
		if err1 := SaveStageProgress(d.stateDB, Headers, missingHeader); err1 != nil {
			return false, fmt.Errorf("resetting SyncStage Headers to missing header: %v", err1)
		}
		// This will cause the sync return to the header stage
		return false, nil
	}
	d.queue.Reset()
	if hashCount == 0 {
		// No more bodies to download
		return false, nil
	}
	from := origin + 1
	d.queue.Prepare(from, d.mode)
	d.queue.ScheduleBodies(from, hashes[:hashCount], headers[:hashCount])
	to := from + uint64(hashCount)
	select {
	case d.bodyWakeCh <- true:
	case <-d.cancelCh:
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
			case <-d.cancelCh:
			}
			return nil
		}
	}
}
