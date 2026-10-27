// Copyright 2026 The Erigon Authors
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

package testfixtures

import (
	"fmt"
	"os"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
)

// EEZDevReplayFile is the eez.dev replay under rollupx/eezdev-699069.
const EEZDevReplayFile = "replay-0-2250.json"

// EEZDevReplay is the start of eez.dev, the eez-rollupX network on Chiado:
// its first fourteen settled batches with their L1 data, and the hashes of
// L2 blocks 0 to 2250.
type EEZDevReplay struct {
	BeaconGenesisTime uint64         `json:"beaconGenesisTime"`
	SecondsPerSlot    uint64         `json:"secondsPerSlot"`
	Registry          common.Address `json:"registry"`
	RollupID          uint64         `json:"rollupId"`
	Batches           []ReplayBatch  `json:"batches"`
	L2Hashes          []common.Hash  `json:"l2Hashes"`
}

// ReplayBatch is one settled batch: its L1 block, posting transaction and
// logs, and the blobs of its beacon slot.
type ReplayBatch struct {
	L1Block struct {
		Number     hexutil.Uint64 `json:"number"`
		Hash       common.Hash    `json:"hash"`
		ParentHash common.Hash    `json:"parentHash"`
		Timestamp  hexutil.Uint64 `json:"timestamp"`
		MixHash    common.Hash    `json:"mixHash"`
	} `json:"l1Block"`
	Transaction struct {
		Hash                common.Hash    `json:"hash"`
		Index               hexutil.Uint64 `json:"index"`
		Input               hexutil.Bytes  `json:"input"`
		BlobVersionedHashes []common.Hash  `json:"blobVersionedHashes"`
	} `json:"transaction"`
	Logs []struct {
		Topics           []common.Hash  `json:"topics"`
		Data             hexutil.Bytes  `json:"data"`
		TransactionIndex hexutil.Uint64 `json:"transactionIndex"`
		LogIndex         hexutil.Uint64 `json:"logIndex"`
	} `json:"logs"`
	Slot uint64 `json:"slot"`
	// SlotBlobs are the slot's blobs with their zero suffix trimmed.
	SlotBlobs []hexutil.Bytes `json:"slotBlobs"`
	L2Range   [2]uint64       `json:"l2Range"`
}

// Blobs returns the slot's blobs at full size.
func (b *ReplayBatch) Blobs() [][]byte {
	out := make([][]byte, len(b.SlotBlobs))
	for i, trimmed := range b.SlotBlobs {
		out[i] = make([]byte, BlobSize)
		copy(out[i], trimmed)
	}
	return out
}

// LoadEEZDevReplay reads rollupx/eezdev-699069/replay-0-2250.json.
func LoadEEZDevReplay() (*EEZDevReplay, error) {
	data, err := os.ReadFile(Path("rollupx", "eezdev-699069", EEZDevReplayFile))
	if err != nil {
		return nil, err
	}
	var r EEZDevReplay
	if err := decodeStrict(data, &r); err != nil {
		return nil, fmt.Errorf("%s: %w", EEZDevReplayFile, err)
	}
	for i := range r.Batches {
		for j, blob := range r.Batches[i].SlotBlobs {
			if len(blob) > BlobSize {
				return nil, fmt.Errorf("%s: batch %d blob %d: %d bytes", EEZDevReplayFile, i, j, len(blob))
			}
		}
	}
	return &r, nil
}
