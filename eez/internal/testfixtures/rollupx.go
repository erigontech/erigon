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
	"bufio"
	"compress/gzip"
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

// RollupXBlocks are the first L2 blocks of a rollupX v0.1 network from
// genesis, in consensus RLP, and the hash its node served for the last.
type RollupXBlocks struct {
	ChainID uint64
	Raw     [][]byte
	Head    common.Hash
}

var rollupXBlocks = map[uint64]struct {
	dir, file string
	last      uint64
	head      common.Hash
}{
	699169: {"chiado-699169", "blocks-0-2600.hex.gz", 2600, common.HexToHash("0x2e4b3f30a9c03fad6d12252b940803a090b26ba24d17447c3651c4a19d4b54f1")},
	916969: {"chiado-916969", "blocks-0-770.hex.gz", 770, common.HexToHash("0x78f2c1113228ca31132d09dd13104526a0d388da347cb705a2869e41bb978617")},
}

// LoadRollupXBlocks reads the blocks of network chainID under rollupx/: one
// 0x-prefixed hex block per line, block 0 first.
func LoadRollupXBlocks(chainID uint64) (*RollupXBlocks, error) {
	src, ok := rollupXBlocks[chainID]
	if !ok {
		return nil, fmt.Errorf("no rollupX blocks of chain %d", chainID)
	}
	f, err := os.Open(Path("rollupx", src.dir, src.file))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	z, err := gzip.NewReader(f)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", src.file, err)
	}
	b := &RollupXBlocks{ChainID: chainID, Head: src.head}
	sc := bufio.NewScanner(z)
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		raw, err := hexutil.Decode(sc.Text())
		if err != nil {
			return nil, fmt.Errorf("%s: block %d: %w", src.file, len(b.Raw), err)
		}
		b.Raw = append(b.Raw, raw)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("%s: %w", src.file, err)
	}
	if uint64(len(b.Raw)) != src.last+1 {
		return nil, fmt.Errorf("%s: %d blocks, want %d", src.file, len(b.Raw), src.last+1)
	}
	return b, nil
}
