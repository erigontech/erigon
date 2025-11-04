// Copyright 2019 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

// Package forkid implements EIP-2124 (https://eips.ethereum.org/EIPS/eip-2124).
package forkid

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
	"math/big"
	"reflect"
	"slices"
	"strings"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/execution/chain"
)

var (
	// ErrRemoteStale is returned by the validator if a remote fork checksum is a
	// subset of our already applied forks, but the announced next fork block is
	// not on our already passed chain.
	ErrRemoteStale = errors.New("remote needs update")

	// ErrLocalIncompatibleOrStale is returned by the validator if a remote fork
	// checksum does not match any local checksum variation, signalling that the
	// two chains have diverged in the past at some point (possibly at genesis).
	ErrLocalIncompatibleOrStale = errors.New("local incompatible or needs update")
)

// ID is a fork identifier as defined by EIP-2124.
type ID struct {
	Hash       [4]byte // CRC32 checksum of the genesis block and passed fork block numbers
	Activation uint64  `rlp:"-"` // Block number/time activation for current fork
	Next       uint64  // Block number/time of the next upcoming fork, or 0 if no forks are known
}

// Filter is a fork id filter to validate a remotely advertised ID.
type Filter func(id ID) error

func NewIDFromForks(heightForks, timeForks []uint64, genesis common.Hash, headHeight, headTime uint64) ID {
	var activation uint64
	// Calculate the starting checksum from the genesis hash
	hash := crc32.ChecksumIEEE(genesis[:])

	// Calculate the current fork checksum and the next fork block
	for _, fork := range heightForks {
		if headHeight >= fork {
			// Fork already passed, checksum the previous hash and the fork number
			hash = ChecksumUpdate(hash, fork)
			activation = fork
			continue
		}
		return ID{Hash: ChecksumToBytes(hash), Activation: activation, Next: fork}
	}
	var next uint64
	for _, fork := range timeForks {
		if headTime >= fork {
			// Fork passed, checksum the previous hash and the fork time
			hash = ChecksumUpdate(hash, fork)
			activation = fork
			continue
		}
		next = fork
		break
	}
	return ID{Hash: ChecksumToBytes(hash), Activation: activation, Next: next}
}

func NextForkHashFromForks(heightForks, timeForks []uint64, genesis common.Hash, headHeight, headTime uint64) [4]byte {
	id := NewIDFromForks(heightForks, timeForks, genesis, headHeight, headTime)
	if id.Next == 0 {
		return id.Hash
	} else {
		hash := binary.BigEndian.Uint32(id.Hash[:])
		return ChecksumToBytes(ChecksumUpdate(hash, id.Next))
	}
}

// NewFilterFromForks creates a filter that returns if a fork ID should be rejected or not
// based on the provided current head.
func NewFilterFromForks(heightForks, timeForks []uint64, genesis common.Hash, headHeight, headTime uint64) Filter {
	return newFilter(heightForks, timeForks, genesis, headHeight, headTime)
}

// NewStaticFilter creates a filter at block zero.
func NewStaticFilter(config *chain.Config, genesisHash common.Hash, genesisTime uint64) Filter {
	heightForks, timeForks := GatherForks(config, genesisTime)
	return newFilter(heightForks, timeForks, genesisHash, 0 /* headHeight */, genesisTime)
}

// Simple heuristic returning true if the value is a Unix time after 2 Dec 2022.
// There are no block heights in the ballpark of 1.67 billion.
func forkIsTimeBased(fork uint64) bool {
	return fork >= 1670000000
}

func newFilter(heightForks, timeForks []uint64, genesis common.Hash, headHeight, headTime uint64) Filter {
	var forks []uint64
	forks = append(forks, heightForks...)
	forks = append(forks, timeForks...)

	// Calculate the all the valid fork hash and fork next combos
	sums := make([][4]byte, len(forks)+1) // 0th is the genesis
	hash := crc32.ChecksumIEEE(genesis[:])
	sums[0] = ChecksumToBytes(hash)
	for i, fork := range forks {
		hash = ChecksumUpdate(hash, fork)
		sums[i+1] = ChecksumToBytes(hash)
	}
	// Add two sentries to simplify the fork checks and don't require special
	// casing the last one.
	forks = append(forks, math.MaxUint64) // Last fork will never be passed

	// Create a validator that will filter out incompatible chains
	return func(id ID) error {
		// Run the fork checksum validation ruleset:
		//   1. If local and remote FORK_CSUM matches, compare local head to FORK_NEXT.
		//        The two nodes are in the same fork state currently. They might know
		//        of differing future forks, but that's not relevant until the fork
		//        triggers (might be postponed, nodes might be updated to match).
		//      1a. A remotely announced but remotely not passed block is already passed
		//          locally, disconnect, since the chains are incompatible.
		//      1b. No remotely announced fork; or not yet passed locally, connect.
		//   2. If the remote FORK_CSUM is a subset of the local past forks and the
		//      remote FORK_NEXT matches with the locally following fork block number,
		//      connect.
		//        Remote node is currently syncing. It might eventually diverge from
		//        us, but at this current point in time we don't have enough information.
		//   3. If the remote FORK_CSUM is a superset of the local past forks and can
		//      be completed with locally known future forks, connect.
		//        Local node is currently syncing. It might eventually diverge from
		//        the remote, but at this current point in time we don't have enough
		//        information.
		//   4. Reject in all other cases.
		for i, fork := range forks {
			// If our head is beyond this fork, continue to the next (we have a dummy
			// fork of maxuint64 as the last item to always fail this check eventually).
			if headHeight > fork || (forkIsTimeBased(fork) && headTime > fork) {
				continue
			}
			// Found the first unpassed fork block, check if our current state matches
			// the remote checksum (rule #1).
			if sums[i] == id.Hash {
				// Fork checksum matched, check if a remote future fork block already passed
				// locally without the local node being aware of it (rule #1a).
				if id.Next > 0 {
					if headHeight >= id.Next || (forkIsTimeBased(id.Next) && headTime >= id.Next) {
						return ErrLocalIncompatibleOrStale
					}
				}
				// Haven't passed locally a remote-only fork, accept the connection (rule #1b).
				return nil
			}
			// The local and remote nodes are in different forks currently, check if the
			// remote checksum is a subset of our local forks (rule #2).
			for j := 0; j < i; j++ {
				if sums[j] == id.Hash {
					// Remote checksum is a subset, validate based on the announced next fork
					if forks[j] != id.Next {
						return ErrRemoteStale
					}
					return nil
				}
			}
			// Remote chain is not a subset of our local one, check if it's a superset by
			// any chance, signalling that we're simply out of sync (rule #3).
			for j := i + 1; j < len(sums); j++ {
				if sums[j] == id.Hash {
					// Yay, remote checksum is a superset, ignore upcoming forks
					return nil
				}
			}
			// No exact, subset or superset match. We are on differing chains, reject.
			return ErrLocalIncompatibleOrStale
		}
		log.Error("Impossible fork ID validation", "id", id)
		return nil // Something's very wrong, accept rather than reject
	}
}

// ChecksumUpdate calculates the next IEEE CRC32 checksum based on the previous
// one and a fork block number (equivalent to CRC32(original-blob || fork)).
func ChecksumUpdate(hash uint32, fork uint64) uint32 {
	var blob [8]byte
	binary.BigEndian.PutUint64(blob[:], fork)
	return crc32.Update(hash, crc32.IEEETable, blob[:])
}

// ChecksumToBytes converts a uint32 checksum into a [4]byte array.
func ChecksumToBytes(hash uint32) [4]byte {
	var blob [4]byte
	binary.BigEndian.PutUint32(blob[:], hash)
	return blob
}

// GatherForks gathers all the known forks and creates a sorted list out of them.
func GatherForks(config *chain.Config, genesisTime uint64) (heightForks []uint64, timeForks []uint64) {
	// Gather all the fork block numbers via reflection
	kind := reflect.TypeOf(chain.Config{})
	conf := reflect.ValueOf(config).Elem()

	for i := 0; i < kind.NumField(); i++ {
		// Fetch the next field and skip non-fork rules
		field := kind.Field(i)
		time := false
		if !strings.HasSuffix(field.Name, "Block") {
			if !strings.HasSuffix(field.Name, "Time") {
				continue
			}
			time = true
		}
		if field.Type != reflect.TypeOf(new(big.Int)) {
			continue
		}
		// Extract the fork rule block number and aggregate it
		rule := conf.Field(i).Interface().(*big.Int)
		if rule != nil {
			if time {
				t := rule.Uint64()
				if t > genesisTime {
					timeForks = append(timeForks, t)
				}
			} else {
				heightForks = append(heightForks, rule.Uint64())
			}
		}
	}

	if config.Aura != nil && config.Aura.PosdaoTransition != nil {
		heightForks = append(heightForks, *config.Aura.PosdaoTransition)
	}

	if config.Bor != nil {
		if config.Bor.GetAgraBlock() != nil {
			heightForks = append(heightForks, config.Bor.GetAgraBlock().Uint64())
		}
		if config.Bor.GetNapoliBlock() != nil {
			heightForks = append(heightForks, config.Bor.GetNapoliBlock().Uint64())
		}
		if config.Bor.GetBhilaiBlock() != nil {
			heightForks = append(heightForks, config.Bor.GetBhilaiBlock().Uint64())
		}
	}

	// Sort the fork block numbers & times to permit chronological XOR
	slices.Sort(heightForks)
	slices.Sort(timeForks)
	// Deduplicate block numbers/times applying to multiple forks
	heightForks = common.RemoveDuplicatesFromSorted(heightForks)
	timeForks = common.RemoveDuplicatesFromSorted(timeForks)
	// Skip any forks in block 0, that's the genesis ruleset
	if len(heightForks) > 0 && heightForks[0] == 0 {
		heightForks = heightForks[1:]
	}
	if len(timeForks) > 0 && timeForks[0] == 0 {
		timeForks = timeForks[1:]
	}
	return heightForks, timeForks
}
