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

package downloader

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/p2p/forkid"
)

// ExpectedParentIdentity is the caller-supplied ground-truth against
// which a fork manifest's declared parent identity is verified.
// A consumer builds it from its local chain registry for the parent
// chain named in ParentSection.Chain and calls ValidateParentIdentity.
type ExpectedParentIdentity struct {
	// GenesisHash is the parent chain's genesis block hash as the
	// consumer's local registry knows it.
	GenesisHash common.Hash

	// HeightForks + TimeForks are the parent chain's activated
	// continuous fork schedule sorted ascending (same convention as
	// forkid.GatherForks output). Only entries at or before the fork's
	// CutBlock are required for the ForkID cross-check to hold —
	// callers that want to enforce F.4 (parent forwards-compat) should
	// pass their FULL local schedule and rely on the manifest's
	// ParentForks being the frozen at-cut snapshot; a mismatch in
	// post-cut entries is not itself a rejection reason (see the
	// per-check errors below).
	HeightForks []uint64
	TimeForks   []uint64
}

// Sentinel errors so callers can distinguish which check failed and
// react appropriately (log, reject, prompt).
var (
	ErrParentGenesisHashMismatch  = errors.New("parent genesis hash on manifest does not match local registry")
	ErrParentGenesisHashMalformed = errors.New("parent genesis hash on manifest is not decodable hex")
	ErrParentForkIDMismatch       = errors.New("parent fork ID recomputed from manifest schedule does not match local registry at cut block")
)

// ExpectedParentIdentityForChain resolves the ExpectedParentIdentity a
// fork-follower cross-checks against, for a locally-known parent chain
// name. Reads the chainspec registry for genesis hash + chain.Config,
// then runs forkid.GatherForks to build the height/time fork arrays.
// genesisTime is required for GatherForks to place time-forks. Returns
// chainspec.ErrChainSpecUnknown when the parent chain isn't registered.
func ExpectedParentIdentityForChain(chainName string, genesisTime uint64) (ExpectedParentIdentity, error) {
	spec, err := chainspec.ChainSpecByName(chainName)
	if err != nil {
		return ExpectedParentIdentity{}, err
	}
	heightForks, timeForks := forkid.GatherForks(spec.Config, genesisTime)
	return ExpectedParentIdentity{
		GenesisHash: spec.GenesisHash,
		HeightForks: heightForks,
		TimeForks:   timeForks,
	}, nil
}

// ValidateParentIdentity is the E.2 cross-check: given a fork manifest's
// ParentSection and the consumer's locally-known identity for the
// declared parent chain, confirms the fork's lineage claim is consistent.
//
// Runs two checks:
//
//  1. Genesis-hash equality — section.ParentGenesisHash (hex) must
//     decode to expected.GenesisHash. A mismatch is unambiguous: the
//     manifest is claiming a lineage the consumer's local knowledge
//     rejects. Returns ErrParentGenesisHashMismatch.
//
//  2. Fork-ID equality at the cut block — the ForkID computed from
//     the manifest's ParentForks (snapshot at cut time) must equal
//     the ForkID computed from expected.HeightForks / TimeForks with
//     the same genesis hash and head=(CutBlock, MaxUint64). headHeight
//     = CutBlock naturally excludes height-forks activating past the
//     cut, so parents that add NEW height-forks after fork creation
//     (F.4 tolerance) don't trip the check. headTime = MaxUint64
//     includes every time-fork; callers that want F.4 tolerance for
//     time-forks must trim expected.TimeForks to the at-cut snapshot
//     before calling. Returns ErrParentForkIDMismatch.
//
// Returns nil when both checks pass. Zero-value ParentGenesisHash on
// the section (empty hex) is treated as an omitted field and skips
// the check — for a strict validation callers should first ensure
// the manifest carries it. ParentForks empty is also a skip on the
// fork-ID check.
func ValidateParentIdentity(section *ParentSection, expected ExpectedParentIdentity) error {
	if section == nil {
		return nil
	}

	if section.ParentGenesisHash != "" {
		got, err := decodeGenesisHash(section.ParentGenesisHash)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrParentGenesisHashMalformed, err)
		}
		if got != expected.GenesisHash {
			return fmt.Errorf("%w: manifest=%x local=%x", ErrParentGenesisHashMismatch, got, expected.GenesisHash)
		}
	}

	if len(section.ParentForks) > 0 {
		manifestHeight, manifestTime := splitForkActivations(section.ParentForks)
		gotID := forkid.NewIDFromForks(manifestHeight, manifestTime, expected.GenesisHash, section.CutBlock, math.MaxUint64)
		wantID := forkid.NewIDFromForks(expected.HeightForks, expected.TimeForks, expected.GenesisHash, section.CutBlock, math.MaxUint64)
		if gotID.Hash != wantID.Hash {
			return fmt.Errorf("%w: manifest=%x local=%x", ErrParentForkIDMismatch, gotID.Hash, wantID.Hash)
		}
	}

	return nil
}

// decodeGenesisHash parses a lower-case hex (no 0x prefix, 64 chars)
// into a 32-byte hash. Matches ParentSection's stated hex convention.
func decodeGenesisHash(s string) (common.Hash, error) {
	s = strings.TrimPrefix(s, "0x")
	if len(s) != 2*length.Hash {
		return common.Hash{}, fmt.Errorf("expected %d hex chars, got %d", 2*length.Hash, len(s))
	}
	var out common.Hash
	if _, err := hex.Decode(out[:], []byte(s)); err != nil {
		return common.Hash{}, err
	}
	return out, nil
}

// splitForkActivations converts ParentSection.ParentForks (Block/Time-
// tagged entries) into the two sorted arrays forkid.NewIDFromForks
// expects. Height-tagged entries (Block > 0) land in heightForks;
// time-tagged (Time > 0) land in timeForks. An entry with both set is
// invalid manifest content — height wins, time is ignored (defensive).
func splitForkActivations(forks []ForkActivation) (heightForks, timeForks []uint64) {
	for _, f := range forks {
		switch {
		case f.Block > 0:
			heightForks = append(heightForks, f.Block)
		case f.Time > 0:
			timeForks = append(timeForks, f.Time)
		}
	}
	sortAsc(heightForks)
	sortAsc(timeForks)
	return heightForks, timeForks
}

func sortAsc(xs []uint64) {
	for i := 1; i < len(xs); i++ {
		x, j := xs[i], i
		for ; j > 0 && xs[j-1] > x; j-- {
			xs[j] = xs[j-1]
		}
		xs[j] = x
	}
}
