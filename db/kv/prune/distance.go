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

package prune

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

const (
	blocksDistanceAliasHint  = `a block count, "keep-post-merge" or "keep-all"`
	historyDistanceAliasHint = `a block count or "keep-all"`
)

// ParseBlocksDistance parses a --prune.distance.blocks value. It accepts a
// decimal block count, the named sentinels ("keep-post-merge" →
// KeepPostMergeBlocksPruneMode, "keep-all" → KeepAllBlocksPruneMode), or an
// empty string (returns 0, meaning unset).
func ParseBlocksDistance(s string) (uint64, error) {
	switch normalizeDistanceAlias(s) {
	case "":
		return 0, nil
	case "keep-post-merge":
		return uint64(KeepPostMergeBlocksPruneMode), nil
	case "keep-all":
		return uint64(KeepAllBlocksPruneMode), nil
	default:
		return parseDistanceNumber(s, "--prune.distance.blocks", blocksDistanceAliasHint)
	}
}

// ParseHistoryDistance parses a --prune.distance value. It accepts a decimal
// block count, the "keep-all" alias (keep all state history), or an empty
// string (returns 0, meaning unset). The block-only "keep-post-merge" policy
// is rejected: chain history-expiry has no meaning for state history.
func ParseHistoryDistance(s string) (uint64, error) {
	switch normalizeDistanceAlias(s) {
	case "":
		return 0, nil
	case "keep-all":
		return math.MaxUint64, nil
	default:
		return parseDistanceNumber(s, "--prune.distance", historyDistanceAliasHint)
	}
}

// ParseCommitmentHistoryDistance parses a --prune.commitment-history.distance
// value. It accepts a decimal block count, the "keep-all" alias (keep all
// commitment history, the default), or an empty string (returns 0, meaning
// unset). Like --prune.distance, the block-only "keep-post-merge" policy is
// rejected: chain history-expiry has no meaning for commitment history.
func ParseCommitmentHistoryDistance(s string) (uint64, error) {
	switch normalizeDistanceAlias(s) {
	case "":
		return 0, nil
	case "keep-all":
		return uint64(KeepAllBlocksPruneMode), nil
	default:
		return parseDistanceNumber(s, "--prune.commitment-history.distance", historyDistanceAliasHint)
	}
}

func normalizeDistanceAlias(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

func parseDistanceNumber(s, flag, aliasHint string) (uint64, error) {
	// Base 0 (prefix-aware: 0x/0o/0b + leading-zero octal) matches how the
	// previous cli.Uint64Flag parsed these flags, so existing configs keep working.
	n, err := strconv.ParseUint(strings.TrimSpace(s), 0, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s value %q: expected %s", flag, s, aliasHint)
	}
	return n, nil
}

// blocksDistanceCLIValue renders a Blocks retention value as the operator-facing
// --prune.distance.blocks argument, preferring the named alias for sentinels.
func blocksDistanceCLIValue(v uint64) string {
	switch Distance(v) {
	case KeepPostMergeBlocksPruneMode:
		return "keep-post-merge"
	case KeepAllBlocksPruneMode:
		return "keep-all"
	default:
		return strconv.FormatUint(v, 10)
	}
}
