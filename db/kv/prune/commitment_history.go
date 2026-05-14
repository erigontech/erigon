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
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/erigontech/erigon/db/kv"
)

// Named modes for --prune.commitment-history.mode. They map to step counts via
// ResolveCommitmentHistorySteps. The step size is configurable (via erigondb.toml);
// these constants only describe the *number* of most-recent steps to keep.
const (
	CommitmentHistoryArchiveMode = "archive"
	CommitmentHistoryRecentMode  = "recent"
	CommitmentHistoryMediumMode  = "medium"
	CommitmentHistoryCustomMode  = "custom"

	CommitmentHistoryRecentSteps uint64 = 1
	CommitmentHistoryMediumSteps uint64 = 8
)

var ErrUnknownCommitmentHistoryMode = fmt.Errorf(
	"--prune.commitment-history.mode must be one of %q, %q, %q, %q",
	CommitmentHistoryArchiveMode, CommitmentHistoryRecentMode,
	CommitmentHistoryMediumMode, CommitmentHistoryCustomMode,
)

// ResolveCommitmentHistorySteps turns (mode, customSteps) from the CLI into the
// raw step distance stored in ethconfig.Sync.CommitmentHistoryDistanceSteps.
// A return of 0 means "unlimited" (archive).
func ResolveCommitmentHistorySteps(mode string, customSteps uint64) (uint64, error) {
	switch mode {
	case "", CommitmentHistoryArchiveMode:
		// Even in archive mode, allow a custom override if the user only passed --steps.
		if customSteps > 0 {
			return customSteps, nil
		}
		return 0, nil
	case CommitmentHistoryRecentMode:
		return CommitmentHistoryRecentSteps, nil
	case CommitmentHistoryMediumMode:
		return CommitmentHistoryMediumSteps, nil
	case CommitmentHistoryCustomMode:
		if customSteps == 0 {
			return 0, errors.New("--prune.commitment-history.mode=custom requires --prune.commitment-history.steps > 0")
		}
		return customSteps, nil
	default:
		return 0, ErrUnknownCommitmentHistoryMode
	}
}

// EnsureCommitmentHistoryDistanceCompatible checks that the configured step
// distance is compatible with what was previously persisted in kv.DatabaseInfo.
//
// Rules:
//   - First-time set: always allowed.
//   - 0 (unlimited) -> N: allowed (we have everything, simply start filtering newer downloads).
//   - N -> M where M <= N: allowed (shrinking the kept window is safe).
//   - N -> M where M > N, or N -> 0: rejected (would require re-downloading filtered files).
//
// On allowed transitions the new value is persisted. The function returns the
// effective value (whatever the caller should use afterwards).
func EnsureCommitmentHistoryDistanceCompatible(tx kv.GetPut, configured uint64) (uint64, error) {
	stored, ok, err := getCommitmentHistoryDistance(tx)
	if err != nil {
		return configured, err
	}
	if !ok {
		if err := setCommitmentHistoryDistance(tx, configured); err != nil {
			return configured, err
		}
		return configured, nil
	}

	switch {
	case stored == 0:
		// We had everything before; any value (including a tighter bound) is fine.
	case configured == 0:
		return stored, fmt.Errorf(
			"--prune.commitment-history is being expanded from %d step(s) to unlimited; previously filtered files would need to be re-downloaded. delete the chaindata folder to start over",
			stored)
	case configured > stored:
		return stored, fmt.Errorf(
			"--prune.commitment-history is being expanded from %d to %d step(s); previously filtered files would need to be re-downloaded. delete the chaindata folder to start over",
			stored, configured)
	}

	if configured != stored {
		if err := setCommitmentHistoryDistance(tx, configured); err != nil {
			return configured, err
		}
	}
	return configured, nil
}

func getCommitmentHistoryDistance(tx kv.Getter) (uint64, bool, error) {
	v, err := tx.GetOne(kv.DatabaseInfo, kv.PruneCommitmentHistoryBlocks)
	if err != nil {
		return 0, false, err
	}
	if len(v) == 0 {
		return 0, false, nil
	}
	if len(v) != 8 {
		return 0, false, fmt.Errorf("unexpected value length %d for %s", len(v), string(kv.PruneCommitmentHistoryBlocks))
	}
	return binary.BigEndian.Uint64(v), true, nil
}

func setCommitmentHistoryDistance(tx kv.GetPut, value uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], value)
	return tx.Put(kv.DatabaseInfo, kv.PruneCommitmentHistoryBlocks, buf[:])
}
