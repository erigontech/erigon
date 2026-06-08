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
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/erigontech/erigon/db/kv"
)

// EnsureCommitmentHistoryOlderCompatible checks that the configured
// --prune.commitment-history.older value (in blocks; 0 = unlimited) is
// compatible with what was previously persisted in kv.DatabaseInfo.
//
// configured reports whether the operator passed the flag this run; when false
// the persisted value is honored unchanged (the zero default is not an explicit
// request to go unlimited).
//
// Rules (when configured):
//   - First-time set: always allowed.
//   - 0 (unlimited) -> N: allowed (we have everything, simply start filtering newer downloads).
//   - N -> M where M <= N: allowed (shrinking the kept window is safe).
//   - N -> M where M > N, or N -> 0: rejected (would require re-downloading filtered files).
//
// On allowed transitions the new value is persisted. The function returns the
// effective value (whatever the caller should use afterwards).
func EnsureCommitmentHistoryOlderCompatible(tx kv.GetPut, configuredBlocks uint64, configured bool) (uint64, error) {
	stored, ok, err := getCommitmentHistoryOlder(tx)
	if err != nil {
		return configuredBlocks, err
	}
	if !configured {
		// Flag absent this run: honor whatever was persisted; never treat the
		// zero default as an explicit request to go unlimited.
		if ok {
			return stored, nil
		}
		return 0, nil
	}
	if !ok {
		if err := setCommitmentHistoryOlder(tx, configuredBlocks); err != nil {
			return configuredBlocks, err
		}
		return configuredBlocks, nil
	}

	switch {
	case stored == 0:
		// We had everything before; any value (including a tighter bound) is fine.
	case configuredBlocks == 0:
		return stored, fmt.Errorf(
			"--prune.commitment-history.older is being expanded from %d block(s) to unlimited; previously filtered files would need to be re-downloaded. delete the chaindata folder to start over",
			stored)
	case configuredBlocks > stored:
		return stored, fmt.Errorf(
			"--prune.commitment-history.older is being expanded from %d to %d block(s); previously filtered files would need to be re-downloaded. delete the chaindata folder to start over",
			stored, configuredBlocks)
	}

	if configuredBlocks != stored {
		if err := setCommitmentHistoryOlder(tx, configuredBlocks); err != nil {
			return configuredBlocks, err
		}
	}
	return configuredBlocks, nil
}

func getCommitmentHistoryOlder(tx kv.Getter) (uint64, bool, error) {
	v, err := tx.GetOne(kv.DatabaseInfo, kv.PruneCommitmentHistory)
	if err != nil {
		return 0, false, err
	}
	if len(v) == 0 {
		return 0, false, nil
	}
	if len(v) != 8 {
		return 0, false, fmt.Errorf("unexpected value length %d for %s", len(v), string(kv.PruneCommitmentHistory))
	}
	typ, err := tx.GetOne(kv.DatabaseInfo, keyType(kv.PruneCommitmentHistory))
	if err != nil {
		return 0, false, err
	}
	if !bytes.Equal(typ, kv.PruneTypeOlder) {
		return 0, false, fmt.Errorf("unexpected type %q for %s (expected %q)",
			string(typ), string(kv.PruneCommitmentHistory), string(kv.PruneTypeOlder))
	}
	return binary.BigEndian.Uint64(v), true, nil
}

func setCommitmentHistoryOlder(tx kv.GetPut, value uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], value)
	if err := tx.Put(kv.DatabaseInfo, kv.PruneCommitmentHistory, buf[:]); err != nil {
		return err
	}
	return tx.Put(kv.DatabaseInfo, keyType(kv.PruneCommitmentHistory), kv.PruneTypeOlder)
}
