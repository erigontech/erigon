package bordb

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/polygon/bor/snaptype"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
)

// PruneBorBlocks - delete [1, to) old blocks after moving it to snapshots.
// keeps genesis in db: [1, to)
// doesn't change sequences of kv.EthTx and kv.NonCanonicalTxs
// doesn't delete Receipts, Senders, Canonical markers, TotalDifficulty
func PruneBorBlocks(tx kv.RwTx, blockTo uint64, blocksDeleteLimit int, SpanIdAt func(number uint64) uint64) error {
	c, err := tx.Cursor(kv.BorEventNums)
	if err != nil {
		return err
	}
	defer c.Close()
	var blockNumBytes [8]byte
	binary.BigEndian.PutUint64(blockNumBytes[:], blockTo)
	k, v, err := c.Seek(blockNumBytes[:])
	if err != nil {
		return err
	}
	var eventIdTo uint64 = math.MaxUint64
	if k != nil {
		eventIdTo = binary.BigEndian.Uint64(v)
	}
	c1, err := tx.RwCursor(kv.BorEvents)
	if err != nil {
		return err
	}
	defer c1.Close()
	counter := blocksDeleteLimit
	for k, _, err = c1.First(); err == nil && k != nil && counter > 0; k, _, err = c1.Next() {
		eventId := binary.BigEndian.Uint64(k)
		if eventId >= eventIdTo {
			break
		}
		if err = c1.DeleteCurrent(); err != nil {
			return err
		}
		counter--
	}
	if err != nil {
		return err
	}
	firstSpanToKeep := SpanIdAt(blockTo)
	c2, err := tx.RwCursor(kv.BorSpans)
	if err != nil {
		return err
	}
	defer c2.Close()
	counter = blocksDeleteLimit
	for k, _, err := c2.First(); err == nil && k != nil && counter > 0; k, _, err = c2.Next() {
		spanId := binary.BigEndian.Uint64(k)
		if spanId >= firstSpanToKeep {
			break
		}
		if err = c2.DeleteCurrent(); err != nil {
			return err
		}
		counter--
	}

	if snaptype.CheckpointsEnabled() {
		checkpointCursor, err := tx.RwCursor(kv.BorCheckpoints)
		if err != nil {
			return err
		}

		defer checkpointCursor.Close()
		lastCheckpointToRemove, err := heimdall.CheckpointIdAt(tx, blockTo)

		if err != nil {
			return err
		}

		var checkpointIdBytes [8]byte
		binary.BigEndian.PutUint64(checkpointIdBytes[:], uint64(lastCheckpointToRemove))
		for k, _, err := checkpointCursor.Seek(checkpointIdBytes[:]); err == nil && k != nil; k, _, err = checkpointCursor.Prev() {
			if err = checkpointCursor.DeleteCurrent(); err != nil {
				return err
			}
		}
	}

	if snaptype.MilestonesEnabled() {
		milestoneCursor, err := tx.RwCursor(kv.BorMilestones)

		if err != nil {
			return err
		}

		defer milestoneCursor.Close()

		var lastMilestoneToRemove heimdall.MilestoneId

		for blockCount := 1; err != nil && blockCount < blocksDeleteLimit; blockCount++ {
			lastMilestoneToRemove, err = heimdall.MilestoneIdAt(tx, blockTo-uint64(blockCount))

			if !errors.Is(err, heimdall.ErrMilestoneNotFound) {
				return err
			} else {
				if blockCount == blocksDeleteLimit-1 {
					return nil
				}
			}
		}

		var milestoneIdBytes [8]byte
		binary.BigEndian.PutUint64(milestoneIdBytes[:], uint64(lastMilestoneToRemove))
		for k, _, err := milestoneCursor.Seek(milestoneIdBytes[:]); err == nil && k != nil; k, _, err = milestoneCursor.Prev() {
			if err = milestoneCursor.DeleteCurrent(); err != nil {
				return err
			}
		}
	}

	return nil
}
