package beacon_indicies

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	_ "github.com/mattn/go-sqlite3"
)

type SQLObject interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func ReadBlockSlotByBlockRoot(ctx context.Context, tx SQLObject, blockRoot libcommon.Hash) (uint64, error) {
	var slot uint64

	// Execute the query.
	err := tx.QueryRowContext(ctx, "SELECT slot FROM beacon_indicies WHERE beacon_block_root = ?", blockRoot[:]).Scan(&slot) // Note: blockRoot[:] converts [32]byte to []byte
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to retrieve slot for BeaconBlockRoot: %v", err)
	}

	return slot, nil
}

func ReadCanonicalBlockRoot(ctx context.Context, db SQLObject, slot uint64) (libcommon.Hash, error) {
	var blockRoot libcommon.Hash

	// Execute the query.
	err := db.QueryRowContext(ctx, "SELECT beacon_block_root FROM beacon_indicies WHERE slot = ? AND canonical = 1", slot).Scan(&blockRoot)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return libcommon.Hash{}, nil
		}
		return libcommon.Hash{}, fmt.Errorf("failed to retrieve BeaconBlockRoot for slot: %v", err)
	}

	// Convert retrieved []byte to [32]byte and return

	return blockRoot, nil
}

func MarkRootCanonical(ctx context.Context, db SQLObject, slot uint64, blockRoot libcommon.Hash) error {
	// First, reset the Canonical status for all other block roots with the same slot
	if _, err := db.ExecContext(ctx, "UPDATE beacon_indicies SET canonical = 0 WHERE slot = ? AND beacon_block_root != ?", slot, blockRoot[:]); err != nil {
		return fmt.Errorf("failed to reset canonical status for other block roots: %v", err)
	}

	// Next, mark the given blockRoot as canonical
	if _, err := db.ExecContext(ctx, "UPDATE beacon_indicies SET canonical = 1 WHERE slot = ? AND beacon_block_root = ?", slot, blockRoot[:]); err != nil {
		return fmt.Errorf("failed to mark block root as canonical: %v", err)
	}

	return nil
}

func GenerateBlockIndicies(ctx context.Context, db SQLObject, block *cltypes.BeaconBlock, forceCanonical bool) error {
	blockRoot, err := block.HashSSZ()
	if err != nil {
		return err
	}

	if forceCanonical {
		_, err = db.ExecContext(ctx, "DELETE FROM beacon_indicies WHERE slot = ?;", block.Slot)
		if err != nil {
			return fmt.Errorf("failed to write block root to beacon_indicies: %v", err)
		}
	}
	_, err = db.ExecContext(ctx, "INSERT OR IGNORE INTO beacon_indicies (slot, proposer_index, beacon_block_root, state_root, parent_block_root, canonical)  VALUES (?, ?, ?, ?, ?, 0);", block.Slot, block.ProposerIndex, blockRoot[:], block.StateRoot[:], block.ParentRoot[:])

	if err != nil {
		return fmt.Errorf("failed to write block root to beacon_indicies: %v", err)
	}

	return nil
}

func ReadParentBlockRoot(ctx context.Context, db SQLObject, blockRoot libcommon.Hash) (libcommon.Hash, error) {
	var parentRoot libcommon.Hash

	// Execute the query.
	err := db.QueryRowContext(ctx, "SELECT parent_block_root FROM beacon_indicies WHERE beacon_block_root = ?", blockRoot[:]).Scan(&parentRoot)
	if err != nil {
		if err == sql.ErrNoRows {
			return libcommon.Hash{}, nil
		}
		return libcommon.Hash{}, fmt.Errorf("failed to retrieve ParentBlockRoot for BeaconBlockRoot: %v", err)
	}

	return parentRoot, nil
}

func TruncateCanonicalChain(ctx context.Context, db SQLObject, slot uint64) error {
	// Execute the query.
	_, err := db.ExecContext(ctx, `
		UPDATE beacon_indicies
		SET canonical = 0
		WHERE slot > ?;
	`, slot)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("failed to truncate canonical chain: %v", err)
	}

	return nil
}

func PruneIndicies(ctx context.Context, db SQLObject, fromSlot, toSlot uint64) error {
	_, err := db.ExecContext(ctx, "DELETE FROM beacon_indicies WHERE slot >= ? AND slot <= ?", fromSlot, toSlot)
	if err != nil {
		return err
	}
	return nil
}

func IterateBeaconIndicies(ctx context.Context, db SQLObject, fromSlot, toSlot uint64, fn func(slot uint64, beaconBlockRoot, parentBlockRoot, stateRoot libcommon.Hash, canonical bool) bool) error {
	rows, err := db.QueryContext(ctx, "SELECT slot, beacon_block_root, state_root, parent_block_root, canonical FROM beacon_indicies WHERE slot >= ? AND slot <= ?", fromSlot, toSlot)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var slot uint64
		var beaconBlockRoot libcommon.Hash
		var stateRoot libcommon.Hash
		var parentBlockRoot libcommon.Hash
		var canonical uint64

		err := rows.Scan(&slot, &beaconBlockRoot, &stateRoot, &parentBlockRoot, &canonical)
		if err != nil {
			return err
		}
		if !fn(slot, beaconBlockRoot, parentBlockRoot, stateRoot, canonical != 0) {
			break
		}
	}

	if err = rows.Err(); err != nil {
		return err
	}

	return nil
}
