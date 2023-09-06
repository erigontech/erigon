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

	return blockRoot, nil
}

func MarkRootCanonical(ctx context.Context, db SQLObject, slot uint64, blockRoot libcommon.Hash) error {
	// First, reset the Canonical status for all other block roots with the same slot
	if _, err := db.ExecContext(ctx, "UPDATE beacon_indicies SET canonical = 0 WHERE slot = ?", slot); err != nil {
		return fmt.Errorf("failed to reset canonical status for other block roots: %v", err)
	}

	// Next, mark the given blockRoot as canonical
	if _, err := db.ExecContext(ctx, "UPDATE beacon_indicies SET canonical = 1 WHERE beacon_block_root = ?", blockRoot[:]); err != nil {
		return fmt.Errorf("failed to mark block root as canonical: %v", err)
	}

	return nil
}

func GenerateBlockIndicies(ctx context.Context, db SQLObject, signedBlock *cltypes.SignedBeaconBlock, forceCanonical bool) error {
	block := signedBlock.Block
	blockRoot, err := block.HashSSZ()
	if err != nil {
		return err
	}

	bodyRoot, err := block.Body.HashSSZ()
	if err != nil {
		return err
	}

	if forceCanonical {
		_, err = db.ExecContext(ctx, "DELETE FROM beacon_indicies WHERE slot = ?;", block.Slot)
		if err != nil {
			return fmt.Errorf("failed to write block root to beacon_indicies: %v", err)
		}
	}
	if _, err = db.ExecContext(ctx, "INSERT OR IGNORE INTO beacon_indicies (slot, proposer_index, beacon_block_root, state_root, parent_block_root, canonical, body_root, signature)  VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
		block.Slot,
		block.ProposerIndex,
		blockRoot[:],
		block.StateRoot[:],
		block.ParentRoot[:],
		forceCanonical,
		bodyRoot[:],
		signedBlock.Signature[:]); err != nil {
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
	rows, err := db.QueryContext(ctx, "SELECT slot, beacon_block_root, state_root, parent_block_root, canonical FROM beacon_indicies WHERE slot BETWEEN ? AND ?", fromSlot, toSlot)
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

func ReadBeaconBlockRootsInSlotRange(ctx context.Context, db SQLObject, fromSlot, count uint64) ([]libcommon.Hash, []uint64, error) {
	rows, err := db.QueryContext(ctx, "SELECT slot, beacon_block_root FROM beacon_indicies WHERE slot >= ? AND canonical > 0 LIMIT ?", fromSlot, count)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	roots := []libcommon.Hash{}
	slots := []uint64{}
	for rows.Next() {
		var beaconBlockRoot libcommon.Hash
		var slot uint64
		err := rows.Scan(&slot, &beaconBlockRoot)
		if err != nil {
			return nil, nil, err
		}
		roots = append(roots, beaconBlockRoot)
		slots = append(slots, slot)
	}

	if err = rows.Err(); err != nil {
		return nil, nil, err
	}

	return roots, slots, nil
}

func ReadSignedHeaderByBlockRoot(ctx context.Context, db SQLObject, blockRoot libcommon.Hash) (*cltypes.SignedBeaconBlockHeader, bool, error) {
	h := &cltypes.SignedBeaconBlockHeader{Header: &cltypes.BeaconBlockHeader{}}
	var canonical bool
	var signature []byte
	// Execute the query.
	err := db.QueryRowContext(ctx, `SELECT 
		slot, proposer_index, state_root, parent_block_root, canonical, body_root, signature 
		FROM beacon_indicies WHERE beacon_block_root = ?`, blockRoot).Scan(
		&h.Header.Slot,
		&h.Header.ProposerIndex,
		&h.Header.Root,
		&h.Header.ParentRoot,
		&canonical,
		&h.Header.BodyRoot,
		&signature,
	)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("failed to retrieve BeaconHeader: %v", err)
	}

	copy(h.Signature[:], signature)
	return h, canonical, nil
}

func parseRowsIntoHeaders(rows *sql.Rows) ([]*cltypes.SignedBeaconBlockHeader, []bool, error) {
	var signedHeaders []*cltypes.SignedBeaconBlockHeader
	var canonicals []bool

	for rows.Next() {
		var canonical bool
		var signature []byte
		h := &cltypes.SignedBeaconBlockHeader{Header: &cltypes.BeaconBlockHeader{}}
		err := rows.Scan(
			&h.Header.Slot,
			&h.Header.ProposerIndex,
			&h.Header.Root,
			&h.Header.ParentRoot,
			&canonical,
			&h.Header.BodyRoot,
			&signature,
		)
		if err != nil {
			return nil, nil, err
		}
		copy(h.Signature[:], signature)
		signedHeaders = append(signedHeaders, h)
		canonicals = append(canonicals, canonical)
	}
	return signedHeaders, canonicals, nil
}

func ReadSignedHeadersBySlot(ctx context.Context, db SQLObject, slot uint64) ([]*cltypes.SignedBeaconBlockHeader, []bool, error) {
	// Execute the query.
	rows, err := db.QueryContext(ctx, `SELECT 
		slot, proposer_index, state_root, parent_block_root, canonical, body_root, signature 
		FROM beacon_indicies WHERE slot = ?`, slot)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	return parseRowsIntoHeaders(rows)
}

func ReadSignedHeadersByParentRoot(ctx context.Context, db SQLObject, parentRoot libcommon.Hash) ([]*cltypes.SignedBeaconBlockHeader, []bool, error) {
	// Execute the query.
	rows, err := db.QueryContext(ctx, `SELECT 
		slot, proposer_index, state_root, parent_block_root, canonical, body_root, signature 
		FROM beacon_indicies WHERE parent_root = ?`, parentRoot)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	return parseRowsIntoHeaders(rows)
}

func ReadSignedHeadersByParentRootAndSlot(ctx context.Context, db SQLObject, parentRoot libcommon.Hash, slot uint64) ([]*cltypes.SignedBeaconBlockHeader, []bool, error) {
	// Execute the query.
	rows, err := db.QueryContext(ctx, `SELECT 
		slot, proposer_index, state_root, parent_block_root, canonical, body_root, signature 
		FROM beacon_indicies WHERE parent_root = ? AND slot = ?`, parentRoot, slot)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	return parseRowsIntoHeaders(rows)
}
