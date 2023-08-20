package beacon_indicies

import (
	"database/sql"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	_ "github.com/mattn/go-sqlite3"
)

func InitBeaconIndicies(db *sql.DB) error {
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS beacon_indicies (
		Slot INTEGER NOT NULL,
		BeaconBlockRoot BLOB NOT NULL CHECK(length(BeaconBlockRoot) = 32), -- Ensure it's 32 bytes
		StateRoot BLOB NOT NULL CHECK(length(BeaconBlockRoot) = 32),
		ParentBlockRoot BLOB NOT NULL CHECK(length(BeaconBlockRoot) = 32),
		Canonical INTEGER NOT NULL DEFAULT 0, -- 0 for false, 1 for true
		PRIMARY KEY (Slot, BeaconBlockRoot)  -- Composite key ensuring unique combination of Slot and BeaconBlockRoot
	);`); err != nil {
		return err
	}
	if _, err := db.Exec(`
		CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_canonical 
		ON beacon_indicies (Slot) 
		WHERE Canonical = 1;`); err != nil {
		return err
	}
	// Initialize the table for indicies
	return nil
}

func ReadBlockSlotByBlockRoot(db *sql.DB, blockRoot libcommon.Hash) (uint64, error) {
	var slot uint64

	// Execute the query.
	err := db.QueryRow("SELECT Slot FROM beacon_indicies WHERE BeaconBlockRoot = ?", blockRoot[:]).Scan(&slot) // Note: blockRoot[:] converts [32]byte to []byte
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to retrieve slot for BeaconBlockRoot: %v", err)
	}

	return slot, nil
}

func ReadCanonicalBlockRoot(db *sql.DB, slot uint64) (libcommon.Hash, error) {
	blockRootBytes := make([]byte, 32)

	// Execute the query.
	err := db.QueryRow("SELECT BeaconBlockRoot FROM beacon_indicies WHERE Slot = ? AND Canonical = 1", slot).Scan(&blockRootBytes)
	if err != nil {
		if err == sql.ErrNoRows {
			return libcommon.Hash{}, nil
		}
		return libcommon.Hash{}, fmt.Errorf("failed to retrieve BeaconBlockRoot for slot: %v", err)
	}

	// Convert retrieved []byte to [32]byte and return
	var blockRoot libcommon.Hash
	copy(blockRoot[:], blockRootBytes)

	return blockRoot, nil
}

func MarkRootCanonical(db *sql.DB, slot uint64, blockRoot libcommon.Hash) error {
	// First, reset the Canonical status for all other block roots with the same slot
	if _, err := db.Exec("UPDATE beacon_indicies SET Canonical = 0 WHERE Slot = ? AND BeaconBlockRoot != ?", slot, blockRoot[:]); err != nil {
		return fmt.Errorf("failed to reset canonical status for other block roots: %v", err)
	}

	// Next, mark the given blockRoot as canonical
	if _, err := db.Exec("UPDATE beacon_indicies SET Canonical = 1 WHERE Slot = ? AND BeaconBlockRoot = ?", slot, blockRoot[:]); err != nil {
		return fmt.Errorf("failed to mark block root as canonical: %v", err)
	}

	return nil
}

func GenerateBlockIndicies(db *sql.DB, block *cltypes.BeaconBlock, forceCanonical bool) error {
	blockRoot, err := block.HashSSZ()
	if err != nil {
		return err
	}
	if forceCanonical {
		_, err = db.Exec(`
		BEGIN TRANSACTION;
		-- Delete existing entries with the same Slot
		DELETE FROM beacon_indices WHERE Slot = ?;

		-- Insert the new entry
		INSERT INTO beacon_indices (Slot, BeaconBlockRoot, StateRoot, ParentBlockRoot, Canonical) 
		VALUES (?, ?, ?, ?, 1);
		COMMIT;`, block.Slot, block.Slot, blockRoot[:], block.StateRoot[:], block.ParentRoot[:])
	} else {
		_, err = db.Exec("INSERT OR IGNORE INTO beacon_indicies (Slot, BeaconBlockRoot, StateRoot, ParentBlockRoot, Canonical)  VALUES (?, ?, ?, ?, 0);", block.Slot, blockRoot[:], block.StateRoot[:], block.ParentRoot[:])
	}
	if err != nil {
		return fmt.Errorf("failed to write block root to beacon_indicies: %v", err)
	}

	return nil
}

func ReadParentBlockRoot(db *sql.DB, blockRoot libcommon.Hash) (libcommon.Hash, error) {
	var parentRoot libcommon.Hash

	// Execute the query.
	err := db.QueryRow("SELECT ParentBlockRoot FROM beacon_indicies WHERE BeaconBlockRoot = ?", blockRoot[:]).Scan(parentRoot[:])
	if err != nil {
		if err == sql.ErrNoRows {
			return libcommon.Hash{}, nil
		}
		return libcommon.Hash{}, fmt.Errorf("failed to retrieve ParentBlockRoot for BeaconBlockRoot: %v", err)
	}

	return parentRoot, nil
}

func TruncateCanonicalChain(db *sql.DB, slot uint64) error {
	// Execute the query.
	_, err := db.Exec(`
		UPDATE beacon_indicies
		SET Canonical = 0
		WHERE Slot > ?;
	`, slot)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		return fmt.Errorf("failed to truncate canonical chain: %v", err)
	}

	return nil
}
