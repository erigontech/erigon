package beacon_indexes

import (
	"database/sql"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	_ "github.com/mattn/go-sqlite3"
)

// Define all queries templates
var beaconIndiciesCreateTableQuery = `CREATE TABLE IF NOT EXISTS beacon_indicies (
    Slot INTEGER NOT NULL,
	BeaconBlockRoot BLOB NOT NULL CHECK(length(BeaconBlockRoot) = 32), -- Ensure it's 32 bytes
	StateRoot BLOB NOT NULL CHECK(length(BeaconBlockRoot) = 32),
	ParentBlockRoot BLOB NOT NULL CHECK(length(BeaconBlockRoot) = 32),
    Canonical INTEGER NOT NULL DEFAULT 0, -- 0 for false, 1 for true
    PRIMARY KEY (Slot, BeaconBlockRoot)  -- Composite key ensuring unique combination of Slot and BeaconBlockRoot
);`

var beaconIndiciesUniqueIndiciesQuery = `
	CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_canonical 
	ON beacon_indicies (Slot) 
	WHERE Canonical = 1;`

var beaconReadBlockSlotByBlockRoot = "SELECT Slot FROM beacon_indicies WHERE BeaconBlockRoot = ?"

var beaconReadParentRootByBlockRoot = "SELECT ParentBlockRoot FROM beacon_indicies WHERE BeaconBlockRoot = ?"

var beaconReadCanonicalBlockRootBySlot = `SELECT BeaconBlockRoot FROM beacon_indicies 
	WHERE Slot = ? AND Canonical = 1`

var markRootCanonicalQuery = `UPDATE beacon_indicies 
	SET Canonical = 1 
	WHERE Slot = ? AND BeaconBlockRoot = ?`

var resetOtherRootsCanonicalQuery = `UPDATE beacon_indicies 
	SET Canonical = 0 
	WHERE Slot = ? AND BeaconBlockRoot != ?`

var insertOrUpdateBlockRoot = `
	INSERT OR IGNORE INTO beacon_indicies (Slot, BeaconBlockRoot, StateRoot, ParentBlockRoot, Canonical) 
	VALUES (?, ?, ?, ?, 0);
`

var truncateCanonicalChain = `
	UPDATE beacon_indicies
	SET Canonical = 0
	WHERE Slot > ?;
`

type SqliteBeaconIndexer struct {
	db *sql.DB
}

func NewSqlBeaconIndexer(db *sql.DB) (SqliteBeaconIndexer, error) {
	if _, err := db.Exec(beaconIndiciesCreateTableQuery); err != nil {
		return SqliteBeaconIndexer{}, err
	}
	if _, err := db.Exec(beaconIndiciesUniqueIndiciesQuery); err != nil {
		return SqliteBeaconIndexer{}, err
	}
	// Initialize the table for indicies
	return SqliteBeaconIndexer{
		db: db,
	}, nil
}

func (s SqliteBeaconIndexer) ReadBlockSlotByBlockRoot(blockRoot libcommon.Hash) (uint64, error) {
	var slot uint64

	// Execute the query.
	err := s.db.QueryRow(beaconReadBlockSlotByBlockRoot, blockRoot[:]).Scan(&slot) // Note: blockRoot[:] converts [32]byte to []byte
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to retrieve slot for BeaconBlockRoot: %v", err)
	}

	return slot, nil
}

func (s SqliteBeaconIndexer) ReadCanonicalBlockRoot(slot uint64) (libcommon.Hash, error) {
	blockRootBytes := make([]byte, 32)

	// Execute the query.
	err := s.db.QueryRow(beaconReadCanonicalBlockRootBySlot, slot).Scan(&blockRootBytes)
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

func (s SqliteBeaconIndexer) MarkRootCanonical(slot uint64, blockRoot libcommon.Hash) error {
	// First, reset the Canonical status for all other block roots with the same slot
	if _, err := s.db.Exec(resetOtherRootsCanonicalQuery, slot, blockRoot[:]); err != nil {
		return fmt.Errorf("failed to reset canonical status for other block roots: %v", err)
	}

	// Next, mark the given blockRoot as canonical
	if _, err := s.db.Exec(markRootCanonicalQuery, slot, blockRoot[:]); err != nil {
		return fmt.Errorf("failed to mark block root as canonical: %v", err)
	}

	return nil
}

func (s SqliteBeaconIndexer) GenerateBlockIndicies(block *cltypes.BeaconBlock) error {
	blockRoot, err := block.HashSSZ()
	if err != nil {
		return err
	}
	_, err = s.db.Exec(insertOrUpdateBlockRoot, block.Slot, blockRoot[:], block.StateRoot[:], block.ParentRoot[:])
	if err != nil {
		return fmt.Errorf("failed to write block root to beacon_indicies: %v", err)
	}

	return nil
}

func (s SqliteBeaconIndexer) ReadParentBlockRoot(blockRoot libcommon.Hash) (libcommon.Hash, error) {
	var parentRoot libcommon.Hash

	// Execute the query.
	err := s.db.QueryRow(beaconReadParentRootByBlockRoot, blockRoot[:]).Scan(parentRoot[:])
	if err != nil {
		if err == sql.ErrNoRows {
			return libcommon.Hash{}, nil
		}
		return libcommon.Hash{}, fmt.Errorf("failed to retrieve ParentBlockRoot for BeaconBlockRoot: %v", err)
	}

	return parentRoot, nil
}

func (s SqliteBeaconIndexer) TruncateCanonicalChain(slot uint64) error {
	// Execute the query.
	_, err := s.db.Exec(truncateCanonicalChain, slot)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		return fmt.Errorf("failed to truncate canonical chain: %v", err)
	}

	return nil
}
