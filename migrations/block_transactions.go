package migrations

import (
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/etl"
	"github.com/ledgerwatch/erigon/ethdb"
)

var splitCanonicalAndNonCanonicalTransactionsBuckets = Migration{
	Name: "split_canonical_and_noncanonical_txs",
	Up: func(db ethdb.Database, tmpdir string, progress []byte, CommitProgress etl.LoadCommitHandler) (err error) {
		collector, err := etl.NewCollectorFromFiles(tmpdir + "canonical")
	})
