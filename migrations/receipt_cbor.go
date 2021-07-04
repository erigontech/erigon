package migrations

import (
	"github.com/ledgerwatch/erigon/common/etl"
	"github.com/ledgerwatch/erigon/ethdb"
)

var receiptCbor = Migration{
	Name: "receipt_cbor",
	Up: func(db ethdb.Database, tmpdir string, progress []byte, CommitProgress etl.LoadCommitHandler) (err error) {
		return CommitProgress(db, nil, true)
	},
}
