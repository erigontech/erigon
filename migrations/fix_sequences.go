package migrations

import (
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/etl"
	"github.com/ledgerwatch/erigon/ethdb"
)

var oldSequences = map[string]string{
	dbutils.EthTx: "eth_tx",
}

var fixSequences = Migration{
	Name: "fix_sequences",
	Up: func(db ethdb.Database, tmpdir string, progress []byte, CommitProgress etl.LoadCommitHandler) (err error) {
		for bkt, oldbkt := range oldSequences {
			seq, getErr := db.GetOne(dbutils.Sequence, []byte(oldbkt))
			if getErr != nil {
				return getErr
			}

			if seq != nil {
				putErr := db.Put(dbutils.Sequence, []byte(bkt), seq)
				if putErr != nil {
					return putErr
				}
			}
		}

		return CommitProgress(db, nil, true)
	},
}
