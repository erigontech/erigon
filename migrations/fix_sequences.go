package migrations

import (
	"context"

	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/ethdb"
)

var oldSequences = map[string]string{
	dbutils.EthTx: "eth_tx",
}

var fixSequences = Migration{
	Name: "fix_sequences",
	Up: func(db ethdb.RwKV, tmpdir string, progress []byte, BeforeCommit Callback) (err error) {
		tx, err := db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()

		for bkt, oldbkt := range oldSequences {
			seq, getErr := tx.GetOne(dbutils.Sequence, []byte(oldbkt))
			if getErr != nil {
				return getErr
			}

			if seq != nil {
				putErr := tx.Put(dbutils.Sequence, []byte(bkt), seq)
				if putErr != nil {
					return putErr
				}
			}
		}

		if err := BeforeCommit(tx, nil, true); err != nil {
			return err
		}
		return tx.Commit()
	},
}
