package migrations

import (
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

var cliqueDB = Migration{
	Name: "clique-db",
	Up: func(db ethdb.Database, tmpdir string, progress []byte, CommitProgress etl.LoadCommitHandler) error {
		logPrefix := "db_migration: clique-db"

		const loadStep = "load"
		if err := stagedsync.ResetIH(db); err != nil {
			return err
		}
		if err := CommitProgress(db, []byte(loadStep), false); err != nil {
			return err
		}

		to, err := stages.GetStageProgress(db, stages.Execution)
		if err != nil {
			return err
		}
		hash, err := rawdb.ReadCanonicalHash(db, to)
		if err != nil {
			return err
		}
		syncHeadHeader := rawdb.ReadHeader(db, hash, to)
		if syncHeadHeader == nil {
			if err := CommitProgress(db, nil, true); err != nil {
				return err
			}
			return nil
		}
		expectedRootHash := syncHeadHeader.Root

		if err := stagedsync.RegenerateIntermediateHashes(logPrefix, db, true, nil, tmpdir, expectedRootHash, nil); err != nil {
			return err
		}
		if err := CommitProgress(db, nil, true); err != nil {
			return err
		}

		return nil
	},
}

/*
func (c *Clique) lookupSnapshot(num uint64) bool {
	var ok bool

	prefix := dbutils.EncodeBlockNumber(num)

	n := 0

	if err := c.db.(ethdb.HasKV).KV().View(context.Background(), func(tx ethdb.Tx) error {
		n++
		cur := tx.Cursor(dbutils.CliqueBucket)
		defer cur.Close()

		k, _, err := cur.Seek(prefix)
		if err != nil {
			debugLog("lookupSnapshot-err-1", err, num)
			return err
		}

		ok = bytes.HasPrefix(k, prefix)

		return nil
	}); err != nil {
		debugLog("lookupSnapshot. while getting a snapshot", "block", num, "err", err)
		return false
	}

	debugLog("seek", n)

	return ok
}


*/
