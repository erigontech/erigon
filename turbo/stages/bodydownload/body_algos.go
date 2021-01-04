package bodydownload

import (
	//"context"
	//"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

// RestoreFromDb reads the state of the database and recreates the state of the body download
func (bd *BodyDownload) RestoreFromDb(db ethdb.Database) error {
	var headerProgress, bodyProgress uint64
	var err error
	headerProgress, err = stages.GetStageProgress(db, stages.Headers)
	if err != nil {
		return err
	}
	bodyProgress, err = stages.GetStageProgress(db, stages.Bodies)
	if err != nil {
		return err
	}
	bd.notDownloaded.AddRange(bodyProgress+1, headerProgress+1)
	return nil
}
