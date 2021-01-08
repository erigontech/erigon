package bodydownload

import (
	"fmt"
	"time"

	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

const (
	logInterval = 30 * time.Second
)

// Forward progresses Bodies stage in the forward direction
func Forward(logPrefix string, db ethdb.Database) error {
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
	log.Info(fmt.Sprintf("[%s] Processing bodies...", logPrefix), "from", bodyProgress, "to", headerProgress)
	return nil
}
