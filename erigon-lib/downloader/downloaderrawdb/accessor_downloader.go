package downloaderrawdb

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
)

func AllSegmentsDownloadComplete(tx kv.Getter) (allSegmentsDownloadComplete bool, err error) {
	snapshotsStageProgress, err := stages.GetStageProgress(tx, stages.Snapshots)
	return snapshotsStageProgress > 0, err
}
func AllSegmentsDownloadCompleteFromDB(db kv.RoDB) (allSegmentsDownloadComplete bool, err error) {
	err = db.View(context.Background(), func(tx kv.Tx) error {
		allSegmentsDownloadComplete, err = AllSegmentsDownloadComplete(tx)
		return err
	})
	return allSegmentsDownloadComplete, err
}
