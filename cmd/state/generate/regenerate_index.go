package generate

import (
	"errors"
	"os"
	"os/signal"
	"time"

	"github.com/ledgerwatch/turbo-geth/common/changeset"

	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

func RegenerateIndex(chaindata string, csBucket string) error {
	db := ethdb.MustOpen(chaindata)
	ch := make(chan os.Signal, 1)
	quitCh := make(chan struct{})
	signal.Notify(ch, os.Interrupt)
	go func() {
		<-ch
		close(quitCh)
	}()

	lastExecutedBlock, _, err := stages.GetStageProgress(db, stages.Execution)
	if err != nil {
		//There could be headers without block in the end
		log.Error("Cant get last executed block", "err", err)
	}

	ig := core.NewIndexGenerator("regenerate", db, quitCh)
	cs, ok := changeset.Mapper[csBucket]
	if !ok {
		return errors.New("unknown changeset")
	}

	err = ig.DropIndex(cs.IndexBucket)
	if err != nil {
		return err
	}
	startTime := time.Now()
	log.Info("Index generation started", "start time", startTime)
	err = ig.GenerateIndex(0, lastExecutedBlock, csBucket, os.TempDir())
	if err != nil {
		return err
	}
	log.Info("Index is successfully regenerated", "it took", time.Since(startTime))
	return nil
}
