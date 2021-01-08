package bodydownload

import (
	"fmt"
	"time"

	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

const (
	logInterval = 30 * time.Second
)

// Forward progresses Bodies stage in the forward direction
func Forward(logPrefix string, db ethdb.Database) error {
	log.Info(fmt.Sprintf("[%s] Processing bodies...", logPrefix))
	return nil
}
