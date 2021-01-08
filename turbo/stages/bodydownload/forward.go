package bodydownload

import (
	"time"

	"github.com/ledgerwatch/turbo-geth/ethdb"
)

const (
	logInterval = 30 * time.Second
)

// Forward progresses Bodies stage in the forward direction
func Forward(logPrefix string, db ethdb.Database) error {
	return nil
}
