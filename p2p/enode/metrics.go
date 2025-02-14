package enode

import "github.com/erigontech/erigon-lib/metrics"

var (
	NodeDbDbSize  = metrics.GetOrCreateGauge(`nodedb_db_size`)  //nolint
	NodeDbTxDirty = metrics.GetOrCreateGauge(`nodedb_tx_dirty`) //nolint
)
