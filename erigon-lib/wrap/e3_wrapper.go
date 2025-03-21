package wrap

import (
	"github.com/erigontech/erigon-lib/kv"
)

type TxContainer struct {
	Tx  kv.RwTx
	Ttx kv.TemporalTx
}
