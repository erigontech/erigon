package wrap

import (
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/state"
)

type TxContainer struct {
	Tx   kv.RwTx
	Ttx  kv.TemporalTx
	Doms *state.SharedDomains
}
