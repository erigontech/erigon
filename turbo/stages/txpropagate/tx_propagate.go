package txpropagate

import (
	mapset "github.com/deckarep/golang-set"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core"
)

// maxTxUnderpricedSetSize is the size of the underpriced transaction set that
// is used to track recent transactions that have been dropped so we don't
// re-request them.
const maxTxUnderpricedSetSize = 32768

type TxPropagate struct {
	txpool   *core.TxPool
	knownTxs map[string]mapset.Set
	requests []TxsRequest

	underpriced        mapset.Set // Transactions discarded as too cheap (don't re-fetch)
	deliveredAnnounces map[string]map[common.Hash]struct{}

	/*
		// Stage 1: Waiting lists for newly discovered transactions that might be
		// broadcast without needing explicit request/reply round trips.
		waitlist  map[common.Hash]map[string]struct{} // Transactions waiting for an potential broadcast
		waittime  map[common.Hash]mclock.AbsTime      // Timestamps when transactions were added to the waitlist
		waitslots map[string]map[common.Hash]struct{} // Waiting announcement sgroupped by peer (DoS protection)

		// Stage 2: Queue of transactions that waiting to be allocated to some peer
		// to be retrieved directly.
		announces map[string]map[common.Hash]struct{} // Set of announced transactions, grouped by origin peer
		announced map[common.Hash]map[string]struct{} // Set of download locations, grouped by transaction hash
	*/
}

func NewTxPropagate(txpool *core.TxPool) *TxPropagate {
	return &TxPropagate{
		txpool:      txpool,
		underpriced: mapset.NewSet(),
		knownTxs:    map[string]mapset.Set{},
		/*
			waitlist:    make(map[common.Hash]map[string]struct{}),
			waittime:    make(map[common.Hash]mclock.AbsTime),
			waitslots:   make(map[string]map[common.Hash]struct{}),
			announces:   make(map[string]map[common.Hash]struct{}),
			announced:   make(map[common.Hash]map[string]struct{}),
		*/
	}
}

type TxsRequest struct {
	Hashes []common.Hash
	PeerID []byte
}
