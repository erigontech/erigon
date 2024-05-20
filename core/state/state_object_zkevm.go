package state

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/log/v3"
)

func (so *stateObject) registerKeyReadForWitness(key *libcommon.Hash) {
	_, witnessGeneration := so.db.stateReader.(*TrieDbState)

	if witnessGeneration {
		log.Debug("[WITNESS] we are registering an extra touch for recently created contract", "addr", so.address, "key", key)
		so.db.stateReader.ReadAccountStorage(so.address, so.data.GetIncarnation(), key)
	}
}
