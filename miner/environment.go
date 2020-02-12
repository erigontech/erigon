package miner

import (
	"math/big"
	"sync"

	mapset "github.com/deckarep/golang-set"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
)

// environment is the worker's current environment and holds all of the current state information.
type environment struct {
	signer types.Signer

	state     *state.IntraBlockState // apply state changes here
	tds       *state.TrieDbState
	ancestors mapset.Set    // ancestor set (used for checking uncle parent validity)
	family    mapset.Set    // family set (used for checking uncle invalidity)
	uncles    mapset.Set    // uncle set
	tcount    int           // tx count in cycle
	gasPool   *core.GasPool // available gas used to pack transactions

	*sync.RWMutex
	header   *types.Header
	txs      []*types.Transaction
	receipts []*types.Receipt
	ctx      consensus.Cancel
}

func (e *environment) Number() *big.Int {
	e.RLock()
	defer e.RUnlock()

	return big.NewInt(0).Set(e.header.Number)
}

func (e *environment) Hash() common.Hash {
	e.RLock()
	defer e.RUnlock()

	return e.header.Hash()
}

func (e *environment) ParentHash() common.Hash {
	e.RLock()
	defer e.RUnlock()

	return e.header.ParentHash
}

func (e *environment) SetHeader(h *types.Header) {
	e.Lock()
	defer e.Unlock()

	e.header = h
}

func (e *environment) GetHeader() *types.Header {
	e.RLock()
	defer e.RUnlock()

	return types.CopyHeader(e.header)
}

func (e *environment) Set(env *environment) {
	em := e.RWMutex
	envm := env.RWMutex
	em.Lock()
	envm.Lock()
	defer func() {
		em.Unlock()
		envm.Unlock()
	}()

	*e = *env
	e.RWMutex = envm
}
