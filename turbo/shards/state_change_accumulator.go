package shards

import (
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon/common"
)

// Accumulator collects state changes in a form that can then be delivered to the RPC daemon
type Accumulator struct {
	changes            []remote.StateChange
	latestChange       *remote.StateChange
	accountChangeIndex map[common.Address]int // For the latest changes, allows finding account change by account's address
	storageChangeIndex map[common.Address]map[common.Hash]int
}

func (a *Accumulator) Reset() {
	a.changes = nil
	a.latestChange = nil
	a.accountChangeIndex = nil
	a.storageChangeIndex = nil
}

// StartChanges begins accumulation of changes for a new block
func (a *Accumulator) StartChange(blockHeight uint64, blockHash common.Hash, unwind bool) {
	a.changes = append(a.changes, remote.StateChange{})
	a.latestChange = &a.changes[len(a.changes)-1]
	a.latestChange.BlockHeight = blockHeight
	a.latestChange.BlockHash = gointerfaces.ConvertHashToH256(blockHash)
	if unwind {
		a.latestChange.Direction = remote.Direction_UNWIND
	} else {
		a.latestChange.Direction = remote.Direction_FORWARD
	}
	a.accountChangeIndex = make(map[common.Address]int)
	a.storageChangeIndex = make(map[common.Address]map[common.Hash]int)
}

// ChangeAction adds modification of account balance or nonce (or both) to the latest change
func (a *Accumulator) ChangeAccount(address common.Address, data []byte) {
	i, ok := a.accountChangeIndex[address]
	if !ok {
		// Account has not been changed in the latest block yet
		i = len(a.latestChange.Changes)
		a.latestChange.Changes = append(a.latestChange.Changes, &remote.AccountChange{})
		a.accountChangeIndex[address] = i
	}
	accountChange := a.latestChange.Changes[i]
	switch accountChange.Action {
	case remote.Action_STORAGE:
		accountChange.Action = remote.Action_UPSERT
	case remote.Action_CODE:
		accountChange.Action = remote.Action_UPSERT_CODE
	case remote.Action_DELETE:
		panic("")
	}
	accountChange.Data = data
}

// DeleteAccount marks account as deleted
func (a *Accumulator) DeleteAccount(address common.Address) {
	i, ok := a.accountChangeIndex[address]
	if !ok {
		// Account has not been changed in the latest block yet
		i = len(a.latestChange.Changes)
		a.latestChange.Changes = append(a.latestChange.Changes, &remote.AccountChange{})
		a.accountChangeIndex[address] = i
	}
	accountChange := a.latestChange.Changes[i]
	if accountChange.Action != remote.Action_STORAGE {
		panic("")
	}
	accountChange.Data = nil
	accountChange.Code = nil
	accountChange.StorageChanges = nil
	accountChange.Action = remote.Action_DELETE
}

// ChangeCode adds code to the latest change
func (a *Accumulator) ChangeCode(address common.Address, incarnation uint64, code []byte) {
	i, ok := a.accountChangeIndex[address]
	if !ok {
		// Account has not been changed in the latest block yet
		i = len(a.latestChange.Changes)
		a.latestChange.Changes = append(a.latestChange.Changes, &remote.AccountChange{})
		a.accountChangeIndex[address] = i
	}
	accountChange := a.latestChange.Changes[i]
	switch accountChange.Action {
	case remote.Action_STORAGE:
		accountChange.Action = remote.Action_CODE
	case remote.Action_UPSERT:
		accountChange.Action = remote.Action_UPSERT_CODE
	case remote.Action_DELETE:
		panic("")
	}
	accountChange.Incarnation = incarnation
	accountChange.Code = code
}

func (a *Accumulator) ChangeStorage(address common.Address, incarnation uint64, location common.Hash, data []byte) {
	i, ok := a.accountChangeIndex[address]
	if !ok {
		// Account has not been changed in the latest block yet
		i = len(a.latestChange.Changes)
		a.latestChange.Changes = append(a.latestChange.Changes, &remote.AccountChange{})
		a.accountChangeIndex[address] = i
	}
	accountChange := a.latestChange.Changes[i]
	if accountChange.Action == remote.Action_DELETE {
		panic("")
	}
	accountChange.Incarnation = incarnation
	si, ok1 := a.storageChangeIndex[address]
	if !ok1 {
		si = make(map[common.Hash]int)
		a.storageChangeIndex[address] = si
	}
	j, ok2 := si[location]
	if !ok2 {
		j = len(accountChange.StorageChanges)
		accountChange.StorageChanges = append(accountChange.StorageChanges, &remote.StorageChange{})
		si[location] = j
	}
	storageChange := accountChange.StorageChanges[j]
	storageChange.Location = gointerfaces.ConvertHashToH256(location)
	storageChange.Data = data
}
