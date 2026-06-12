package state

import "github.com/erigontech/erigon/execution/types/accounts"

type AccessSet map[accounts.Address]struct{}

func (sdb *IntraBlockState) AccessedAddresses() AccessSet { return nil }
