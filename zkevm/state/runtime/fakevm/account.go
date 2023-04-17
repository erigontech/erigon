package fakevm

import "github.com/ledgerwatch/erigon-lib/common"

// Account represents a fake EVM account.
type Account struct {
	address common.Address
}

// NewAccount is the Account constructor.
func NewAccount(address common.Address) *Account {
	return &Account{address: address}
}

// Address is the address getter.
func (a *Account) Address() common.Address { return a.address }
