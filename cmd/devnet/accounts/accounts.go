package accounts

import (
	"crypto/ecdsa"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/crypto"
)

type Account struct {
	Name    string
	Address libcommon.Address
	sigKey  *ecdsa.PrivateKey
}

func init() {
	core.DevnetSignKey = func(address libcommon.Address) *ecdsa.PrivateKey {
		if account, ok := accountsByAddress[address]; ok {
			return account.sigKey
		}

		if address == core.DevnetEtherbase {
			return core.DevnetSignPrivateKey
		}

		return nil
	}
}

var accountsByAddress = map[libcommon.Address]*Account{}
var accountsByName = map[string]*Account{}

func NewAccount(name string) *Account {
	if account, ok := accountsByName[name]; ok {
		return account
	}

	sigKey, _ := crypto.GenerateKey()

	account := &Account{
		Name:    name,
		Address: crypto.PubkeyToAddress(sigKey.PublicKey),
		sigKey:  sigKey,
	}

	accountsByAddress[account.Address] = account
	accountsByName[name] = account

	return account
}
