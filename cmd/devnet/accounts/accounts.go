package accounts

import (
	"crypto/ecdsa"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/crypto"
)

const DevAddress = "0x67b1d87101671b127f5f8714789C7192f7ad340e"

type Account struct {
	Name    string
	Address libcommon.Address
	sigKey  *ecdsa.PrivateKey
}

func init() {
	core.DevnetSignKey = func(addr libcommon.Address) *ecdsa.PrivateKey {
		return SigKey(addr)
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

func (a *Account) SigKey() *ecdsa.PrivateKey {
	return a.sigKey
}

func GetAccount(account string) *Account {
	if account, ok := accountsByName[account]; ok {
		return account
	}

	if account, ok := accountsByAddress[libcommon.HexToAddress(account)]; ok {
		return account
	}

	return nil
}

func SigKey(source interface{}) *ecdsa.PrivateKey {
	switch source := source.(type) {
	case libcommon.Address:
		if account, ok := accountsByAddress[source]; ok {
			return account.sigKey
		}

		if source == core.DevnetEtherbase {
			return core.DevnetSignPrivateKey
		}
	case string:
		if account := GetAccount(source); account != nil {
			return account.sigKey
		}
	}

	return nil
}
