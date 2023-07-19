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
	core.DevnetSignKey = SigKey
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

func SigKey(address libcommon.Address) *ecdsa.PrivateKey {
	if account, ok := accountsByAddress[address]; ok {
		return account.sigKey
	}

	if address == core.DevnetEtherbase {
		return core.DevnetSignPrivateKey
	}

	return nil
}
