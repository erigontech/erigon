package apis

import (
	"errors"

	"github.com/ledgerwatch/turbo-geth/ethdb"
)

var ErrEntityNotFound = errors.New("entity not found")

type Env struct {
	KV              ethdb.KV
	DB              ethdb.Getter
	Back            ethdb.Backend
	Chaindata       string
	RemoteDBAddress string
}
