package apis

import (
	"errors"

	"github.com/ledgerwatch/turbo-geth/ethdb"
)

var ErrEntityNotFound = errors.New("entity not found")

type Env struct {
	DB              ethdb.KV
	Chaindata       string
	RemoteDBAddress string
}
