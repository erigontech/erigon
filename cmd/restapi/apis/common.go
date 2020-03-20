package apis

import (
	"errors"

	"github.com/ledgerwatch/turbo-geth/ethdb/remote"
)

var ErrEntityNotFound = errors.New("entity not found")

type Env struct {
	DB *remote.DB
}
