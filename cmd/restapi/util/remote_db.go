package util

import (
	"context"

	"github.com/ledgerwatch/turbo-geth/ethdb/remote"
)

func ConnectRemoteDB(remoteDbAddress string) (*remote.DB, error) {
	return remote.Open(context.TODO(), remote.DefaultOpts.Addr(remoteDbAddress))
}
