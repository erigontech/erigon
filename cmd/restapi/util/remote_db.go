package util

import (
	"context"

	"github.com/ledgerwatch/turbo-geth/ethdb/remote"
)

func ConnectRemoteDB(remoteDbAddress string) (*remote.DB, error) {
	opts := remote.DefaultOptions()
	opts.DialAddress = remoteDbAddress
	return remote.Open(context.TODO(), opts)
}
