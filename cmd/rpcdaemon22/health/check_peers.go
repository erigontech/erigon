package health

import (
	"context"
	"errors"
	"fmt"
)

var (
	errNotEnoughPeers = errors.New("not enough peers")
)

func checkMinPeers(minPeerCount uint, api NetAPI) error {
	if api == nil {
		return fmt.Errorf("no connection to the Erigon server or `net` namespace isn't enabled")
	}

	peerCount, err := api.PeerCount(context.TODO())
	if err != nil {
		return err
	}

	if uint64(peerCount) < uint64(minPeerCount) {
		return fmt.Errorf("%w: %d (minimum %d)", errNotEnoughPeers, peerCount, minPeerCount)
	}

	return nil
}
