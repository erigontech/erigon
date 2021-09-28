package health

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon/common/hexutil"
)

type NetAPI interface {
	PeerCount(_ context.Context) (hexutil.Uint, error)
}

func checkMinPeers(minPeerCount uint, api NetAPI) error {
	if api == nil {
		return fmt.Errorf("no connection to the Erigon server")
	}

	peerCount, err := api.PeerCount(context.TODO())
	if err != nil {
		return err
	}

	if uint(peerCount) < minPeerCount {
		return fmt.Errorf("not enough peers: %d (minimum %d))", peerCount, minPeerCount)
	}

	return nil
}
