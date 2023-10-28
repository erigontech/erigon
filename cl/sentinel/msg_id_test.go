package sentinel

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/snappy"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/utils"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/stretchr/testify/require"
)

func TestMsgID(t *testing.T) {
	g := clparams.GenesisConfigs[clparams.MainnetNetwork]
	n := clparams.NetworkConfigs[clparams.MainnetNetwork]
	s := &Sentinel{
		ctx: context.TODO(),
		cfg: &SentinelConfig{
			BeaconConfig:  &clparams.MainnetBeaconConfig,
			GenesisConfig: &g,
			NetworkConfig: &n,
		},
	}
	d := [4]byte{108, 122, 33, 65}
	tpc := fmt.Sprintf("/eth2/%x/beacon_block", d)
	topicLen := uint64(len(tpc))
	topicLenBytes := utils.Uint64ToLE(topicLen)
	invalidSnappy := [32]byte{'J', 'U', 'N', 'K'}
	pMsg := &pubsubpb.Message{Data: invalidSnappy[:], Topic: &tpc}
	// Create object to hash
	combinedObj := append(n.MessageDomainInvalidSnappy[:], topicLenBytes...)
	combinedObj = append(combinedObj, tpc...)
	combinedObj = append(combinedObj, pMsg.Data...)
	hashedData := utils.Keccak256(combinedObj)
	msgID := string(hashedData[:20])
	require.Equal(t, msgID, s.msgId(pMsg), "Got incorrect msg id")

	validObj := [32]byte{'v', 'a', 'l', 'i', 'd'}
	enc := snappy.Encode(nil, validObj[:])
	nMsg := &pubsubpb.Message{Data: enc, Topic: &tpc}
	// Create object to hash
	combinedObj = append(n.MessageDomainValidSnappy[:], topicLenBytes...)
	combinedObj = append(combinedObj, tpc...)
	combinedObj = append(combinedObj, validObj[:]...)
	hashedData = utils.Keccak256(combinedObj)
	msgID = string(hashedData[:20])
	require.Equal(t, msgID, s.msgId(nMsg), "Got incorrect msg id")
}
