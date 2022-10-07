package fork

import (
	"github.com/ledgerwatch/erigon/cmd/lightclient/utils"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
)

// The one suggested by the spec is too over-engineered.
func MsgID(pmsg *pubsubpb.Message) string {
	hash := utils.Keccak256(pmsg.Data)
	return string(hash[:])
}
