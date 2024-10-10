package fork

import (
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/utils"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
)

// MsgID return the id of Gossip message during subscription.
func MsgID(pmsg *pubsubpb.Message, networkConfig *clparams.NetworkConfig, beaconConfig *clparams.BeaconChainConfig, genesisConfig *clparams.GenesisConfig) string {
	if pmsg == nil || pmsg.Data == nil || pmsg.Topic == nil {
		// Impossible condition that should
		// never be hit.
		msg := make([]byte, 20)
		copy(msg, "invalid")
		return string(msg)
	}

	fEpoch := getLastForkEpoch(beaconConfig, genesisConfig)

	if fEpoch >= beaconConfig.AltairForkEpoch {
		return postAltairMsgID(pmsg, fEpoch, networkConfig, beaconConfig)
	}

	decodedData, err := utils.DecompressSnappy(pmsg.Data)
	if err != nil {
		msg := make([]byte, 20)
		copy(msg, "invalid")
		return string(msg)
	}

	if err != nil {
		combinedData := append(networkConfig.MessageDomainInvalidSnappy[:], pmsg.Data...)
		h := utils.Keccak256(combinedData)
		return string(h[:20])
	}
	combinedData := append(networkConfig.MessageDomainValidSnappy[:], decodedData...)
	h := utils.Keccak256(combinedData)
	return string(h[:20])
}

func postAltairMsgID(pmsg *pubsubpb.Message, fEpoch uint64, networkConfig *clparams.NetworkConfig, beaconConfig *clparams.BeaconChainConfig) string {
	topic := *pmsg.Topic
	topicLen := len(topic)
	topicLenBytes := utils.Uint64ToLE(uint64(topicLen)) // topicLen cannot be negative

	// beyond Bellatrix epoch, allow 10 Mib gossip data size
	gossipPubSubSize := networkConfig.GossipMaxSize
	if fEpoch >= beaconConfig.BellatrixForkEpoch {
		gossipPubSubSize = networkConfig.GossipMaxSizeBellatrix
	}

	decodedData, err := utils.DecompressSnappy(pmsg.Data)
	if err != nil {
		totalLength := len(networkConfig.MessageDomainValidSnappy) + len(topicLenBytes) + topicLen + len(pmsg.Data)
		if uint64(totalLength) > gossipPubSubSize {
			// this should never happen
			msg := make([]byte, 20)
			copy(msg, "invalid")
			return string(msg)
		}
		combinedData := make([]byte, 0, totalLength)
		combinedData = append(combinedData, networkConfig.MessageDomainInvalidSnappy[:]...)
		combinedData = append(combinedData, topicLenBytes...)
		combinedData = append(combinedData, topic...)
		combinedData = append(combinedData, pmsg.Data...)
		h := utils.Keccak256(combinedData)
		return string(h[:20])
	}
	totalLength := len(networkConfig.MessageDomainValidSnappy) +
		len(topicLenBytes) +
		topicLen +
		len(decodedData)

	combinedData := make([]byte, 0, totalLength)
	combinedData = append(combinedData, networkConfig.MessageDomainValidSnappy[:]...)
	combinedData = append(combinedData, topicLenBytes...)
	combinedData = append(combinedData, topic...)
	combinedData = append(combinedData, decodedData...)
	h := utils.Keccak256(combinedData)
	return string(h[:20])
}

func getLastForkEpoch(
	beaconConfig *clparams.BeaconChainConfig,
	genesisConfig *clparams.GenesisConfig,
) uint64 {
	currentEpoch := utils.GetCurrentEpoch(genesisConfig.GenesisTime, beaconConfig.SecondsPerSlot, beaconConfig.SlotsPerEpoch)
	// Retrieve current fork version.
	var currentForkEpoch uint64
	for _, fork := range forkList(beaconConfig.ForkVersionSchedule) {
		if currentEpoch >= fork.epoch {
			currentForkEpoch = fork.epoch
			continue
		}
		break
	}
	return currentForkEpoch
}
