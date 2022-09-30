package fork

import (
	"encoding/hex"
	"strings"

	"github.com/ledgerwatch/erigon/cmd/lightclient/clparams"
	"github.com/ledgerwatch/erigon/cmd/lightclient/utils"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
)

// Specifies the prefix for any pubsub topic.
const (
	gossipTopicPrefix = "/eth2/"
	digestLength      = 4
)

// MsgID is a content addressable ID function.
//
// Ethereum Beacon Chain spec defines the message ID as:
//    The `message-id` of a gossipsub message MUST be the following 20 byte value computed from the message data:
//    If `message.data` has a valid snappy decompression, set `message-id` to the first 20 bytes of the `SHA256` hash of
//    the concatenation of `MESSAGE_DOMAIN_VALID_SNAPPY` with the snappy decompressed message data,
//    i.e. `SHA256(MESSAGE_DOMAIN_VALID_SNAPPY + snappy_decompress(message.data))[:20]`.
//
//    Otherwise, set `message-id` to the first 20 bytes of the `SHA256` hash of
//    the concatenation of `MESSAGE_DOMAIN_INVALID_SNAPPY` with the raw message data,
//    i.e. `SHA256(MESSAGE_DOMAIN_INVALID_SNAPPY + message.data)[:20]`.
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

// Spec:
// The derivation of the message-id has changed starting with Altair to incorporate the message topic along with the message data.
// These are fields of the Message Protobuf, and interpreted as empty byte strings if missing. The message-id MUST be the following
// 20 byte value computed from the message:
//
// If message.data has a valid snappy decompression, set message-id to the first 20 bytes of the SHA256 hash of the concatenation of
// the following data: MESSAGE_DOMAIN_VALID_SNAPPY, the length of the topic byte string (encoded as little-endian uint64), the topic
// byte string, and the snappy decompressed message data: i.e. SHA256(MESSAGE_DOMAIN_VALID_SNAPPY + uint_to_bytes(uint64(len(message.topic)))
// + message.topic + snappy_decompress(message.data))[:20]. Otherwise, set message-id to the first 20 bytes of the SHA256 hash of the concatenation
// of the following data: MESSAGE_DOMAIN_INVALID_SNAPPY, the length of the topic byte string (encoded as little-endian uint64),
// the topic byte string, and the raw message data: i.e. SHA256(MESSAGE_DOMAIN_INVALID_SNAPPY + uint_to_bytes(uint64(len(message.topic))) + message.topic + message.data)[:20].
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

func extractGossipDigest(topic string) ([4]byte, bool) {
	// Ensure the topic prefix is correct.
	if len(topic) < len(gossipTopicPrefix)+1 || topic[:len(gossipTopicPrefix)] != gossipTopicPrefix {
		return [4]byte{}, true
	}
	start := len(gossipTopicPrefix)
	end := strings.Index(topic[start:], "/")
	if end == -1 { // Ensure a topic suffix exists.
		return [4]byte{}, true
	}
	end += start
	strDigest := topic[start:end]
	digest, err := hex.DecodeString(strDigest)
	if err != nil {
		return [4]byte{}, true
	}
	if len(digest) != digestLength {
		return [4]byte{}, true
	}
	return utils.BytesToBytes4(digest), false
}
