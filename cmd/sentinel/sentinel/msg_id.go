package sentinel

import (
	"github.com/ledgerwatch/erigon/cl/utils"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
)

// Spec:BeaconConfig()
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
func (s *Sentinel) msgId(pmsg *pubsubpb.Message) string {
	topic := *pmsg.Topic
	topicLen := len(topic)
	topicLenBytes := utils.Uint64ToLE(uint64(topicLen)) // topicLen cannot be negative

	// beyond Bellatrix epoch, allow 10 Mib gossip data size
	gossipPubSubSize := s.cfg.NetworkConfig.GossipMaxSizeBellatrix

	decodedData, err := utils.DecompressSnappy(pmsg.Data)
	if err != nil || uint64(len(decodedData)) > gossipPubSubSize {
		totalLength :=
			len(s.cfg.NetworkConfig.MessageDomainValidSnappy) +
				len(topicLenBytes) +
				topicLen +
				len(pmsg.Data)
		if uint64(totalLength) > gossipPubSubSize {
			// this should never happen
			msg := make([]byte, 20)
			copy(msg, "invalid")
			return string(msg)
		}
		combinedData := make([]byte, 0, totalLength)
		combinedData = append(combinedData, s.cfg.NetworkConfig.MessageDomainInvalidSnappy[:]...)
		combinedData = append(combinedData, topicLenBytes...)
		combinedData = append(combinedData, topic...)
		combinedData = append(combinedData, pmsg.Data...)
		h := utils.Keccak256(combinedData)
		return string(h[:20])
	}
	totalLength := len(s.cfg.NetworkConfig.MessageDomainValidSnappy) +
		len(topicLenBytes) +
		topicLen +
		len(decodedData)

	combinedData := make([]byte, 0, totalLength)
	combinedData = append(combinedData, s.cfg.NetworkConfig.MessageDomainValidSnappy[:]...)
	combinedData = append(combinedData, topicLenBytes...)
	combinedData = append(combinedData, topic...)
	combinedData = append(combinedData, decodedData...)
	h := utils.Keccak256(combinedData)
	return string(h[:20])
}
