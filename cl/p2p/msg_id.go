package p2p

import (
	"crypto/sha256"

	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"

	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/utils"
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
func (p *p2pManager) msgId(pmsg *pubsubpb.Message) string {
	topic := *pmsg.Topic
	limit := gossip.MaxUncompressedSize(gossip.ExtractTopicName(topic), p.cfg.BeaconConfig, p.cfg.NetworkConfig)
	domain, payload := p.cfg.NetworkConfig.MessageDomainValidSnappy, pmsg.Data
	if decoded, err := utils.DecompressSnappyWithLimit(pmsg.Data, limit); err != nil {
		domain = p.cfg.NetworkConfig.MessageDomainInvalidSnappy
	} else {
		payload = decoded
	}
	h := sha256.New()
	h.Write(domain[:])
	h.Write(utils.Uint64ToLE(uint64(len(topic))))
	h.Write([]byte(topic))
	h.Write(payload)
	sum := h.Sum(nil)
	return string(sum[:20])
}
