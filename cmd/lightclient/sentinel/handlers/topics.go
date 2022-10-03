package handlers

const ProtocolPrefix = "/eth2/beacon_chain/req"
const EncodingProtocol = "/ssz_snappy"

// request and response versions
const Schema1 = "/1"
const Schema2 = "/2"

// Request and Response topics
const MetadataTopic = "/metadata"
const PingTopic = "/ping"
const StatusTopic = "/status"
const GoodbyeTopic = "/goodbye"
const BeaconBlockByRangeTopic = "/beacon_block_by_range"
const BeaconBlockByRootTopic = "/beacon_block_by_root"

// Request and Response protocol ids
var (
	PingProtocolV1    = ProtocolPrefix + PingTopic + Schema1 + EncodingProtocol
	GoodbyeProtocolV1 = ProtocolPrefix + GoodbyeTopic + Schema1 + EncodingProtocol

	MedataProtocolV1 = ProtocolPrefix + MetadataTopic + Schema1 + EncodingProtocol
	MedataProtocolV2 = ProtocolPrefix + MetadataTopic + Schema2 + EncodingProtocol

	StatusProtocolV1 = ProtocolPrefix + StatusTopic + Schema1 + EncodingProtocol

	BeaconBlockByRangeProtocolV1 = ProtocolPrefix + BeaconBlockByRangeTopic + Schema1 + EncodingProtocol
	BeaconBlockByRootProtocolV1  = ProtocolPrefix + BeaconBlockByRootTopic + Schema1 + EncodingProtocol
)
