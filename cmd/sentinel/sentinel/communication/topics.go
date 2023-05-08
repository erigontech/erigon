/*
   Copyright 2022 Erigon-Lightclient contributors
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package communication

const MaximumRequestClientUpdates = 128

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
const BeaconBlocksByRangeTopic = "/beacon_blocks_by_range"
const BeaconBlocksByRootTopic = "/beacon_blocks_by_root"
const BlobSidecarByRootTopic = "/blob_sidecars_by_root"
const BlobSidecarByRangeTopic = "/blob_sidecars_by_range"

// Request and Response protocol ids
var (
	PingProtocolV1    = ProtocolPrefix + PingTopic + Schema1 + EncodingProtocol
	GoodbyeProtocolV1 = ProtocolPrefix + GoodbyeTopic + Schema1 + EncodingProtocol

	MetadataProtocolV1 = ProtocolPrefix + MetadataTopic + Schema1 + EncodingProtocol
	MetadataProtocolV2 = ProtocolPrefix + MetadataTopic + Schema2 + EncodingProtocol

	StatusProtocolV1 = ProtocolPrefix + StatusTopic + Schema1 + EncodingProtocol

	BeaconBlocksByRangeProtocolV1 = ProtocolPrefix + BeaconBlocksByRangeTopic + Schema1 + EncodingProtocol
	BeaconBlocksByRangeProtocolV2 = ProtocolPrefix + BeaconBlocksByRangeTopic + Schema2 + EncodingProtocol

	BeaconBlocksByRootProtocolV1 = ProtocolPrefix + BeaconBlocksByRootTopic + Schema1 + EncodingProtocol
	BeaconBlocksByRootProtocolV2 = ProtocolPrefix + BeaconBlocksByRootTopic + Schema2 + EncodingProtocol

	BlobSidecarByRootProtocolV1 = ProtocolPrefix + BlobSidecarByRootTopic + Schema1 + EncodingProtocol

	BlobSidecarByRangeProtocolV1 = ProtocolPrefix + BlobSidecarByRangeTopic + Schema1 + EncodingProtocol
)
