// Copyright 2022 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package communication

const MaximumRequestClientUpdates = 128

const ProtocolPrefix = "/eth2/beacon_chain/req"
const EncodingProtocol = "/ssz_snappy"

// request and response versions
const Schema1 = "/1"
const Schema2 = "/2"
const Schema3 = "/3"

// Request and Response topics
const MetadataTopic = "/metadata"
const PingTopic = "/ping"
const StatusTopic = "/status"
const GoodbyeTopic = "/goodbye"
const BeaconBlocksByRangeTopic = "/beacon_blocks_by_range"
const BeaconBlocksByRootTopic = "/beacon_blocks_by_root"
const BlobSidecarByRootTopic = "/blob_sidecars_by_root"
const BlobSidecarByRangeTopic = "/blob_sidecars_by_range"
const LightClientOptimisticUpdateTopic = "/light_client_optimistic_update"
const LightClientFinalityUpdateTopic = "/light_client_finality_update"
const LightClientBootstrapTopic = "/light_client_bootstrap"
const LightClientUpdatesByRangeTopic = "/light_client_updates_by_range"

const DataColumnSidecarsByRangeTopic = "/data_column_sidecars_by_range"
const DataColumnSidecarsByRootTopic = "/data_column_sidecars_by_root"

// Request and Response protocol ids
var (
	PingProtocolV1    = ProtocolPrefix + PingTopic + Schema1 + EncodingProtocol
	GoodbyeProtocolV1 = ProtocolPrefix + GoodbyeTopic + Schema1 + EncodingProtocol

	MetadataProtocolV1 = ProtocolPrefix + MetadataTopic + Schema1 + EncodingProtocol
	MetadataProtocolV2 = ProtocolPrefix + MetadataTopic + Schema2 + EncodingProtocol
	MetadataProtocolV3 = ProtocolPrefix + MetadataTopic + Schema3 + EncodingProtocol

	StatusProtocolV1 = ProtocolPrefix + StatusTopic + Schema1 + EncodingProtocol
	StatusProtocolV2 = ProtocolPrefix + StatusTopic + Schema2 + EncodingProtocol

	BeaconBlocksByRangeProtocolV1 = ProtocolPrefix + BeaconBlocksByRangeTopic + Schema1 + EncodingProtocol
	BeaconBlocksByRangeProtocolV2 = ProtocolPrefix + BeaconBlocksByRangeTopic + Schema2 + EncodingProtocol

	BeaconBlocksByRootProtocolV1 = ProtocolPrefix + BeaconBlocksByRootTopic + Schema1 + EncodingProtocol
	BeaconBlocksByRootProtocolV2 = ProtocolPrefix + BeaconBlocksByRootTopic + Schema2 + EncodingProtocol

	BlobSidecarByRootProtocolV1  = ProtocolPrefix + BlobSidecarByRootTopic + Schema1 + EncodingProtocol
	BlobSidecarByRangeProtocolV1 = ProtocolPrefix + BlobSidecarByRangeTopic + Schema1 + EncodingProtocol

	DataColumnSidecarsByRootProtocolV1  = ProtocolPrefix + DataColumnSidecarsByRootTopic + Schema1 + EncodingProtocol
	DataColumnSidecarsByRangeProtocolV1 = ProtocolPrefix + DataColumnSidecarsByRangeTopic + Schema1 + EncodingProtocol

	LightClientOptimisticUpdateProtocolV1 = ProtocolPrefix + LightClientOptimisticUpdateTopic + Schema1 + EncodingProtocol
	LightClientFinalityUpdateProtocolV1   = ProtocolPrefix + LightClientFinalityUpdateTopic + Schema1 + EncodingProtocol
	LightClientBootstrapProtocolV1        = ProtocolPrefix + LightClientBootstrapTopic + Schema1 + EncodingProtocol
	LightClientUpdatesByRangeProtocolV1   = ProtocolPrefix + LightClientUpdatesByRangeTopic + Schema1 + EncodingProtocol
)
