package cltypes

import "github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/communication"

func (*SignedBeaconBlockBellatrix) Clone() communication.Packet {
	return &SignedBeaconBlockBellatrix{}
}

func (*LightClientFinalityUpdate) Clone() communication.Packet {
	return &LightClientFinalityUpdate{}
}

func (*LightClientOptimisticUpdate) Clone() communication.Packet {
	return &LightClientOptimisticUpdate{}
}

func (*MetadataV1) Clone() communication.Packet {
	return &MetadataV1{}
}

func (*MetadataV2) Clone() communication.Packet {
	return &MetadataV2{}
}

func (*Ping) Clone() communication.Packet {
	return &Ping{}
}

func (*Status) Clone() communication.Packet {
	return &Status{}
}
