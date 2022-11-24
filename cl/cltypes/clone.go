package cltypes

import "github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/communication"

func (*SignedBeaconBlockBellatrix) Clone() communication.Packet {
	return &SignedBeaconBlockBellatrix{}
}

func (*SignedAggregateAndProof) Clone() communication.Packet {
	return &SignedAggregateAndProof{}
}

func (*SignedVoluntaryExit) Clone() communication.Packet {
	return &SignedVoluntaryExit{}
}

func (*ProposerSlashing) Clone() communication.Packet {
	return &ProposerSlashing{}
}

func (*AttesterSlashing) Clone() communication.Packet {
	return &AttesterSlashing{}
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

func (*SingleRoot) Clone() communication.Packet {
	return &SingleRoot{}
}

func (*LightClientBootstrap) Clone() communication.Packet {
	return &LightClientBootstrap{}
}

func (*LightClientUpdate) Clone() communication.Packet {
	return &LightClientUpdate{}
}
