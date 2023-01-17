package cltypes

import "github.com/ledgerwatch/erigon/cl/cltypes/clonable"

func (*SignedBeaconBlock) Clone() clonable.Clonable {
	return &SignedBeaconBlock{}
}

func (*Attestation) Clone() clonable.Clonable {
	return &Attestation{}
}

func (*Status) Clone() clonable.Clonable {
	return &Status{}
}

func (*SignedAggregateAndProof) Clone() clonable.Clonable {
	return &SignedAggregateAndProof{}
}

func (*SignedVoluntaryExit) Clone() clonable.Clonable {
	return &SignedVoluntaryExit{}
}

func (*ProposerSlashing) Clone() clonable.Clonable {
	return &ProposerSlashing{}
}

func (*AttesterSlashing) Clone() clonable.Clonable {
	return &AttesterSlashing{}
}

func (*LightClientFinalityUpdate) Clone() clonable.Clonable {
	return &LightClientFinalityUpdate{}
}

func (*LightClientOptimisticUpdate) Clone() clonable.Clonable {
	return &LightClientOptimisticUpdate{}
}

func (*Metadata) Clone() clonable.Clonable {
	return &Metadata{}
}

func (*Ping) Clone() clonable.Clonable {
	return &Ping{}
}

func (*Deposit) Clone() clonable.Clonable {
	return &Deposit{}
}

func (*LightClientBootstrap) Clone() clonable.Clonable {
	return &LightClientBootstrap{}
}

func (*LightClientUpdate) Clone() clonable.Clonable {
	return &LightClientUpdate{}
}
