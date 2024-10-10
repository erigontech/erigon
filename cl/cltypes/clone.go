package cltypes

import "github.com/ledgerwatch/erigon/cl/cltypes/clonable"

func (*SignedBeaconBlock) Clone() clonable.Clonable {
	return &SignedBeaconBlock{}
}

func (*PendingAttestation) Clone() clonable.Clonable {
	return &PendingAttestation{}
}

func (*BeaconBody) Clone() clonable.Clonable {
	return &BeaconBody{}
}

func (*Eth1Block) Clone() clonable.Clonable {
	return &Eth1Block{}
}

func (*BeaconBlocksByRootRequest) Clone() clonable.Clonable {
	return &BeaconBlocksByRootRequest{}
}

func (*Eth1Data) Clone() clonable.Clonable {
	return &Eth1Data{}
}

func (*SignedBLSToExecutionChange) Clone() clonable.Clonable {
	return &SignedBLSToExecutionChange{}
}

func (*HistoricalSummary) Clone() clonable.Clonable {
	return &HistoricalSummary{}
}

func (*Validator) Clone() clonable.Clonable {
	return &Validator{}
}

func (*Attestation) Clone() clonable.Clonable {
	return &Attestation{}
}

func (*Checkpoint) Clone() clonable.Clonable {
	return &Checkpoint{}
}

func (*DepositData) Clone() clonable.Clonable {
	return &DepositData{}
}

func (*Status) Clone() clonable.Clonable {
	return &Status{}
}

func (*SignedAggregateAndProof) Clone() clonable.Clonable {
	return &SignedAggregateAndProof{}
}

func (*SyncAggregate) Clone() clonable.Clonable {
	return &SyncAggregate{}
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

func (*BeaconBlock) Clone() clonable.Clonable {
	return &BeaconBlock{}
}

func (*LightClientUpdate) Clone() clonable.Clonable {
	return &LightClientUpdate{}
}
