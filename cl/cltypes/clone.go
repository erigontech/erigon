package cltypes

import (
	"github.com/ledgerwatch/erigon-lib/types/clonable"
)

func (*SignedBeaconBlock) Clone() clonable.Clonable {
	return &SignedBeaconBlock{}
}

func (*IndexedAttestation) Clone() clonable.Clonable {
	return &IndexedAttestation{}
}

func (*BeaconBody) Clone() clonable.Clonable {
	return &BeaconBody{}
}

func (*Eth1Block) Clone() clonable.Clonable {
	return &Eth1Block{}
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

func (*Metadata) Clone() clonable.Clonable {
	return &Metadata{}
}

func (*Ping) Clone() clonable.Clonable {
	return &Ping{}
}

func (*Deposit) Clone() clonable.Clonable {
	return &Deposit{}
}

func (*BeaconBlock) Clone() clonable.Clonable {
	return &BeaconBlock{}
}

func (*AggregateAndProof) Clone() clonable.Clonable {
	return &AggregateAndProof{}
}

func (*BeaconBlockHeader) Clone() clonable.Clonable {
	return &BeaconBlockHeader{}
}

func (*BLSToExecutionChange) Clone() clonable.Clonable {
	return &BLSToExecutionChange{}
}

func (*SignedBeaconBlockHeader) Clone() clonable.Clonable {
	return &SignedBeaconBlockHeader{}
}

func (*Fork) Clone() clonable.Clonable {
	return &Fork{}
}

func (*KZGCommitment) Clone() clonable.Clonable {
	return &KZGCommitment{}
}

func (*Eth1Header) Clone() clonable.Clonable {
	return &Eth1Header{}
}
