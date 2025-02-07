// Copyright 2024 The Erigon Authors
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

package cltypes

import (
	"github.com/erigontech/erigon-lib/types/clonable"
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

func (s *SignedBeaconBlock) Clone() clonable.Clonable {
	other := NewSignedBeaconBlock(s.Block.Body.beaconCfg, s.Version())
	other.Block.Body.Version = s.Block.Body.Version
	return other
}

func (i *IndexedAttestation) Clone() clonable.Clonable {
	/*
	   var attestingIndices *solid.RawUint64List

	   	if i.AttestingIndices != nil {
	   		attestingIndices = solid.NewRawUint64List(i.AttestingIndices.Cap(), []uint64{})
	   	}

	*/
	return &IndexedAttestation{
		//AttestingIndices: attestingIndices,
		Data: &solid.AttestationData{},
	}
}

func (b *BeaconBody) Clone() clonable.Clonable {
	other := NewBeaconBody(b.beaconCfg, b.Version)
	return other
}

func (e *Eth1Block) Clone() clonable.Clonable {
	return NewEth1Block(e.version, e.beaconCfg)
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

func (b *BeaconBlock) Clone() clonable.Clonable {
	other := NewBeaconBlock(b.Body.beaconCfg, b.Version())
	other.Body.Version = b.Body.Version
	return other
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

func (*Withdrawal) Clone() clonable.Clonable {
	return &Withdrawal{}
}

func (s *SignedContributionAndProof) Clone() clonable.Clonable {
	return &SignedContributionAndProof{}
}

func (s *ContributionAndProof) Clone() clonable.Clonable {
	return &ContributionAndProof{}
}

func (s *Contribution) Clone() clonable.Clonable {
	return &Contribution{}
}

func (*Root) Clone() clonable.Clonable {
	return &Root{}
}

func (*LightClientUpdatesByRangeRequest) Clone() clonable.Clonable {
	return &LightClientUpdatesByRangeRequest{}
}
