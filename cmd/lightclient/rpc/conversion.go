package rpc

import (
	"github.com/ledgerwatch/erigon/cmd/lightclient/rpc/lightrpc"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/proto/p2p"
)

func ConvertSignedP2PBellatrixBlockToLightrpc(block *p2p.SignedBeaconBlockBellatrix) *lightrpc.SignedBeaconBlockBellatrix {
	return &lightrpc.SignedBeaconBlockBellatrix{
		Signature: block.Signature[:],
		Block:     ConvertP2PBellatrixBlockToLightGrpc(&block.Block),
	}
}

func ConvertP2PBellatrixBlockToLightGrpc(block *p2p.BeaconBlockBellatrix) *lightrpc.BeaconBlockBellatrix {
	return &lightrpc.BeaconBlockBellatrix{
		Slot:          uint64(block.Slot),
		ProposerIndex: uint64(block.ProposerIndex),
		Root:          block.StateRoot[:],
		ParentRoot:    block.ParentRoot[:],
		Body:          ConvertP2PBellatrixBodyToLightGrpc(&block.Body),
	}
}

func ConvertP2PBellatrixBodyToLightGrpc(body *p2p.BeaconBlockBodyBellatrix) *lightrpc.BeaconBodyBellatrix {
	return &lightrpc.BeaconBodyBellatrix{
		RandaoReveal:      body.RandaoReveal[:],
		Eth1Data:          ConvertP2PDepositDataToLightGrpc(&body.Eth1Data),
		Graffiti:          body.Graffiti[:],
		ProposerSlashings: ConvertP2PProposerSlashingsToLightGrpc(body.ProposerSlashings),
		AttesterSlashings: ConvertP2PAttesterSlashingsToLightGrpc(body.AttesterSlashings),
		Attestations:      ConvertP2PAttestationsToLightGrpc(body.Attestations),
		Deposits:          ConvertP2PDepositsToLightGrpc(body.Deposits),
		VoluntaryExits:    ConvertP2PExitsToLightGrpc(body.VoluntaryExits),
		SyncAggregate:     ConvertP2PAggregateToLightGrpc(&body.SyncAggregate),
		ExecutionPayload:  ConvertP2PPayloadToLightGrpc(&body.ExecutionPayload),
	}
}

func ConvertP2PDepositDataToLightGrpc(data *p2p.Eth1Data) *lightrpc.Eth1Data {
	return &lightrpc.Eth1Data{
		Root:         data.DepositRoot[:],
		DepositCount: data.DepositCount,
		BlockHash:    data.BlockHash[:],
	}
}

func ConvertP2PProposerSlashingsToLightGrpc(slashings []*p2p.ProposerSlashing) (ret []*lightrpc.Slashing) {
	for _, slashing := range slashings {
		ret = append(ret, &lightrpc.Slashing{
			Header1: ConvertP2PHeaderToLightGrpc(&slashing.Header1),
			Header2: ConvertP2PHeaderToLightGrpc(&slashing.Header2),
		})
	}
	return
}

func ConvertP2PAttesterSlashingsToLightGrpc(slashings []*p2p.AttesterSlashing) (ret []*lightrpc.Slashing) {
	for _, slashing := range slashings {
		ret = append(ret, &lightrpc.Slashing{
			Header1: ConvertP2PHeaderToLightGrpc(&slashing.Header1),
			Header2: ConvertP2PHeaderToLightGrpc(&slashing.Header2),
		})
	}
	return
}

func ConvertP2PAttestationsToLightGrpc(attestations []*p2p.Attestation) (ret []*lightrpc.Attestation) {
	for _, attestation := range attestations {
		ret = append(ret, &lightrpc.Attestation{
			AggregationBits: attestation.AggregationBits,
			Data: &lightrpc.AttestationData{
				Slot:            attestation.Data.Slot,
				Index:           attestation.Data.Index,
				BeaconBlockHash: attestation.Data.BeaconBlockHash[:],
			},
			Signature: attestation.Signature[:],
		})
	}
	return
}

func ConvertP2PExitsToLightGrpc(exits []*p2p.SignedVoluntaryExit) (ret []*lightrpc.SignedVoluntaryExit) {
	for _, exit := range exits {
		ret = append(ret, &lightrpc.SignedVoluntaryExit{
			Signature: exit.Signature[:],
			VolunaryExit: &lightrpc.VoluntaryExit{
				Epoch:          exit.Exit.Epoch,
				ValidatorIndex: exit.Exit.ValidatorIndex,
			},
		})
	}
	return
}

func ConvertP2PPayloadToLightGrpc(p *p2p.ExecutionPayload) *lightrpc.ExecutionPayload {
	return &lightrpc.ExecutionPayload{
		ParentHash:    p.ParentHash[:],
		FeeRecipient:  p.FeeRecipient[:],
		StateRoot:     p.StateRoot[:],
		ReceiptsRoot:  p.ReceiptsRoot[:],
		LogsBloom:     p.LogsBloom[:],
		PrevRandao:    p.PrevRandao[:],
		BlockNumber:   p.BlockNumber,
		GasLimit:      p.GasLimit,
		GasUsed:       p.GasUsed,
		Timestamp:     p.Timestamp,
		ExtraData:     p.ExtraData,
		BaseFeePerGas: p.BaseFeePerGas[:],
		BlockHash:     p.BlockHash[:],
		Transactions:  p.Transactions,
	}
}

func ConvertP2PHeaderToLightGrpc(header *p2p.SignedBeaconBlockHeader) *lightrpc.SignedBeaconBlockHeader {
	return &lightrpc.SignedBeaconBlockHeader{
		Signature: header.Signature[:],
		Header: &lightrpc.BeaconBlockHeader{
			Slot:          uint64(header.Header.Slot),
			ProposerIndex: header.Header.ProposerIndex,
			ParentRoot:    header.Header.ParentRoot[:],
			Root:          header.Header.StateRoot[:],
			BodyRoot:      header.Header.BodyRoot[:],
		},
	}
}

func ConvertP2PAggregateToLightGrpc(agg *p2p.SyncAggregate) *lightrpc.SyncAggregate {
	return &lightrpc.SyncAggregate{
		SyncCommiteeBits:      agg.SyncCommiteeBits[:],
		SyncCommiteeSignature: agg.SyncCommiteeSignature[:],
	}
}

func ConvertP2PDepositsToLightGrpc(deposits []*p2p.Deposit) (ret []*lightrpc.Deposit) {
	for _, deposit := range deposits {
		var proof [][]byte

		for _, c := range deposit.Proof {
			proof = append(proof, c[:])
		}
		ret = append(ret, &lightrpc.Deposit{
			Proof: proof,
			Data: &lightrpc.DepositData{
				PubKey:                deposit.Data.Pubkey[:],
				WithdrawalCredentials: deposit.Data.WithdrawalCredentials[:],
				Amount:                deposit.Data.Amount,
				Signature:             deposit.Data.Signature[:],
				Root:                  deposit.Data.Root[:],
			},
		})
	}
	return
}
