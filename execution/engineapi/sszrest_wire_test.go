// Copyright 2026 The Erigon Authors
// This file is part of Erigon.

package engineapi

import (
	"fmt"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/types"
)

func encodeHashListRequest(hashes []common.Hash, limit int) ([]byte, error) {
	list := solid.NewHashList(limit)
	for _, hash := range hashes {
		list.Append(hash)
	}
	return ssz2.MarshalSSZ(nil, list)
}

func encodeNewPayloadEnvelope(version clparams.StateVersion, payload *engine_types.ExecutionPayload, parentRoot common.Hash, requests []hexutil.Bytes) ([]byte, error) {
	return ssz2.MarshalSSZ(nil, newPayloadEnvelopeSchema(version, payload, &parentRoot, newTransactionsSSZ(requests))...)
}

func encodeForkchoiceUpdate(version clparams.StateVersion, state *engine_types.ForkChoiceState, attrs *engine_types.PayloadAttributes, custody []byte) ([]byte, error) {
	attrsList := newOptionalPayloadAttributes(version)
	if attrs != nil {
		attrs.SSZVersion = version
		attrsList.Append(attrs)
	}
	custodyList := solid.NewByteListSSZ(sszCellBitvectorBytes)
	if err := custodyList.SetBytes(custody); err != nil {
		return nil, err
	}
	return ssz2.MarshalSSZ(nil, forkchoiceUpdateSchema(version, state, attrsList, custodyList)...)
}

func decodeForkchoiceResponse(buf []byte) (*engine_types.ForkChoiceUpdatedResponse, error) {
	status := &engine_types.PayloadStatus{}
	payloadID := solid.NewByteListSSZ(8)
	if err := ssz2.UnmarshalSSZ(buf, 0, status, payloadID); err != nil {
		return nil, err
	}
	resp := &engine_types.ForkChoiceUpdatedResponse{PayloadStatus: status}
	if l := len(payloadID.Bytes()); l != 0 {
		if l != 8 {
			return nil, fmt.Errorf("payload ID length %d, want 0 or 8", l)
		}
		id := hexutil.Bytes(payloadID.Bytes())
		resp.PayloadId = &id
	}
	return resp, nil
}

func decodeBlockValue(v common.Hash) *hexutil.U256 {
	u := new(uint256.Int)
	_ = u.UnmarshalSSZ(v[:])
	return (*hexutil.U256)(u)
}

func decodeBuiltPayload(buf []byte, version clparams.StateVersion) (*engine_types.GetPayloadResponse, error) {
	payload := engine_types.NewExecutionPayloadSSZ(version)
	blockValue := common.Hash{}
	blobsBundle := engine_types.NewBlobsBundleSSZ(version)
	requests := solid.NewTransactionsSSZWithLimits(sszMaxExecutionRequests, sszMaxBytesPerExecutionRequest)
	shouldOverride := false
	var err error
	switch {
	case version < clparams.DenebVersion:
		err = ssz2.UnmarshalSSZ(buf, int(version), payload, blockValue[:])
	case version < clparams.ElectraVersion:
		err = ssz2.UnmarshalSSZ(buf, int(version), payload, blockValue[:], blobsBundle, &shouldOverride)
	default:
		err = ssz2.UnmarshalSSZ(buf, int(version), payload, blockValue[:], blobsBundle, requests, &shouldOverride)
	}
	if err != nil {
		return nil, err
	}
	resp := &engine_types.GetPayloadResponse{
		ExecutionPayload:      payload,
		BlockValue:            decodeBlockValue(blockValue),
		ShouldOverrideBuilder: shouldOverride,
	}
	if version >= clparams.DenebVersion {
		resp.BlobsBundle = blobsBundle
	}
	if version >= clparams.ElectraVersion {
		resp.ExecutionRequests = transactionsBytes(requests)
	}
	return resp, nil
}

func withdrawalsFromSSZList(l *solid.ListSSZ[*cltypes.Withdrawal]) []*types.Withdrawal {
	out := make([]*types.Withdrawal, 0, l.Len())
	l.Range(func(_ int, w *cltypes.Withdrawal, _ int) bool {
		out = append(out, &types.Withdrawal{Index: hexutil.Uint64(w.Index), Validator: hexutil.Uint64(w.Validator), Address: w.Address, Amount: hexutil.Uint64(w.Amount)})
		return true
	})
	return out
}

type decodedBodyEntry struct {
	Available       bool
	Transactions    []hexutil.Bytes
	Withdrawals     []*types.Withdrawal
	BlockAccessList hexutil.Bytes
}

func decodeBodiesResponse(buf []byte, version clparams.StateVersion) ([]*decodedBodyEntry, error) {
	entries := solid.NewDynamicListSSZ[*sszBodyEntry](sszMaxBodiesRequest)
	if err := ssz2.UnmarshalSSZ(buf, int(version), entries); err != nil {
		return nil, err
	}
	out := make([]*decodedBodyEntry, 0, entries.Len())
	entries.Range(func(_ int, e *sszBodyEntry, _ int) bool {
		out = append(out, &decodedBodyEntry{
			Available:       e.Available,
			Transactions:    transactionsBytes(e.Body.transactions),
			Withdrawals:     withdrawalsFromSSZList(e.Body.withdrawals),
			BlockAccessList: e.Body.blockAccessList.Bytes(),
		})
		return true
	})
	return out, nil
}

func decodeBlobsV1Response(buf []byte) ([]*engine_types.BlobAndProofV1, error) {
	entries := solid.NewStaticListSSZ[*sszBlobV1Entry](maxGetBlobHashes, 1+sszBlobBytes+sszKZGBytes)
	if err := ssz2.UnmarshalSSZ(buf, 0, entries); err != nil {
		return nil, err
	}
	out := make([]*engine_types.BlobAndProofV1, entries.Len())
	entries.Range(func(i int, e *sszBlobV1Entry, _ int) bool {
		if e.Available {
			out[i] = &engine_types.BlobAndProofV1{Blob: e.Blob, Proof: e.Proof}
		}
		return true
	})
	return out, nil
}

func decodeBlobsV2Response(buf []byte) ([]*engine_types.BlobAndProofV2, error) {
	entries := solid.NewDynamicListSSZ[*sszBlobV2Entry](maxGetBlobHashes)
	if err := ssz2.UnmarshalSSZ(buf, 0, entries); err != nil {
		return nil, err
	}
	out := make([]*engine_types.BlobAndProofV2, entries.Len())
	entries.Range(func(i int, e *sszBlobV2Entry, _ int) bool {
		out[i] = e.Contents
		return true
	})
	return out, nil
}
