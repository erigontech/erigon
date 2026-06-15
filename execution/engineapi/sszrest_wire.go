// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// SSZ wire codecs for the Engine API v2 REST transport
// (https://github.com/ethereum/execution-apis/pull/793).

package engineapi

import (
	"fmt"
	"math/big"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/clonable"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/types"
)

const (
	sszMaxGetBlobHashes      = 128
	sszMaxBodiesRequest      = 32
	sszMaxRequestBody        = 64 << 20 // MAX_REQUEST_BODY_SIZE = 2**26
	sszBlobBytes             = 0x20000
	sszKZGBytes              = 48
	sszCellsPerExtBlob       = 128
	sszCustodyBitvectorBytes = sszCellsPerExtBlob / 8
	sszMaxBALBytes           = 0x40000000
	sszMaxWithdrawals        = 16
	sszWithdrawalBytes       = 44
)

var engineForkOrder = []string{"paris", "shanghai", "cancun", "prague", "osaka", "amsterdam"}

func engineForkVersion(name string) (clparams.StateVersion, bool) {
	switch name {
	case "paris":
		return clparams.BellatrixVersion, true
	case "shanghai":
		return clparams.CapellaVersion, true
	case "cancun":
		return clparams.DenebVersion, true
	case "prague":
		return clparams.ElectraVersion, true
	case "osaka":
		return clparams.FuluVersion, true
	case "amsterdam":
		return clparams.GloasVersion, true
	default:
		return 0, false
	}
}

func forkScheduled(cfg *chain.Config, name string) bool {
	switch name {
	case "paris":
		return true
	case "shanghai":
		return cfg.ShanghaiTime != nil
	case "cancun":
		return cfg.CancunTime != nil
	case "prague":
		return cfg.PragueTime != nil
	case "osaka":
		return cfg.OsakaTime != nil
	case "amsterdam":
		return cfg.AmsterdamTime != nil
	default:
		return false
	}
}

func supportedForkNames(cfg *chain.Config) []string {
	out := make([]string, 0, len(engineForkOrder))
	for _, name := range engineForkOrder {
		if forkScheduled(cfg, name) {
			out = append(out, name)
		}
	}
	return out
}

func forkNameAtTime(cfg *chain.Config, time uint64) string {
	switch {
	case cfg.IsAmsterdam(time):
		return "amsterdam"
	case cfg.IsOsaka(time):
		return "osaka"
	case cfg.IsPrague(time):
		return "prague"
	case cfg.IsCancun(time):
		return "cancun"
	case cfg.IsShanghai(time):
		return "shanghai"
	default:
		return "paris"
	}
}

func hashListValues(l solid.HashListSSZ) []common.Hash {
	if l == nil {
		return nil
	}
	out := make([]common.Hash, 0, l.Length())
	l.Range(func(_ int, hash common.Hash, _ int) bool {
		out = append(out, hash)
		return true
	})
	return out
}

// BodiesByHashRequest {block_hashes} and BlobsVNRequest {versioned_hashes} are
// single-field containers wrapping a List[Hash32, limit]; they share this codec.
func encodeHashListRequest(hashes []common.Hash, limit int) ([]byte, error) {
	list := solid.NewHashList(limit)
	for _, hash := range hashes {
		list.Append(hash)
	}
	return ssz2.MarshalSSZ(nil, list)
}

func decodeHashListRequest(buf []byte, limit int) ([]common.Hash, error) {
	list := solid.NewHashList(limit)
	if err := ssz2.UnmarshalSSZ(buf, 0, list); err != nil {
		return nil, err
	}
	return hashListValues(list), nil
}

func transactionsBytes(txs *solid.TransactionsSSZ) []hexutil.Bytes {
	if txs == nil {
		return nil
	}
	out := make([]hexutil.Bytes, 0)
	txs.ForEach(func(tx []byte, _ int, _ int) bool {
		out = append(out, tx)
		return true
	})
	return out
}

func newTransactionsSSZ(txs []hexutil.Bytes) *solid.TransactionsSSZ {
	binaryTxs := make([][]byte, len(txs))
	for i, tx := range txs {
		binaryTxs[i] = tx
	}
	return solid.NewTransactionsSSZFromTransactions(binaryTxs)
}

// ExecutionPayloadEnvelope is the request body of POST /{fork}/payloads.
// parent_beacon_block_root exists since Cancun, execution_requests since
// Prague; expected blob versioned hashes are recomputed from the transactions.
func newPayloadEnvelopeSchema(version clparams.StateVersion, payload *engine_types.ExecutionPayload, parentRoot *common.Hash, requests *solid.TransactionsSSZ) []any {
	switch {
	case version < clparams.DenebVersion:
		return []any{payload}
	case version < clparams.ElectraVersion:
		return []any{payload, parentRoot[:]}
	default:
		return []any{payload, parentRoot[:], requests}
	}
}

func decodeNewPayloadEnvelope(buf []byte, version clparams.StateVersion) (*engine_types.ExecutionPayload, common.Hash, []hexutil.Bytes, error) {
	payload := engine_types.NewExecutionPayloadSSZ(version)
	parentRoot := common.Hash{}
	requests := &solid.TransactionsSSZ{}
	if err := ssz2.UnmarshalSSZ(buf, int(version), newPayloadEnvelopeSchema(version, payload, &parentRoot, requests)...); err != nil {
		return nil, common.Hash{}, nil, err
	}
	return payload, parentRoot, transactionsBytes(requests), nil
}

func encodeNewPayloadEnvelope(version clparams.StateVersion, payload *engine_types.ExecutionPayload, parentRoot common.Hash, requests []hexutil.Bytes) ([]byte, error) {
	return ssz2.MarshalSSZ(nil, newPayloadEnvelopeSchema(version, payload, &parentRoot, newTransactionsSSZ(requests))...)
}

// blobVersionedHashesFromTxs recomputes the versioned hashes the legacy API
// received as expectedBlobVersionedHashes. Undecodable transactions yield an
// empty list; newPayload re-decodes them and reports INVALID itself.
func blobVersionedHashesFromTxs(txs []hexutil.Bytes) []common.Hash {
	hashes := []common.Hash{}
	binaryTxs := make([][]byte, len(txs))
	for i, tx := range txs {
		binaryTxs[i] = tx
	}
	transactions, err := types.DecodeTransactions(binaryTxs)
	if err != nil {
		return hashes
	}
	for _, txn := range transactions {
		if txn.Type() == types.BlobTxType {
			hashes = append(hashes, txn.GetBlobHashes()...)
		}
	}
	return hashes
}

// ForkchoiceUpdate is the request body of POST /{fork}/forkchoice.
// custody_columns (Optional[Bitvector[CELLS_PER_EXT_BLOB]]) exists since Amsterdam.
func forkchoiceUpdateSchema(version clparams.StateVersion, state *engine_types.ForkChoiceState, attrs *solid.ListSSZ[*engine_types.PayloadAttributes], custody *solid.ByteListSSZ) []any {
	if version < clparams.GloasVersion {
		return []any{state, attrs}
	}
	return []any{state, attrs, custody}
}

func decodeForkchoiceUpdate(buf []byte, version clparams.StateVersion) (engine_types.ForkChoiceState, *engine_types.PayloadAttributes, []byte, error) {
	state := engine_types.ForkChoiceState{}
	attrsList := solid.NewDynamicListSSZ[*engine_types.PayloadAttributes](1)
	custodyList := solid.NewByteListSSZ(sszCustodyBitvectorBytes)
	if err := ssz2.UnmarshalSSZ(buf, int(version), forkchoiceUpdateSchema(version, &state, attrsList, custodyList)...); err != nil {
		return state, nil, nil, err
	}
	var custody []byte
	if l := len(custodyList.Bytes()); l != 0 {
		if l != sszCustodyBitvectorBytes {
			return state, nil, nil, fmt.Errorf("custody columns bitvector length %d, want %d", l, sszCustodyBitvectorBytes)
		}
		custody = custodyList.Bytes()
	}
	var attrs *engine_types.PayloadAttributes
	if attrsList.Len() != 0 {
		attrs = attrsList.Get(0)
		attrs.SSZVersion = version
	}
	return state, attrs, custody, nil
}

func encodeForkchoiceUpdate(version clparams.StateVersion, state *engine_types.ForkChoiceState, attrs *engine_types.PayloadAttributes, custody []byte) ([]byte, error) {
	attrsList := solid.NewDynamicListSSZ[*engine_types.PayloadAttributes](1)
	if attrs != nil {
		attrs.SSZVersion = version
		attrsList.Append(attrs)
	}
	custodyList := solid.NewByteListSSZ(sszCustodyBitvectorBytes)
	if err := custodyList.SetBytes(custody); err != nil {
		return nil, err
	}
	return ssz2.MarshalSSZ(nil, forkchoiceUpdateSchema(version, state, attrsList, custodyList)...)
}

// ForkchoiceUpdateResponse is {payload_status: PayloadStatus, payload_id: Optional[Bytes8]}.
func encodeForkchoiceResponse(resp *engine_types.ForkChoiceUpdatedResponse) ([]byte, error) {
	payloadID := solid.NewByteListSSZ(8)
	if resp.PayloadId != nil {
		if len(*resp.PayloadId) != 8 {
			return nil, fmt.Errorf("payload ID length %d, want 8", len(*resp.PayloadId))
		}
		if err := payloadID.SetBytes(*resp.PayloadId); err != nil {
			return nil, err
		}
	}
	return ssz2.MarshalSSZ(nil, resp.PayloadStatus, payloadID)
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

func blockValueHash(v *hexutil.Big) common.Hash {
	var out common.Hash
	if v == nil {
		return out
	}
	u := uint256.MustFromBig((*big.Int)(v))
	b := u.Bytes32()
	for i := range b {
		out[i] = b[31-i]
	}
	return out
}

func blockValueFromHash(v common.Hash) *hexutil.Big {
	var be [32]byte
	for i := range v {
		be[i] = v[31-i]
	}
	u := new(uint256.Int).SetBytes(be[:])
	return (*hexutil.Big)(u.ToBig())
}

func newBlobsBundleSSZ(b *engine_types.BlobsBundle, version clparams.StateVersion) *engine_types.BlobsBundle {
	if b == nil {
		return engine_types.NewBlobsBundleSSZ(version)
	}
	b.SSZVersion = version
	return b
}

// BuiltPayload is the response of GET /{fork}/payloads/{payloadId}:
// {payload, block_value, blobs_bundle, execution_requests, should_override_builder},
// with should_override_builder existing since Shanghai, blobs_bundle since Cancun
// and execution_requests since Prague.
func encodeBuiltPayload(resp *engine_types.GetPayloadResponse, version clparams.StateVersion) ([]byte, error) {
	payload := resp.ExecutionPayload
	payload.SSZVersion = version
	blockValue := blockValueHash(resp.BlockValue)
	blobsBundle := newBlobsBundleSSZ(resp.BlobsBundle, version)
	requests := newTransactionsSSZ(resp.ExecutionRequests)
	switch {
	case version < clparams.CapellaVersion:
		return ssz2.MarshalSSZ(nil, payload, blockValue[:])
	case version < clparams.DenebVersion:
		return ssz2.MarshalSSZ(nil, payload, blockValue[:], resp.ShouldOverrideBuilder)
	case version < clparams.ElectraVersion:
		return ssz2.MarshalSSZ(nil, payload, blockValue[:], blobsBundle, resp.ShouldOverrideBuilder)
	default:
		return ssz2.MarshalSSZ(nil, payload, blockValue[:], blobsBundle, requests, resp.ShouldOverrideBuilder)
	}
}

func decodeBuiltPayload(buf []byte, version clparams.StateVersion) (*engine_types.GetPayloadResponse, error) {
	payload := engine_types.NewExecutionPayloadSSZ(version)
	blockValue := common.Hash{}
	blobsBundle := engine_types.NewBlobsBundleSSZ(version)
	requests := &solid.TransactionsSSZ{}
	shouldOverride := false
	var err error
	switch {
	case version < clparams.CapellaVersion:
		err = ssz2.UnmarshalSSZ(buf, int(version), payload, blockValue[:])
	case version < clparams.DenebVersion:
		err = ssz2.UnmarshalSSZ(buf, int(version), payload, blockValue[:], &shouldOverride)
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
		BlockValue:            blockValueFromHash(blockValue),
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

func newWithdrawalListSSZ(withdrawals []*types.Withdrawal) *solid.ListSSZ[*cltypes.Withdrawal] {
	l := solid.NewStaticListSSZ[*cltypes.Withdrawal](sszMaxWithdrawals, sszWithdrawalBytes)
	for _, w := range withdrawals {
		l.Append(&cltypes.Withdrawal{Index: w.Index, Validator: w.Validator, Address: w.Address, Amount: w.Amount})
	}
	return l
}

func withdrawalsFromSSZList(l *solid.ListSSZ[*cltypes.Withdrawal]) []*types.Withdrawal {
	out := make([]*types.Withdrawal, 0, l.Len())
	l.Range(func(_ int, w *cltypes.Withdrawal, _ int) bool {
		out = append(out, &types.Withdrawal{Index: w.Index, Validator: w.Validator, Address: w.Address, Amount: w.Amount})
		return true
	})
	return out
}

// sszPayloadBody is the fork-scoped ExecutionPayloadBody: withdrawals exist
// since Shanghai and block_access_list since Amsterdam.
type sszPayloadBody struct {
	version         clparams.StateVersion
	transactions    *solid.TransactionsSSZ
	withdrawals     *solid.ListSSZ[*cltypes.Withdrawal]
	blockAccessList *solid.ByteListSSZ
}

func newSSZPayloadBody(version clparams.StateVersion) *sszPayloadBody {
	return &sszPayloadBody{
		version:         version,
		transactions:    &solid.TransactionsSSZ{},
		withdrawals:     solid.NewStaticListSSZ[*cltypes.Withdrawal](sszMaxWithdrawals, sszWithdrawalBytes),
		blockAccessList: solid.NewByteListSSZ(sszMaxBALBytes),
	}
}

func (b *sszPayloadBody) schema() []any {
	switch {
	case b.version < clparams.CapellaVersion:
		return []any{b.transactions}
	case b.version < clparams.GloasVersion:
		return []any{b.transactions, b.withdrawals}
	default:
		return []any{b.transactions, b.withdrawals, b.blockAccessList}
	}
}

func (b *sszPayloadBody) Static() bool { return false }

func (b *sszPayloadBody) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, b.schema()...)
}

func (b *sszPayloadBody) DecodeSSZ(buf []byte, version int) error {
	b.version = clparams.StateVersion(version)
	return ssz2.UnmarshalSSZ(buf, version, b.schema()...)
}

func (b *sszPayloadBody) EncodingSizeSSZ() int {
	size := 4 + b.transactions.EncodingSizeSSZ()
	if b.version >= clparams.CapellaVersion {
		size += 4 + b.withdrawals.EncodingSizeSSZ()
	}
	if b.version >= clparams.GloasVersion {
		size += 4 + b.blockAccessList.EncodingSizeSSZ()
	}
	return size
}

func (b *sszPayloadBody) Clone() clonable.Clonable { return newSSZPayloadBody(b.version) }

// sszBodyEntry is BodyEntry {available: boolean, body: ExecutionPayloadBody}.
// When Available is false the body is zero-valued.
type sszBodyEntry struct {
	Available       bool
	Transactions    []hexutil.Bytes
	Withdrawals     []*types.Withdrawal
	BlockAccessList hexutil.Bytes
	version         clparams.StateVersion
}

func (e *sszBodyEntry) Static() bool { return false }

func (e *sszBodyEntry) body() (*sszPayloadBody, error) {
	body := newSSZPayloadBody(e.version)
	body.transactions = newTransactionsSSZ(e.Transactions)
	body.withdrawals = newWithdrawalListSSZ(e.Withdrawals)
	if err := body.blockAccessList.SetBytes(e.BlockAccessList); err != nil {
		return nil, err
	}
	return body, nil
}

func (e *sszBodyEntry) EncodeSSZ(dst []byte) ([]byte, error) {
	body, err := e.body()
	if err != nil {
		return nil, err
	}
	return ssz2.MarshalSSZ(dst, e.Available, body)
}

func (e *sszBodyEntry) DecodeSSZ(buf []byte, version int) error {
	e.version = clparams.StateVersion(version)
	body := newSSZPayloadBody(e.version)
	if err := ssz2.UnmarshalSSZ(buf, version, &e.Available, body); err != nil {
		return err
	}
	e.Transactions = transactionsBytes(body.transactions)
	e.Withdrawals = withdrawalsFromSSZList(body.withdrawals)
	e.BlockAccessList = body.blockAccessList.Bytes()
	return nil
}

func (e *sszBodyEntry) EncodingSizeSSZ() int {
	body, err := e.body()
	if err != nil {
		return 0
	}
	return 1 + 4 + body.EncodingSizeSSZ()
}

func (e *sszBodyEntry) HashSSZ() ([32]byte, error) { return [32]byte{}, nil }

func (*sszBodyEntry) Clone() clonable.Clonable { return &sszBodyEntry{} }

// The response of POST /{fork}/bodies/hash and GET /{fork}/bodies is
// BodiesResponse {entries: List[BodyEntry, MAX_BODIES_REQUEST]}; nil bodies
// become available=false.
func encodeBodiesResponse(bodies []*engine_types.ExecutionPayloadBodyV2, version clparams.StateVersion) ([]byte, error) {
	entries := solid.NewDynamicListSSZ[*sszBodyEntry](sszMaxBodiesRequest)
	for _, body := range bodies {
		entry := &sszBodyEntry{version: version}
		if body != nil {
			entry.Available = true
			entry.Transactions = body.Transactions
			entry.Withdrawals = body.Withdrawals
			entry.BlockAccessList = body.BlockAccessList
		}
		entries.Append(entry)
	}
	return ssz2.MarshalSSZ(nil, entries)
}

func decodeBodiesResponse(buf []byte, version clparams.StateVersion) ([]*sszBodyEntry, error) {
	entries := solid.NewDynamicListSSZ[*sszBodyEntry](sszMaxBodiesRequest)
	if err := ssz2.UnmarshalSSZ(buf, int(version), entries); err != nil {
		return nil, err
	}
	out := make([]*sszBodyEntry, 0, entries.Len())
	entries.Range(func(_ int, e *sszBodyEntry, _ int) bool {
		out = append(out, e)
		return true
	})
	return out, nil
}

// sszBlobV1Entry is BlobEntry {available: boolean, contents: BlobAndProofV1}.
type sszBlobV1Entry struct {
	Available bool
	Blob      hexutil.Bytes
	Proof     hexutil.Bytes
}

func (*sszBlobV1Entry) Static() bool { return true }

func (*sszBlobV1Entry) EncodingSizeSSZ() int { return 1 + sszBlobBytes + sszKZGBytes }

func (e *sszBlobV1Entry) EncodeSSZ(dst []byte) ([]byte, error) {
	blob, proof := []byte(e.Blob), []byte(e.Proof)
	if !e.Available {
		blob, proof = make([]byte, sszBlobBytes), make([]byte, sszKZGBytes)
	}
	if len(blob) != sszBlobBytes || len(proof) != sszKZGBytes {
		return nil, fmt.Errorf("bad blob/proof length %d/%d", len(blob), len(proof))
	}
	return ssz2.MarshalSSZ(dst, e.Available, blob, proof)
}

func (e *sszBlobV1Entry) DecodeSSZ(buf []byte, version int) error {
	e.Blob = make(hexutil.Bytes, sszBlobBytes)
	e.Proof = make(hexutil.Bytes, sszKZGBytes)
	return ssz2.UnmarshalSSZ(buf, version, &e.Available, []byte(e.Blob), []byte(e.Proof))
}

func (e *sszBlobV1Entry) HashSSZ() ([32]byte, error) { return [32]byte{}, nil }

func (*sszBlobV1Entry) Clone() clonable.Clonable { return &sszBlobV1Entry{} }

// sszBlobV2Entry is BlobEntry {available: boolean, contents: BlobAndProofV2},
// shared by /blobs/v2 and /blobs/v3.
type sszBlobV2Entry struct {
	Available bool
	Contents  *engine_types.BlobAndProofV2
}

func (*sszBlobV2Entry) Static() bool { return false }

func (e *sszBlobV2Entry) EncodeSSZ(dst []byte) ([]byte, error) {
	contents := e.Contents
	if contents == nil {
		contents = &engine_types.BlobAndProofV2{}
	}
	return ssz2.MarshalSSZ(dst, e.Available, contents)
}

func (e *sszBlobV2Entry) DecodeSSZ(buf []byte, version int) error {
	e.Contents = &engine_types.BlobAndProofV2{}
	if err := ssz2.UnmarshalSSZ(buf, version, &e.Available, e.Contents); err != nil {
		return err
	}
	if !e.Available {
		e.Contents = nil
	}
	return nil
}

func (e *sszBlobV2Entry) EncodingSizeSSZ() int { out, _ := e.EncodeSSZ(nil); return len(out) }

func (e *sszBlobV2Entry) HashSSZ() ([32]byte, error) { return [32]byte{}, nil }

func (*sszBlobV2Entry) Clone() clonable.Clonable { return &sszBlobV2Entry{} }

// The response of POST /blobs/vN is BlobsVNResponse {entries:
// List[BlobVNEntry, MAX_BLOBS_REQUEST]}; nil contents become available=false
// with zero-valued contents.
func encodeBlobsV1Response(blobs []*engine_types.BlobAndProofV1) ([]byte, error) {
	entries := solid.NewStaticListSSZ[*sszBlobV1Entry](sszMaxGetBlobHashes, 1+sszBlobBytes+sszKZGBytes)
	for _, blob := range blobs {
		entry := &sszBlobV1Entry{}
		if blob != nil {
			entry.Available = true
			entry.Blob = blob.Blob
			entry.Proof = blob.Proof
		}
		entries.Append(entry)
	}
	return ssz2.MarshalSSZ(nil, entries)
}

func decodeBlobsV1Response(buf []byte) ([]*engine_types.BlobAndProofV1, error) {
	entries := solid.NewStaticListSSZ[*sszBlobV1Entry](sszMaxGetBlobHashes, 1+sszBlobBytes+sszKZGBytes)
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

func encodeBlobsV2Response(blobs []*engine_types.BlobAndProofV2) ([]byte, error) {
	entries := solid.NewDynamicListSSZ[*sszBlobV2Entry](sszMaxGetBlobHashes)
	for _, blob := range blobs {
		entries.Append(&sszBlobV2Entry{Available: blob != nil, Contents: blob})
	}
	return ssz2.MarshalSSZ(nil, entries)
}

func decodeBlobsV2Response(buf []byte) ([]*engine_types.BlobAndProofV2, error) {
	entries := solid.NewDynamicListSSZ[*sszBlobV2Entry](sszMaxGetBlobHashes)
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
