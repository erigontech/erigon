// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// SSZ wire codecs for the Engine REST API under /engine/v1.
// See https://github.com/ethereum/execution-apis/blob/22e87b3c689f4fd1696baf40328d3c94c02275dd/src/engine/refactor-ssz.md.

package engineapi

import (
	"fmt"

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
)

const (
	sszMaxBodiesRequest            = 32
	sszMaxPayloadRequestBody       = 64 << 20 // MAX_REQUEST_BODY_SIZE = 2**26
	sszMaxForkchoiceRequestBody    = 1 << 10
	sszPayloadAttributesParisBytes = 60
	sszContainerOffsetBytes        = 4
	sszHashBytes                   = 32
	sszMaxBodiesRequestBody        = sszContainerOffsetBytes + sszMaxBodiesRequest*sszHashBytes
	sszMaxBlobsRequestBody         = sszContainerOffsetBytes + maxGetBlobHashes*sszHashBytes
	sszBlobBytes                   = 0x20000
	sszCellBytes                   = 2048
	sszKZGBytes                    = 48
	sszCellsPerExtBlob             = 128
	sszCellBitvectorBytes          = sszCellsPerExtBlob / 8
	sszMaxBALBytes                 = 0x40000000
)

// These are implemented wire schemas, independent of the chain's fork schedule.
// Each operation applies its own fork activation rules after decoding.
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

// Body requests by hash and blob requests v1-v3 wrap a hash list in a container.
// Blob v4 requests have an additional indices bitvector and use a separate codec.
func decodeHashListRequest(buf []byte, limit int) ([]common.Hash, error) {
	list := solid.NewHashList(limit)
	if err := ssz2.UnmarshalSSZStrict(buf, 0, list); err != nil {
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

// execution_requests list bounds; enforced on decode (TransactionsSSZ.EncodeSSZ ignores limits).
const (
	sszMaxExecutionRequests        = 1 << 8  // MAX_EXECUTION_REQUESTS_PER_PAYLOAD
	sszMaxBytesPerExecutionRequest = 1 << 30 // MAX_BYTES_PER_EXECUTION_REQUEST
)

// ExecutionPayloadEnvelope is the request body of POST /payloads.
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
	requests := solid.NewTransactionsSSZWithLimits(sszMaxExecutionRequests, sszMaxBytesPerExecutionRequest)
	if err := ssz2.UnmarshalSSZStrict(buf, int(version), newPayloadEnvelopeSchema(version, payload, &parentRoot, requests)...); err != nil {
		return nil, common.Hash{}, nil, err
	}
	return payload, parentRoot, transactionsBytes(requests), nil
}

// Paris attributes have a fixed SSZ size. Withdrawals make later versions
// variable-size, so a present optional value needs an element offset.
func newOptionalPayloadAttributes(version clparams.StateVersion) *solid.ListSSZ[*engine_types.PayloadAttributes] {
	if version < clparams.CapellaVersion {
		return solid.NewStaticListSSZ[*engine_types.PayloadAttributes](1, sszPayloadAttributesParisBytes)
	}
	return solid.NewDynamicListSSZ[*engine_types.PayloadAttributes](1)
}

// ForkchoiceUpdate is the request body of POST /forkchoice.
// custody_columns (Optional[Bitvector[CELLS_PER_EXT_BLOB]]) exists since Amsterdam.
func forkchoiceUpdateSchema(version clparams.StateVersion, state *engine_types.ForkChoiceState, attrs *solid.ListSSZ[*engine_types.PayloadAttributes], custody *solid.ByteListSSZ) []any {
	if version < clparams.GloasVersion {
		return []any{state, attrs}
	}
	return []any{state, attrs, custody}
}

func decodeForkchoiceUpdate(buf []byte, version clparams.StateVersion) (engine_types.ForkChoiceState, *engine_types.PayloadAttributes, []byte, error) {
	state := engine_types.ForkChoiceState{}
	attrsList := newOptionalPayloadAttributes(version)
	custodyList := solid.NewByteListSSZ(sszCellBitvectorBytes)
	if err := ssz2.UnmarshalSSZStrict(buf, int(version), forkchoiceUpdateSchema(version, &state, attrsList, custodyList)...); err != nil {
		return state, nil, nil, err
	}
	var custody []byte
	if l := len(custodyList.Bytes()); l != 0 {
		if l != sszCellBitvectorBytes {
			return state, nil, nil, fmt.Errorf("custody columns bitvector length %d, want %d", l, sszCellBitvectorBytes)
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

func encodeBlockValue(v *hexutil.U256) common.Hash {
	var out common.Hash
	if v != nil {
		_, _ = (*uint256.Int)(v).MarshalSSZAppend(out[:0])
	}
	return out
}

func newBlobsBundleSSZ(b *engine_types.BlobsBundle, version clparams.StateVersion) *engine_types.BlobsBundle {
	if b == nil {
		return engine_types.NewBlobsBundleSSZ(version)
	}
	b.SSZVersion = version
	return b
}

// BuiltPayload is the response of GET /payloads/{payloadId}:
// {payload, block_value, blobs_bundle, execution_requests, should_override_builder},
// with blobs_bundle and should_override_builder since Cancun and
// execution_requests since Prague.
func encodeBuiltPayload(resp *engine_types.GetPayloadResponse, version clparams.StateVersion) ([]byte, error) {
	payload := resp.ExecutionPayload
	payload.SSZVersion = version
	blockValue := encodeBlockValue(resp.BlockValue)
	blobsBundle := newBlobsBundleSSZ(resp.BlobsBundle, version)
	requests := newTransactionsSSZ(resp.ExecutionRequests)
	switch {
	case version < clparams.DenebVersion:
		return ssz2.MarshalSSZ(nil, payload, blockValue[:])
	case version < clparams.ElectraVersion:
		return ssz2.MarshalSSZ(nil, payload, blockValue[:], blobsBundle, resp.ShouldOverrideBuilder)
	default:
		return ssz2.MarshalSSZ(nil, payload, blockValue[:], blobsBundle, requests, resp.ShouldOverrideBuilder)
	}
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
		withdrawals:     engine_types.NewWithdrawalList(nil),
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
// Unavailable entries must have a zero-valued body.
type sszBodyEntry struct {
	Available bool
	Body      *sszPayloadBody
}

func (e *sszBodyEntry) Static() bool { return false }

func (e *sszBodyEntry) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, e.Available, e.Body)
}

func (e *sszBodyEntry) DecodeSSZ(buf []byte, version int) error {
	e.Body = newSSZPayloadBody(clparams.StateVersion(version))
	return ssz2.UnmarshalSSZ(buf, version, &e.Available, e.Body)
}

func (e *sszBodyEntry) EncodingSizeSSZ() int {
	return 1 + sszContainerOffsetBytes + e.Body.EncodingSizeSSZ()
}

func (e *sszBodyEntry) HashSSZ() ([32]byte, error) { return [32]byte{}, nil }

func (*sszBodyEntry) Clone() clonable.Clonable { return &sszBodyEntry{} }

// BodiesResponse wraps List[BodyEntry, MAX_BODIES_REQUEST] in a container.
// Missing bodies and Amsterdam bodies without a block access list are encoded
// as unavailable, with zero-valued bodies and their response positions preserved.
func encodeBodiesResponse(bodies []*engine_types.ExecutionPayloadBodyV2, version clparams.StateVersion) ([]byte, error) {
	entries := solid.NewDynamicListSSZ[*sszBodyEntry](sszMaxBodiesRequest)
	for _, body := range bodies {
		entry := &sszBodyEntry{Body: newSSZPayloadBody(version)}
		if body != nil && (version < clparams.GloasVersion || body.BlockAccessList != nil) {
			entry.Available = true
			entry.Body.transactions = newTransactionsSSZ(body.Transactions)
			entry.Body.withdrawals = engine_types.NewWithdrawalList(body.Withdrawals)
			if body.BlockAccessList != nil {
				if err := entry.Body.blockAccessList.SetBytes(*body.BlockAccessList); err != nil {
					return nil, err
				}
			}
		}
		entries.Append(entry)
	}
	return ssz2.MarshalSSZ(nil, entries)
}

// Shared read-only zero buffers for unavailable blob entries; never mutated.
var (
	zeroSSZBlob  = make([]byte, sszBlobBytes)
	zeroSSZProof = make([]byte, sszKZGBytes)
)

// sszBlobV1Entry is BlobV1Entry {available: boolean, contents: BlobAndProofV1}.
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
		blob, proof = zeroSSZBlob, zeroSSZProof
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

// sszBlobV2Entry is BlobV2Entry {available: boolean, contents: BlobAndProofV2},
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

func (e *sszBlobV2Entry) EncodingSizeSSZ() int {
	proofs := 0
	if e.Contents != nil {
		proofs = len(e.Contents.CellProofs)
	}
	return 1 + 4 + sszBlobBytes + 4 + proofs*sszKZGBytes
}

func (e *sszBlobV2Entry) HashSSZ() ([32]byte, error) { return [32]byte{}, nil }

func (*sszBlobV2Entry) Clone() clonable.Clonable { return &sszBlobV2Entry{} }

// BlobsV1Response wraps List[BlobV1Entry, MAX_BLOBS_REQUEST] in a container.
// Missing blobs keep their positions with available=false and zero-valued contents.
func encodeBlobsV1Response(blobs []*engine_types.BlobAndProofV1) ([]byte, error) {
	entries := solid.NewStaticListSSZ[*sszBlobV1Entry](maxGetBlobHashes, 1+sszBlobBytes+sszKZGBytes)
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

func encodeBlobsV2Response(blobs []*engine_types.BlobAndProofV2) ([]byte, error) {
	entries := solid.NewDynamicListSSZ[*sszBlobV2Entry](maxGetBlobHashes)
	for _, blob := range blobs {
		entries.Append(&sszBlobV2Entry{Available: blob != nil, Contents: blob})
	}
	return ssz2.MarshalSSZ(nil, entries)
}

func decodeBlobsV4Request(buf []byte) ([]common.Hash, [sszCellBitvectorBytes]byte, error) {
	hashes := solid.NewHashList(maxGetBlobHashes)
	var cellMask [sszCellBitvectorBytes]byte
	if err := ssz2.UnmarshalSSZStrict(buf, 0, hashes, cellMask[:]); err != nil {
		return nil, cellMask, err
	}
	return hashListValues(hashes), cellMask, nil
}

// Optional fixed-size cells and proofs encode as empty bytes or the value itself,
// so TransactionsSSZ provides the required list encoding.
type sszBlobV4Contents struct {
	cells  *solid.TransactionsSSZ
	proofs *solid.TransactionsSSZ
}

func (*sszBlobV4Contents) Static() bool { return false }

func (c *sszBlobV4Contents) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, c.cells, c.proofs)
}

func (c *sszBlobV4Contents) DecodeSSZ(buf []byte, version int) error {
	c.cells = solid.NewTransactionsSSZWithLimits(sszCellsPerExtBlob, sszCellBytes)
	c.proofs = solid.NewTransactionsSSZWithLimits(sszCellsPerExtBlob, sszKZGBytes)
	return ssz2.UnmarshalSSZStrict(buf, version, c.cells, c.proofs)
}

func (c *sszBlobV4Contents) EncodingSizeSSZ() int {
	return 2*sszContainerOffsetBytes + c.cells.EncodingSizeSSZ() + c.proofs.EncodingSizeSSZ()
}

func (*sszBlobV4Contents) Clone() clonable.Clonable { return &sszBlobV4Contents{} }

type sszBlobV4Entry struct {
	Available bool
	Contents  sszBlobV4Contents
}

func (*sszBlobV4Entry) Static() bool { return false }

func (e *sszBlobV4Entry) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, e.Available, &e.Contents)
}

func (e *sszBlobV4Entry) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZStrict(buf, version, &e.Available, &e.Contents)
}

func (e *sszBlobV4Entry) EncodingSizeSSZ() int {
	return 1 + sszContainerOffsetBytes + e.Contents.EncodingSizeSSZ()
}

func (*sszBlobV4Entry) HashSSZ() ([32]byte, error) { return [32]byte{}, nil }

func (*sszBlobV4Entry) Clone() clonable.Clonable { return &sszBlobV4Entry{} }

func encodeBlobsV4Response(blobs engine_types.BlobsBundleV3, cellMask [sszCellBitvectorBytes]byte) ([]byte, error) {
	if len(blobs) > maxGetBlobHashes {
		return nil, fmt.Errorf("too many blob entries: %d", len(blobs))
	}
	cellIndices := blobCellIndices(cellMask[:])
	entries := solid.NewDynamicListSSZ[*sszBlobV4Entry](maxGetBlobHashes)
	for _, blob := range blobs {
		var cells, proofs [][]byte
		if blob != nil {
			if len(blob.BlobCells) != len(cellIndices) || len(blob.Proofs) != len(cellIndices) {
				return nil, fmt.Errorf("blob cells/proofs length %d/%d, want %d", len(blob.BlobCells), len(blob.Proofs), len(cellIndices))
			}
			// Available SSZ blobs retain all 128 positions; unrequested or missing
			// cell/proof pairs stay empty.
			cells = make([][]byte, sszCellsPerExtBlob)
			proofs = make([][]byte, sszCellsPerExtBlob)
			for selectedIndex, cellIndex := range cellIndices {
				cell, proof := blob.BlobCells[selectedIndex], blob.Proofs[selectedIndex]
				if cell == nil && proof == nil {
					continue
				}
				if cell == nil || proof == nil || len(*cell) != sszCellBytes || len(*proof) != sszKZGBytes {
					return nil, fmt.Errorf("invalid cell or proof at index %d", cellIndex)
				}
				cells[cellIndex] = *cell
				proofs[cellIndex] = *proof
			}
		}
		entries.Append(&sszBlobV4Entry{
			Available: blob != nil,
			Contents: sszBlobV4Contents{
				cells:  solid.NewTransactionsSSZFromTransactions(cells),
				proofs: solid.NewTransactionsSSZFromTransactions(proofs),
			},
		})
	}
	return ssz2.MarshalSSZ(nil, entries)
}
