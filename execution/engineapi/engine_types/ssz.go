// Copyright 2026 The Erigon Authors
// This file is part of Erigon.

package engine_types

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/clonable"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/types"
)

const (
	sszMaxBlobHashes          = 4096
	sszMaxGetBlobHashes       = 128
	sszMaxBytesPerTransaction = 0x40000000
	sszMaxValidationError     = 1024
	sszBlobBytes              = 0x20000
	sszKZGBytes               = 48
	sszCellsPerExtBlob        = 128
	sszMaxCellProofs          = 33554432
)

var mainnetBeaconCfg = &clparams.MainnetBeaconConfig

func NewExecutionPayloadSSZ(version clparams.StateVersion) *ExecutionPayload {
	return &ExecutionPayload{SSZVersion: version}
}

func (p *ExecutionPayload) Static() bool { return false }

func (p *ExecutionPayload) EncodeSSZ(dst []byte) ([]byte, error) {
	block, err := p.ToSSZBlock(p.sszVersion())
	if err != nil {
		return nil, err
	}
	return block.EncodeSSZ(dst)
}

func (p *ExecutionPayload) DecodeSSZ(buf []byte, version int) error {
	p.SSZVersion = clparams.StateVersion(version)
	block := cltypes.NewEth1Block(p.SSZVersion, mainnetBeaconCfg)
	if err := block.DecodeSSZ(buf, version); err != nil {
		return err
	}
	*p = *ExecutionPayloadFromSSZBlock(block, p.SSZVersion)
	return nil
}

func (p *ExecutionPayload) EncodingSizeSSZ() int {
	block, err := p.ToSSZBlock(p.sszVersion())
	if err != nil {
		return cltypes.NewEth1Block(p.sszVersion(), mainnetBeaconCfg).EncodingSizeSSZ()
	}
	return block.EncodingSizeSSZ()
}

func (p *ExecutionPayload) Clone() clonable.Clonable { return NewExecutionPayloadSSZ(p.sszVersion()) }

func (p *ExecutionPayload) sszVersion() clparams.StateVersion {
	if p != nil && p.SSZVersion != 0 {
		return p.SSZVersion
	}
	return clparams.BellatrixVersion
}

func (p *ExecutionPayload) ToSSZBlock(version clparams.StateVersion) (*cltypes.Eth1Block, error) {
	block := cltypes.NewEth1Block(version, mainnetBeaconCfg)
	block.ParentHash = p.ParentHash
	block.FeeRecipient = p.FeeRecipient
	block.StateRoot = p.StateRoot
	block.ReceiptsRoot = p.ReceiptsRoot
	if len(p.LogsBloom) != 0 && len(p.LogsBloom) != len(block.LogsBloom) {
		return nil, fmt.Errorf("invalid logsBloom length %d", len(p.LogsBloom))
	}
	if len(p.LogsBloom) != 0 {
		copy(block.LogsBloom[:], p.LogsBloom)
	}
	block.PrevRandao = p.PrevRandao
	block.BlockNumber = uint64(p.BlockNumber)
	block.GasLimit = uint64(p.GasLimit)
	block.GasUsed = uint64(p.GasUsed)
	block.Time = uint64(p.Timestamp)
	block.Extra = solid.NewExtraData()
	block.Extra.SetBytes(p.ExtraData)
	if p.BaseFeePerGas != nil {
		baseFee := uint256.MustFromBig(p.BaseFeePerGas.ToInt())
		baseFeeBytes := baseFee.Bytes32()
		for i, j := 0, len(baseFeeBytes)-1; i < j; i, j = i+1, j-1 {
			baseFeeBytes[i], baseFeeBytes[j] = baseFeeBytes[j], baseFeeBytes[i]
		}
		copy(block.BaseFeePerGas[:], baseFeeBytes[:])
	}
	block.BlockHash = p.BlockHash
	txs := make([][]byte, len(p.Transactions))
	for i, tx := range p.Transactions {
		txs[i] = tx
	}
	block.Transactions = solid.NewTransactionsSSZFromTransactions(txs)
	if version >= clparams.CapellaVersion {
		block.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(mainnetBeaconCfg.MaxWithdrawalsPerPayload), 44)
		for _, w := range p.Withdrawals {
			block.Withdrawals.Append(&cltypes.Withdrawal{Index: w.Index, Validator: w.Validator, Address: w.Address, Amount: w.Amount})
		}
	}
	if p.BlobGasUsed != nil {
		block.BlobGasUsed = uint64(*p.BlobGasUsed)
	}
	if p.ExcessBlobGas != nil {
		block.ExcessBlobGas = uint64(*p.ExcessBlobGas)
	}
	if version >= clparams.GloasVersion {
		block.BlockAccessList = solid.NewByteListSSZ(sszMaxBytesPerTransaction)
		if p.BlockAccessList != nil {
			if err := block.BlockAccessList.SetBytes(*p.BlockAccessList); err != nil {
				return nil, err
			}
		}
		if p.SlotNumber != nil {
			block.SlotNumber = uint64(*p.SlotNumber)
		}
	}
	return block, nil
}

func ExecutionPayloadFromSSZBlock(block *cltypes.Eth1Block, version clparams.StateVersion) *ExecutionPayload {
	baseFeeBytes := common.Copy(block.BaseFeePerGas[:])
	for i, j := 0, len(baseFeeBytes)-1; i < j; i, j = i+1, j-1 {
		baseFeeBytes[i], baseFeeBytes[j] = baseFeeBytes[j], baseFeeBytes[i]
	}
	baseFee := new(uint256.Int).SetBytes(baseFeeBytes)
	body := block.Body()
	p := &ExecutionPayload{
		ParentHash:    block.ParentHash,
		FeeRecipient:  block.FeeRecipient,
		StateRoot:     block.StateRoot,
		ReceiptsRoot:  block.ReceiptsRoot,
		LogsBloom:     block.LogsBloom[:],
		PrevRandao:    block.PrevRandao,
		BlockNumber:   hexutil.Uint64(block.BlockNumber),
		GasLimit:      hexutil.Uint64(block.GasLimit),
		GasUsed:       hexutil.Uint64(block.GasUsed),
		Timestamp:     hexutil.Uint64(block.Time),
		ExtraData:     block.Extra.Bytes(),
		BaseFeePerGas: (*hexutil.Big)(baseFee.ToBig()),
		BlockHash:     block.BlockHash,
		Withdrawals:   body.Withdrawals,
		SSZVersion:    version,
	}
	for _, tx := range body.Transactions {
		p.Transactions = append(p.Transactions, tx)
	}
	if version >= clparams.DenebVersion {
		bg, ebg := hexutil.Uint64(block.BlobGasUsed), hexutil.Uint64(block.ExcessBlobGas)
		p.BlobGasUsed, p.ExcessBlobGas = &bg, &ebg
	}
	if version >= clparams.GloasVersion {
		bal := hexutil.Bytes{}
		if block.BlockAccessList != nil {
			bal = block.BlockAccessList.Bytes()
		}
		p.BlockAccessList = &bal
		slot := hexutil.Uint64(block.SlotNumber)
		p.SlotNumber = &slot
	}
	return p
}

func newWithdrawalList(ws []*types.Withdrawal) *solid.ListSSZ[*cltypes.Withdrawal] {
	l := solid.NewStaticListSSZ[*cltypes.Withdrawal](int(mainnetBeaconCfg.MaxWithdrawalsPerPayload), 44)
	for _, w := range ws {
		l.Append(&cltypes.Withdrawal{Index: w.Index, Validator: w.Validator, Address: w.Address, Amount: w.Amount})
	}
	return l
}

func withdrawalsFromList(l *solid.ListSSZ[*cltypes.Withdrawal]) []*types.Withdrawal {
	if l == nil {
		return nil
	}
	out := make([]*types.Withdrawal, 0, l.Len())
	l.Range(func(_ int, w *cltypes.Withdrawal, _ int) bool {
		out = append(out, &types.Withdrawal{Index: w.Index, Validator: w.Validator, Address: w.Address, Amount: w.Amount})
		return true
	})
	return out
}

func (s *PayloadStatus) Static() bool { return false }

func (s *PayloadStatus) EncodeSSZ(dst []byte) ([]byte, error) {
	status, err := payloadStatusByte(s.Status)
	if err != nil {
		return nil, err
	}
	var latest []common.Hash
	if s.LatestValidHash != nil {
		latest = []common.Hash{*s.LatestValidHash}
	}
	latestHashes := solid.NewHashList(1)
	for _, hash := range latest {
		latestHashes.Append(hash)
	}
	errBytes := solid.NewByteListSSZ(sszMaxValidationError)
	if s.ValidationError != nil && s.ValidationError.Error() != nil {
		msg := []byte(s.ValidationError.Error().Error())
		if len(msg) > sszMaxValidationError {
			msg = msg[:sszMaxValidationError]
		}
		_ = errBytes.SetBytes(msg)
	}
	return ssz2.MarshalSSZ(dst, []byte{status}, latestHashes, errBytes)
}

func (s *PayloadStatus) DecodeSSZ(buf []byte, version int) error {
	latest := solid.NewHashList(1)
	errBytes := solid.NewByteListSSZ(sszMaxValidationError)
	status := []byte{0}
	if err := ssz2.UnmarshalSSZ(buf, version, status, latest, errBytes); err != nil {
		return err
	}
	engineStatus, err := payloadStatusFromByte(status[0])
	if err != nil {
		return err
	}
	s.Status = engineStatus
	if latest.Length() > 0 {
		hash := latest.Get(0)
		s.LatestValidHash = &hash
	}
	if msg := string(errBytes.Bytes()); msg != "" {
		s.ValidationError = NewStringifiedErrorFromString(msg)
	}
	return nil
}

func (s *PayloadStatus) EncodingSizeSSZ() int { out, _ := s.EncodeSSZ(nil); return len(out) }
func (*PayloadStatus) Clone() clonable.Clonable {
	return &PayloadStatus{}
}

func payloadStatusByte(status EngineStatus) (uint8, error) {
	switch status {
	case ValidStatus:
		return 0, nil
	case InvalidStatus:
		return 1, nil
	case SyncingStatus:
		return 2, nil
	case AcceptedStatus:
		return 3, nil
	default:
		return 0, fmt.Errorf("unknown payload status %q", status)
	}
}

func payloadStatusFromByte(status uint8) (EngineStatus, error) {
	switch status {
	case 0:
		return ValidStatus, nil
	case 1:
		return InvalidStatus, nil
	case 2:
		return SyncingStatus, nil
	case 3:
		return AcceptedStatus, nil
	default:
		return "", fmt.Errorf("unknown payload status %d", status)
	}
}

func (*ForkChoiceState) Static() bool         { return true }
func (*ForkChoiceState) EncodingSizeSSZ() int { return 96 }
func (s *ForkChoiceState) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, s.HeadHash[:], s.SafeBlockHash[:], s.FinalizedBlockHash[:])
}
func (s *ForkChoiceState) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, s.HeadHash[:], s.SafeBlockHash[:], s.FinalizedBlockHash[:])
}
func (*ForkChoiceState) Clone() clonable.Clonable { return &ForkChoiceState{} }

func NewPayloadAttributesSSZ(version clparams.StateVersion) *PayloadAttributes {
	return &PayloadAttributes{SSZVersion: version}
}

func (a *PayloadAttributes) Static() bool { return false }

func (a *PayloadAttributes) EncodeSSZ(dst []byte) ([]byte, error) {
	version := a.payloadAttributesSSZVersion()
	withdrawals := newWithdrawalList(a.Withdrawals)
	var root common.Hash
	if a.ParentBeaconBlockRoot != nil {
		root = *a.ParentBeaconBlockRoot
	}
	var slot uint64
	if a.SlotNumber != nil {
		slot = uint64(*a.SlotNumber)
	}
	var targetGasLimit uint64
	if a.TargetGasLimit != nil {
		targetGasLimit = uint64(*a.TargetGasLimit)
	}
	switch version {
	case clparams.BellatrixVersion:
		return ssz2.MarshalSSZ(dst, uint64(a.Timestamp), a.PrevRandao[:], a.SuggestedFeeRecipient[:])
	case clparams.CapellaVersion:
		return ssz2.MarshalSSZ(dst, uint64(a.Timestamp), a.PrevRandao[:], a.SuggestedFeeRecipient[:], withdrawals)
	case clparams.DenebVersion:
		return ssz2.MarshalSSZ(dst, uint64(a.Timestamp), a.PrevRandao[:], a.SuggestedFeeRecipient[:], withdrawals, root[:])
	default: // GloasVersion+
		return ssz2.MarshalSSZ(dst, uint64(a.Timestamp), a.PrevRandao[:], a.SuggestedFeeRecipient[:], withdrawals, root[:], slot, targetGasLimit)
	}
}

func (a *PayloadAttributes) DecodeSSZ(buf []byte, version int) error {
	a.SSZVersion = clparams.StateVersion(version)
	withdrawals := newWithdrawalList(nil)
	var timestamp uint64
	var root common.Hash
	var slot uint64
	switch a.SSZVersion {
	case clparams.BellatrixVersion:
		if err := ssz2.UnmarshalSSZ(buf, version, &timestamp, a.PrevRandao[:], a.SuggestedFeeRecipient[:]); err != nil {
			return err
		}
	case clparams.CapellaVersion:
		if err := ssz2.UnmarshalSSZ(buf, version, &timestamp, a.PrevRandao[:], a.SuggestedFeeRecipient[:], withdrawals); err != nil {
			return err
		}
		a.Withdrawals = withdrawalsFromList(withdrawals)
	case clparams.DenebVersion:
		if err := ssz2.UnmarshalSSZ(buf, version, &timestamp, a.PrevRandao[:], a.SuggestedFeeRecipient[:], withdrawals, root[:]); err != nil {
			return err
		}
		a.Withdrawals = withdrawalsFromList(withdrawals)
		a.ParentBeaconBlockRoot = &root
	default: // GloasVersion+
		var targetGasLimit uint64
		if err := ssz2.UnmarshalSSZ(buf, version, &timestamp, a.PrevRandao[:], a.SuggestedFeeRecipient[:], withdrawals, root[:], &slot, &targetGasLimit); err != nil {
			return err
		}
		a.Withdrawals = withdrawalsFromList(withdrawals)
		a.ParentBeaconBlockRoot = &root
		slotNumber := hexutil.Uint64(slot)
		a.SlotNumber = &slotNumber
		tgl := hexutil.Uint64(targetGasLimit)
		a.TargetGasLimit = &tgl
	}
	a.Timestamp = hexutil.Uint64(timestamp)
	return nil
}

func (a *PayloadAttributes) EncodingSizeSSZ() int { out, _ := a.EncodeSSZ(nil); return len(out) }
func (a *PayloadAttributes) Clone() clonable.Clonable {
	return NewPayloadAttributesSSZ(a.payloadAttributesSSZVersion())
}
func (a *PayloadAttributes) HashSSZ() ([32]byte, error) { return [32]byte{}, nil }

func (a *PayloadAttributes) payloadAttributesSSZVersion() clparams.StateVersion {
	if a != nil && a.SSZVersion != 0 {
		return a.SSZVersion
	}
	return clparams.BellatrixVersion
}

func (v *ClientVersionV1) Static() bool { return false }

func (v *ClientVersionV1) EncodeSSZ(dst []byte) ([]byte, error) {
	code := solid.NewByteListSSZ(2)
	name := solid.NewByteListSSZ(64)
	version := solid.NewByteListSSZ(64)
	_ = code.SetBytes([]byte(v.Code))
	_ = name.SetBytes([]byte(v.Name))
	_ = version.SetBytes([]byte(v.Version))
	var commit [4]byte
	ch := strings.TrimPrefix(v.Commit, "0x")
	if b, err := hex.DecodeString(ch); err == nil && len(b) >= 4 {
		copy(commit[:], b[:4])
	}
	return ssz2.MarshalSSZ(dst, code, name, version, commit[:])
}

func (v *ClientVersionV1) DecodeSSZ(buf []byte, version int) error {
	code := solid.NewByteListSSZ(2)
	name := solid.NewByteListSSZ(64)
	ver := solid.NewByteListSSZ(64)
	var commit [4]byte
	if err := ssz2.UnmarshalSSZ(buf, version, code, name, ver, commit[:]); err != nil {
		return err
	}
	v.Code = string(code.Bytes())
	v.Name = string(name.Bytes())
	v.Version = string(ver.Bytes())
	v.Commit = "0x" + hex.EncodeToString(commit[:])
	return nil
}

func (v *ClientVersionV1) EncodingSizeSSZ() int { out, _ := v.EncodeSSZ(nil); return len(out) }
func (v *ClientVersionV1) HashSSZ() ([32]byte, error) {
	return [32]byte{}, nil
}
func (*ClientVersionV1) Clone() clonable.Clonable { return &ClientVersionV1{} }

func newKZGCommitment(in []byte) *cltypes.KZGCommitment {
	var out cltypes.KZGCommitment
	copy(out[:], in)
	return &out
}

func newKZGProof(in []byte) *cltypes.KZGProof {
	var out cltypes.KZGProof
	copy(out[:], in)
	return &out
}

func newBlob(in []byte) *cltypes.Blob {
	var out cltypes.Blob
	copy(out[:], in)
	return &out
}

func NewBlobsBundleSSZ(version clparams.StateVersion) *BlobsBundle {
	return &BlobsBundle{SSZVersion: version}
}

func (b *BlobsBundle) Static() bool { return false }

func (b *BlobsBundle) EncodeSSZ(dst []byte) ([]byte, error) {
	proofsLimit := sszMaxBlobHashes
	if b.blobsBundleSSZVersion() >= clparams.FuluVersion {
		proofsLimit = sszMaxCellProofs
	}
	commitments := solid.NewStaticListSSZ[*cltypes.KZGCommitment](sszMaxBlobHashes, sszKZGBytes)
	proofs := solid.NewStaticListSSZ[*cltypes.KZGProof](proofsLimit, sszKZGBytes)
	blobs := solid.NewStaticListSSZ[*cltypes.Blob](sszMaxBlobHashes, sszBlobBytes)
	for _, commitment := range b.Commitments {
		commitments.Append(newKZGCommitment(commitment))
	}
	for _, proof := range b.Proofs {
		proofs.Append(newKZGProof(proof))
	}
	for _, blob := range b.Blobs {
		blobs.Append(newBlob(blob))
	}
	return ssz2.MarshalSSZ(dst, commitments, proofs, blobs)
}

func (b *BlobsBundle) DecodeSSZ(buf []byte, version int) error {
	b.SSZVersion = clparams.StateVersion(version)
	proofsLimit := sszMaxBlobHashes
	if b.SSZVersion >= clparams.FuluVersion {
		proofsLimit = sszMaxCellProofs
	}
	commitments := solid.NewStaticListSSZ[*cltypes.KZGCommitment](sszMaxBlobHashes, sszKZGBytes)
	proofs := solid.NewStaticListSSZ[*cltypes.KZGProof](proofsLimit, sszKZGBytes)
	blobs := solid.NewStaticListSSZ[*cltypes.Blob](sszMaxBlobHashes, sszBlobBytes)
	if err := ssz2.UnmarshalSSZ(buf, version, commitments, proofs, blobs); err != nil {
		return err
	}
	b.Commitments = kzgCommitmentBytes(commitments)
	b.Proofs = kzgProofBytes(proofs)
	b.Blobs = blobBytes(blobs)
	return nil
}

func (b *BlobsBundle) EncodingSizeSSZ() int { out, _ := b.EncodeSSZ(nil); return len(out) }
func (b *BlobsBundle) Clone() clonable.Clonable {
	return NewBlobsBundleSSZ(b.blobsBundleSSZVersion())
}

func (b *BlobsBundle) blobsBundleSSZVersion() clparams.StateVersion {
	if b != nil && b.SSZVersion != 0 {
		return b.SSZVersion
	}
	return clparams.DenebVersion
}

func kzgCommitmentBytes(list *solid.ListSSZ[*cltypes.KZGCommitment]) []hexutil.Bytes {
	out := make([]hexutil.Bytes, 0, list.Len())
	list.Range(func(_ int, v *cltypes.KZGCommitment, _ int) bool {
		value := *v
		out = append(out, common.Copy(value[:]))
		return true
	})
	return out
}

func kzgProofBytes(list *solid.ListSSZ[*cltypes.KZGProof]) []hexutil.Bytes {
	out := make([]hexutil.Bytes, 0, list.Len())
	list.Range(func(_ int, v *cltypes.KZGProof, _ int) bool {
		value := *v
		out = append(out, common.Copy(value[:]))
		return true
	})
	return out
}

func blobBytes(list *solid.ListSSZ[*cltypes.Blob]) []hexutil.Bytes {
	out := make([]hexutil.Bytes, 0, list.Len())
	list.Range(func(_ int, v *cltypes.Blob, _ int) bool {
		value := *v
		out = append(out, common.Copy(value[:]))
		return true
	})
	return out
}

func (*BlobAndProofV1) Static() bool         { return true }
func (*BlobAndProofV1) EncodingSizeSSZ() int { return sszBlobBytes + sszKZGBytes }
func (b *BlobAndProofV1) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, newBlob(b.Blob), newKZGProof(b.Proof))
}
func (b *BlobAndProofV1) DecodeSSZ(buf []byte, version int) error {
	blob := &cltypes.Blob{}
	proof := &cltypes.KZGProof{}
	if err := ssz2.UnmarshalSSZ(buf, version, blob, proof); err != nil {
		return err
	}
	b.Blob = common.Copy(blob[:])
	b.Proof = common.Copy(proof[:])
	return nil
}
func (b *BlobAndProofV1) HashSSZ() ([32]byte, error) { return [32]byte{}, nil }
func (*BlobAndProofV1) Clone() clonable.Clonable     { return &BlobAndProofV1{} }

func (*BlobAndProofV2) Static() bool { return false }
func (b *BlobAndProofV2) EncodeSSZ(dst []byte) ([]byte, error) {
	proofs := solid.NewStaticListSSZ[*cltypes.KZGProof](sszCellsPerExtBlob, sszKZGBytes)
	for _, proof := range b.CellProofs {
		proofs.Append(newKZGProof(proof))
	}
	return ssz2.MarshalSSZ(dst, newBlob(b.Blob), proofs)
}
func (b *BlobAndProofV2) DecodeSSZ(buf []byte, version int) error {
	blob := &cltypes.Blob{}
	proofs := solid.NewStaticListSSZ[*cltypes.KZGProof](sszCellsPerExtBlob, sszKZGBytes)
	if err := ssz2.UnmarshalSSZ(buf, version, blob, proofs); err != nil {
		return err
	}
	b.Blob = common.Copy(blob[:])
	b.CellProofs = kzgProofBytes(proofs)
	return nil
}
func (b *BlobAndProofV2) EncodingSizeSSZ() int       { out, _ := b.EncodeSSZ(nil); return len(out) }
func (b *BlobAndProofV2) HashSSZ() ([32]byte, error) { return [32]byte{}, nil }
func (*BlobAndProofV2) Clone() clonable.Clonable     { return &BlobAndProofV2{} }

type NullableBlobAndProofV2 struct {
	BlobAndProof *BlobAndProofV2
}

func NewNullableBlobAndProofV2(blob *BlobAndProofV2) *NullableBlobAndProofV2 {
	return &NullableBlobAndProofV2{BlobAndProof: blob}
}

func (n *NullableBlobAndProofV2) Static() bool { return false }

func (n *NullableBlobAndProofV2) EncodeSSZ(dst []byte) ([]byte, error) {
	blobAndProof := solid.NewDynamicListSSZ[*BlobAndProofV2](1)
	if n != nil && n.BlobAndProof != nil {
		blobAndProof.Append(n.BlobAndProof)
	}
	return ssz2.MarshalSSZ(dst, blobAndProof)
}

func (n *NullableBlobAndProofV2) DecodeSSZ(buf []byte, version int) error {
	blobAndProof := solid.NewDynamicListSSZ[*BlobAndProofV2](1)
	if err := ssz2.UnmarshalSSZ(buf, version, blobAndProof); err != nil {
		return err
	}
	n.BlobAndProof = nil
	if blobAndProof.Len() != 0 {
		n.BlobAndProof = blobAndProof.Get(0)
	}
	return nil
}

func (n *NullableBlobAndProofV2) EncodingSizeSSZ() int { out, _ := n.EncodeSSZ(nil); return len(out) }
func (n *NullableBlobAndProofV2) HashSSZ() ([32]byte, error) {
	return [32]byte{}, nil
}
func (*NullableBlobAndProofV2) Clone() clonable.Clonable {
	return &NullableBlobAndProofV2{}
}
