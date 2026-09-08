package consensus_tests

import (
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/merkle_tree"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/clonable"
)

type depositMessage struct {
	Pubkey                common.Bytes48
	WithdrawalCredentials common.Hash
	Amount                uint64
}

func (*depositMessage) Clone() clonable.Clonable { return &depositMessage{} }
func (*depositMessage) Static() bool             { return true }
func (*depositMessage) EncodingSizeSSZ() int     { return 88 }
func (d *depositMessage) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, d.Pubkey[:], d.WithdrawalCredentials[:], d.Amount)
}
func (d *depositMessage) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, d.Pubkey[:], d.WithdrawalCredentials[:], &d.Amount)
}
func (d *depositMessage) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(d.Pubkey[:], d.WithdrawalCredentials[:], d.Amount)
}

type validatorEth1Block struct {
	Timestamp    uint64
	DepositRoot  common.Hash
	DepositCount uint64
}

func (*validatorEth1Block) Clone() clonable.Clonable { return &validatorEth1Block{} }
func (*validatorEth1Block) Static() bool             { return true }
func (*validatorEth1Block) EncodingSizeSSZ() int     { return 48 }
func (b *validatorEth1Block) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, b.Timestamp, b.DepositRoot[:], b.DepositCount)
}
func (b *validatorEth1Block) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, &b.Timestamp, b.DepositRoot[:], &b.DepositCount)
}
func (b *validatorEth1Block) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(b.Timestamp, b.DepositRoot[:], b.DepositCount)
}

type forkData struct {
	CurrentVersion        [4]byte
	GenesisValidatorsRoot common.Hash
}

func (*forkData) Clone() clonable.Clonable { return &forkData{} }
func (*forkData) Static() bool             { return true }
func (*forkData) EncodingSizeSSZ() int     { return 36 }
func (d *forkData) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, d.CurrentVersion[:], d.GenesisValidatorsRoot[:])
}
func (d *forkData) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, d.CurrentVersion[:], d.GenesisValidatorsRoot[:])
}
func (d *forkData) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(d.CurrentVersion[:], d.GenesisValidatorsRoot[:])
}

type powBlock struct {
	BlockHash       common.Hash
	ParentHash      common.Hash
	TotalDifficulty [32]byte
}

func (*powBlock) Clone() clonable.Clonable { return &powBlock{} }
func (*powBlock) Static() bool             { return true }
func (*powBlock) EncodingSizeSSZ() int     { return 96 }
func (b *powBlock) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, b.BlockHash[:], b.ParentHash[:], b.TotalDifficulty[:])
}
func (b *powBlock) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, b.BlockHash[:], b.ParentHash[:], b.TotalDifficulty[:])
}
func (b *powBlock) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(b.BlockHash[:], b.ParentHash[:], b.TotalDifficulty[:])
}

type signingData struct {
	ObjectRoot common.Hash
	Domain     common.Hash
}

func (*signingData) Clone() clonable.Clonable { return &signingData{} }
func (*signingData) Static() bool             { return true }
func (*signingData) EncodingSizeSSZ() int     { return 64 }
func (d *signingData) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, d.ObjectRoot[:], d.Domain[:])
}
func (d *signingData) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, d.ObjectRoot[:], d.Domain[:])
}
func (d *signingData) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(d.ObjectRoot[:], d.Domain[:])
}

type partialDataColumnGroupID struct {
	BeaconBlockRoot common.Hash
	Slot            uint64
	version         clparams.StateVersion
}

func (g *partialDataColumnGroupID) Clone() clonable.Clonable {
	return &partialDataColumnGroupID{version: g.version}
}
func (*partialDataColumnGroupID) Static() bool { return true }
func (g *partialDataColumnGroupID) SetVersion(version clparams.StateVersion) {
	g.version = version
}
func (g *partialDataColumnGroupID) schema() []any {
	if g.version >= clparams.GloasVersion {
		return []any{g.BeaconBlockRoot[:], &g.Slot}
	}
	return []any{g.BeaconBlockRoot[:]}
}
func (g *partialDataColumnGroupID) EncodingSizeSSZ() int {
	if g.version >= clparams.GloasVersion {
		return 40
	}
	return 32
}
func (g *partialDataColumnGroupID) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, g.schema()...)
}
func (g *partialDataColumnGroupID) DecodeSSZ(buf []byte, version int) error {
	g.version = clparams.StateVersion(version)
	return ssz2.UnmarshalSSZ(buf, version, g.schema()...)
}
func (g *partialDataColumnGroupID) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(g.schema()...)
}
