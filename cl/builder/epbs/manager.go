package epbs

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/ssz"
)

// BuilderManager manages the builder identity and signing operations for ePBS.
type BuilderManager struct {
	signer                Signer
	builderIndex          *uint64 // nil = not yet resolved; 0 is a valid index
	beaconCfg             *clparams.BeaconChainConfig
	genesisValidatorsRoot common.Hash
}

// NewBuilderManager creates a BuilderManager.
// Pass builderIndex as a pointer; nil means the index has not been resolved yet
// (e.g. deposit not finalized). The balance monitor will re-resolve it later.
func NewBuilderManager(
	signer Signer,
	builderIndex *uint64,
	beaconCfg *clparams.BeaconChainConfig,
	genesisValidatorsRoot common.Hash,
) *BuilderManager {
	return &BuilderManager{
		signer:                signer,
		builderIndex:          builderIndex,
		beaconCfg:             beaconCfg,
		genesisValidatorsRoot: genesisValidatorsRoot,
	}
}

// Pubkey returns the builder's public key.
func (m *BuilderManager) Pubkey() common.Bytes48 {
	return m.signer.Pubkey()
}

// BuilderIndex returns the on-chain builder index.
// Returns (index, true) if resolved, or (0, false) if not yet resolved.
func (m *BuilderManager) BuilderIndex() (uint64, bool) {
	if m.builderIndex == nil {
		return 0, false
	}
	return *m.builderIndex, true
}

// SetBuilderIndex updates the on-chain builder index. Used by the balance
// monitor to re-resolve the index after the initial resolve failed (e.g.
// node was not synced or deposit was not yet finalized at startup).
func (m *BuilderManager) SetBuilderIndex(idx uint64) {
	m.builderIndex = &idx
}

// SignBid stamps the manager's BuilderIndex onto the bid, computes the signing
// root using DomainBeaconBuilder (0x0B000000), and signs it. The BuilderIndex
// is always overwritten to ensure the signed message matches the key that
// produced the signature — on-chain verification looks up the pubkey by the
// BuilderIndex inside the message, not by any external identity.
func (m *BuilderManager) SignBid(ctx context.Context, bid *cltypes.ExecutionPayloadBid) (*cltypes.SignedExecutionPayloadBid, error) {
	idx, ok := m.BuilderIndex()
	if !ok {
		return nil, fmt.Errorf("epbs/manager: builder index not resolved")
	}
	bid.BuilderIndex = idx

	domain, err := m.builderDomain(bid.Slot)
	if err != nil {
		return nil, fmt.Errorf("epbs/manager: compute domain for bid: %w", err)
	}

	signingRoot, err := fork.ComputeSigningRoot(bid, domain)
	if err != nil {
		return nil, fmt.Errorf("epbs/manager: compute signing root for bid: %w", err)
	}

	sig, err := m.signer.SignBid(ctx, signingRoot)
	if err != nil {
		return nil, fmt.Errorf("epbs/manager: sign bid: %w", err)
	}

	return &cltypes.SignedExecutionPayloadBid{
		Message:   bid,
		Signature: sig,
	}, nil
}

// SignEnvelope stamps the manager's BuilderIndex onto the envelope, computes
// the signing root using DomainBeaconBuilder, and signs it. The slot parameter
// is required because the envelope type does not carry a slot field; it
// corresponds to the beacon block slot this envelope belongs to.
func (m *BuilderManager) SignEnvelope(ctx context.Context, envelope *cltypes.ExecutionPayloadEnvelope, slot uint64) (*cltypes.SignedExecutionPayloadEnvelope, error) {
	idx, ok := m.BuilderIndex()
	if !ok {
		return nil, fmt.Errorf("epbs/manager: builder index not resolved")
	}
	envelope.BuilderIndex = idx

	domain, err := m.builderDomain(slot)
	if err != nil {
		return nil, fmt.Errorf("epbs/manager: compute domain for envelope: %w", err)
	}

	signingRoot, err := fork.ComputeSigningRoot(envelope, domain)
	if err != nil {
		return nil, fmt.Errorf("epbs/manager: compute signing root for envelope: %w", err)
	}

	sig, err := m.signer.SignEnvelope(ctx, signingRoot)
	if err != nil {
		return nil, fmt.Errorf("epbs/manager: sign envelope: %w", err)
	}

	return &cltypes.SignedExecutionPayloadEnvelope{
		Message:   envelope,
		Signature: sig,
	}, nil
}

// SignDeposit computes the signing root for a builder deposit (or any
// HashableSSZ without a dedicated signed wrapper) at the given slot using
// DomainBeaconBuilder, and returns the raw signature via Signer.SignDeposit.
func (m *BuilderManager) SignDeposit(ctx context.Context, obj ssz.HashableSSZ, slot uint64) (common.Bytes96, error) {
	domain, err := m.builderDomain(slot)
	if err != nil {
		return common.Bytes96{}, fmt.Errorf("epbs/manager: compute domain for deposit: %w", err)
	}

	signingRoot, err := fork.ComputeSigningRoot(obj, domain)
	if err != nil {
		return common.Bytes96{}, fmt.Errorf("epbs/manager: compute signing root for deposit: %w", err)
	}

	return m.signer.SignDeposit(ctx, signingRoot)
}

// builderDomain computes the BLS domain for DomainBeaconBuilder at the given slot.
// This mirrors the domain computation in state.GetDomain but without requiring
// a full beacon state — we use the config's fork schedule and genesis validators root.
func (m *BuilderManager) builderDomain(slot uint64) ([]byte, error) {
	epoch := slot / m.beaconCfg.SlotsPerEpoch
	stateVersion := m.beaconCfg.GetCurrentStateVersion(epoch)
	forkVersion := utils.Uint32ToBytes4(m.beaconCfg.GetForkVersionByVersion(stateVersion))
	return fork.ComputeDomain(
		m.beaconCfg.DomainBeaconBuilder[:],
		forkVersion,
		m.genesisValidatorsRoot,
	)
}
