package devvalidator

import (
	"encoding/binary"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/ssz"
)

// signing provides BLS signing helpers using the correct domain separation
// as defined by the Ethereum consensus spec.

// forkVersionForEpoch returns the fork version active at the given epoch,
// per the beacon chain config's fork schedule. This mirrors the spec's
// get_domain logic: find the latest fork whose epoch <= the target epoch.
func forkVersionForEpoch(epoch uint64, cfg *clparams.BeaconChainConfig) common.Bytes4 {
	stateVersion := cfg.GetCurrentStateVersion(epoch)
	return utils.Uint32ToBytes4(cfg.GetForkVersionByVersion(stateVersion))
}

// signObject signs an SSZ-hashable object with the given domain type.
func signObject(
	key *ValidatorKey,
	obj ssz.HashableSSZ,
	domainType common.Bytes4,
	epoch uint64,
	cfg *clparams.BeaconChainConfig,
	genesisValidatorsRoot common.Hash,
) (common.Bytes96, error) {
	forkVersion := forkVersionForEpoch(epoch, cfg)
	domain, err := fork.ComputeDomain(domainType[:], forkVersion, genesisValidatorsRoot)
	if err != nil {
		return common.Bytes96{}, err
	}

	signingRoot, err := fork.ComputeSigningRoot(obj, domain)
	if err != nil {
		return common.Bytes96{}, err
	}

	sig := key.PrivKey.Sign(signingRoot[:])
	var result common.Bytes96
	copy(result[:], sig.Bytes())
	return result, nil
}

// epochSSZ is a minimal SSZ type wrapping a uint64 for RANDAO signing.
type epochSSZ uint64

func (e epochSSZ) HashSSZ() ([32]byte, error) {
	var buf [32]byte
	binary.LittleEndian.PutUint64(buf[:8], uint64(e))
	return buf, nil
}

func (e epochSSZ) EncodingSizeSSZ() int { return 8 }

// signRandaoReveal computes the RANDAO reveal for the given epoch:
// BLS_sign(compute_signing_root(epoch, get_domain(DOMAIN_RANDAO, epoch)))
func signRandaoReveal(
	key *ValidatorKey,
	epoch uint64,
	cfg *clparams.BeaconChainConfig,
	genesisValidatorsRoot common.Hash,
) (common.Bytes96, error) {
	return signObject(key, epochSSZ(epoch), cfg.DomainRandao, epoch, cfg, genesisValidatorsRoot)
}

// signBlock signs a beacon block with DOMAIN_BEACON_PROPOSER.
func signBlock(
	key *ValidatorKey,
	block ssz.HashableSSZ,
	slot uint64,
	cfg *clparams.BeaconChainConfig,
	genesisValidatorsRoot common.Hash,
) (common.Bytes96, error) {
	epoch := slot / cfg.SlotsPerEpoch
	return signObject(key, block, cfg.DomainBeaconProposer, epoch, cfg, genesisValidatorsRoot)
}

// signAttestation signs attestation data with DOMAIN_BEACON_ATTESTER.
func signAttestation(
	key *ValidatorKey,
	attData ssz.HashableSSZ,
	slot uint64,
	cfg *clparams.BeaconChainConfig,
	genesisValidatorsRoot common.Hash,
) (common.Bytes96, error) {
	epoch := slot / cfg.SlotsPerEpoch
	return signObject(key, attData, cfg.DomainBeaconAttester, epoch, cfg, genesisValidatorsRoot)
}
