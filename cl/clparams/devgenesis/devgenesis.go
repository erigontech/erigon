// Package devgenesis builds a valid beacon genesis state for dev mode.
// It creates deterministic validators from a seed string, producing
// a self-contained PoS genesis that can run with an embedded validator.
package devgenesis

import (
	"crypto/ecdsa"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	clutils "github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
)

// DeriveKey derives a deterministic BLS private key from a seed and index.
// Uses KeyGen (IKM expansion) to ensure the result is a valid BLS scalar.
// Not for production — dev/test only.
func DeriveKey(seed string, index uint64) (*bls.PrivateKey, error) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], index)
	// Use sha256 to get 32 bytes of IKM, then KeyGen to reduce modulo curve order.
	ikm := sha256.Sum256(append([]byte(seed), buf[:]...))
	return bls.NewPrivateKeyFromIKM(ikm[:])
}

// DeriveKeys derives N deterministic BLS keypairs from a seed.
func DeriveKeys(seed string, count int) ([]*bls.PrivateKey, error) {
	keys := make([]*bls.PrivateKey, count)
	for i := 0; i < count; i++ {
		var err error
		keys[i], err = DeriveKey(seed, uint64(i))
		if err != nil {
			return nil, fmt.Errorf("derive key %d: %w", i, err)
		}
	}
	return keys, nil
}

// DeriveSignerKey derives a deterministic secp256k1 private key from the seed.
// This is the EL transaction signing key — the corresponding address is pre-funded
// in the dev genesis. Not for production.
func DeriveSignerKey(seed string) (*ecdsa.PrivateKey, common.Address, error) {
	h := sha256.Sum256(append([]byte("signer:"), []byte(seed)...))
	key, err := crypto.ToECDSA(h[:])
	if err != nil {
		return nil, common.Address{}, fmt.Errorf("derive signer key: %w", err)
	}
	addr := crypto.PubkeyToAddress(key.PublicKey)
	return key, addr, nil
}

// DevConfig holds all the pieces needed to start a dev node.
type DevConfig struct {
	BeaconState   *state.CachingBeaconState
	ValidatorKeys []*bls.PrivateKey
	SignerKey     *ecdsa.PrivateKey
	SignerAddress common.Address
	BeaconConfig  *clparams.BeaconChainConfig
}

// BuildGenesisState creates a valid beacon genesis state for dev mode.
//
// Parameters:
//   - seed: deterministic key derivation seed
//   - validatorCount: number of genesis validators
//   - cfg: beacon chain config (use minimal preset for dev)
//   - genesisTime: unix timestamp for genesis
//   - elGenesisHash: execution layer genesis block hash (for Eth1Data)
//
// The returned state is ready to be SSZ-encoded and used by Caplin.
func BuildGenesisState(
	seed string,
	validatorCount int,
	cfg *clparams.BeaconChainConfig,
	genesisTime uint64,
	elGenesisHash common.Hash,
) (*state.CachingBeaconState, []*bls.PrivateKey, error) {
	if validatorCount == 0 {
		return nil, nil, fmt.Errorf("validator count must be > 0")
	}

	// Derive BLS keys.
	keys, err := DeriveKeys(seed, validatorCount)
	if err != nil {
		return nil, nil, fmt.Errorf("derive keys: %w", err)
	}

	// Create empty state with the given config at the correct version.
	beaconState := state.New(cfg)
	beaconState.SetVersion(cfg.GetCurrentStateVersion(0))

	// Set genesis time and slot.
	beaconState.SetGenesisTime(genesisTime)
	beaconState.SetSlot(0)

	// Set fork parameters for genesis. When multiple forks activate at
	// epoch 0 (typical in dev mode), CurrentVersion must reflect the
	// latest active fork, not GenesisForkVersion. This matches the spec's
	// fork transition logic and ensures domain computations are consistent
	// between the beacon state and the dev validator's signing code.
	genesisVersion := clutils.Uint32ToBytes4(uint32(cfg.GenesisForkVersion))
	currentStateVersion := cfg.GetCurrentStateVersion(0)
	currentVersion := clutils.Uint32ToBytes4(cfg.GetForkVersionByVersion(currentStateVersion))
	beaconState.SetFork(&cltypes.Fork{
		PreviousVersion: genesisVersion,
		CurrentVersion:  currentVersion,
		Epoch:           0,
	})

	// Set Eth1Data linking to the EL genesis.
	// DepositCount = validatorCount, and eth1DepositIndex = validatorCount
	// so the spec thinks all deposits have been processed (genesis validators
	// are injected directly, not via deposit transactions).
	beaconState.SetEth1Data(&cltypes.Eth1Data{
		Root:         common.Hash{}, // empty deposit root for dev genesis
		DepositCount: uint64(validatorCount),
		BlockHash:    elGenesisHash,
	})
	beaconState.SetEth1DepositIndex(uint64(validatorCount))

	// Collect pubkeys for sync committee initialization.
	var allPubkeys []common.Bytes48

	// Add validators.
	maxEffectiveBalance := cfg.MaxEffectiveBalance
	for i := 0; i < validatorCount; i++ {
		pubkey := keys[i].PublicKey()
		pubkeyCompressed := bls.CompressPublicKey(pubkey)
		var pubkeyBytes [48]byte
		copy(pubkeyBytes[:], pubkeyCompressed)

		// Withdrawal credentials: 0x01 prefix + 11 zero bytes + 20-byte address.
		// Address derived from pubkey hash for simplicity.
		var withdrawalCreds [32]byte
		withdrawalCreds[0] = 0x01 // ETH1_ADDRESS_WITHDRAWAL_PREFIX
		pubkeyHash := crypto.Keccak256(pubkeyBytes[:])
		copy(withdrawalCreds[12:], pubkeyHash[12:]) // last 20 bytes as address

		validator := solid.NewValidatorFromParameters(
			pubkeyBytes,
			withdrawalCreds,
			maxEffectiveBalance,
			false,          // not slashed
			0,              // activation eligibility epoch
			0,              // activation epoch (active from genesis)
			math.MaxUint64, // exit epoch (far future)
			math.MaxUint64, // withdrawable epoch (far future)
		)

		beaconState.AddValidator(validator, maxEffectiveBalance)
		allPubkeys = append(allPubkeys, common.Bytes48(pubkeyBytes))
	}

	// Initialize sync committees (Altair+). Fill with validator pubkeys
	// cycling through the set to reach SyncCommitteeSize.
	version := cfg.GetCurrentStateVersion(0)
	if version >= clparams.AltairVersion {
		committeeSize := int(cfg.SyncCommitteeSize)
		committeePubkeys := make([]common.Bytes48, committeeSize)
		for i := 0; i < committeeSize; i++ {
			committeePubkeys[i] = allPubkeys[i%len(allPubkeys)]
		}
		// Compute aggregate pubkey (needed for sync committee verification).
		aggPubkeyBytes := make([][]byte, committeeSize)
		for i, pk := range committeePubkeys {
			aggPubkeyBytes[i] = pk[:]
		}
		aggPubkey, err := bls.AggregatePublickKeys(aggPubkeyBytes)
		if err != nil {
			return nil, nil, fmt.Errorf("aggregate sync committee pubkeys: %w", err)
		}
		var aggPubkeyFixed common.Bytes48
		copy(aggPubkeyFixed[:], aggPubkey)

		syncCommittee := solid.NewSyncCommitteeFromParameters(committeePubkeys, aggPubkeyFixed)
		beaconState.SetCurrentSyncCommittee(syncCommittee)
		beaconState.SetNextSyncCommittee(syncCommittee)

		// Initialize epoch participation flags and inactivity scores.
		beaconState.SetPreviousEpochParticipationFlags(make(cltypes.ParticipationFlagsList, validatorCount))
		beaconState.SetCurrentEpochParticipationFlags(make(cltypes.ParticipationFlagsList, validatorCount))
		beaconState.SetInactivityScores(make([]uint64, validatorCount))
	}

	// Compute genesis validators root.
	validatorsRoot, err := beaconState.Validators().HashSSZ()
	if err != nil {
		return nil, nil, fmt.Errorf("hash validators: %w", err)
	}
	beaconState.SetGenesisValidatorsRoot(common.Hash(validatorsRoot))

	// Initialize RANDAO mixes with the genesis validators root.
	for i := uint64(0); i < cfg.EpochsPerHistoricalVector; i++ {
		beaconState.SetRandaoMixAt(int(i), common.Hash(validatorsRoot))
	}

	// Set the latest execution payload header referencing the EL genesis block.
	execHeader := cltypes.NewEth1Header(version)
	execHeader.BlockHash = elGenesisHash
	beaconState.SetLatestExecutionPayloadHeader(execHeader)

	// Set latest block header. The body root for genesis is the hash of
	// an empty BeaconBlockBody at the genesis version.
	genesisBody := cltypes.NewBeaconBody(cfg, version)
	// Ensure the execution payload has all required sub-fields initialized.
	genesisBody.ExecutionPayload.Extra = solid.NewExtraData()
	genesisBody.ExecutionPayload.Transactions = &solid.TransactionsSSZ{}
	if version >= clparams.CapellaVersion {
		genesisBody.ExecutionPayload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(cfg.MaxWithdrawalsPerPayload), 44)
	}
	if version >= clparams.AltairVersion {
		genesisBody.SyncAggregate = cltypes.NewSyncAggregateWithSize(int(cfg.SyncCommitteeSize) / 8)
	}
	bodyRoot, err := genesisBody.HashSSZ()
	if err != nil {
		return nil, nil, fmt.Errorf("hash genesis body: %w", err)
	}
	latestHeader := &cltypes.BeaconBlockHeader{
		BodyRoot: common.Hash(bodyRoot),
	}
	beaconState.SetLatestBlockHeader(latestHeader)

	// Set justification bits (all zero for genesis).
	beaconState.SetJustificationBits(cltypes.JustificationBits{})

	return beaconState, keys, nil
}
