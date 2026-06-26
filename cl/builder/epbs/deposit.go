package epbs

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
)

// BuildWithdrawalCredentials constructs withdrawal credentials for a builder deposit.
// Format: PAYLOAD_BUILDER_VERSION (0x00) + 11 zero bytes + 20-byte execution address.
func BuildWithdrawalCredentials(feeRecipient common.Address) common.Hash {
	var creds common.Hash
	creds[0] = clparams.PayloadBuilderVersion
	copy(creds[12:], feeRecipient[:])
	return creds
}

// BuildDepositData constructs a signed DepositData for builder registration.
//
// The signature uses DomainBuilderDeposit (0x0E000000) with the genesis fork
// version and a zero genesis validators root, matching the spec's
// DOMAIN_BUILDER_DEPOSIT for post-fork builder deposits.
//
// The resulting DepositData can be submitted as a deposit request on the
// execution layer to register the builder in the beacon state.
func BuildDepositData(
	ctx context.Context,
	signer Signer,
	feeRecipient common.Address,
	amount uint64,
	beaconCfg *clparams.BeaconChainConfig,
) (*cltypes.DepositData, error) {
	pubkey := signer.Pubkey()
	creds := BuildWithdrawalCredentials(feeRecipient)

	// Build unsigned deposit data to compute the message hash.
	dd := &cltypes.DepositData{
		PubKey:                pubkey,
		WithdrawalCredentials: creds,
		Amount:                amount,
	}

	// Compute deposit domain: DomainBuilderDeposit + genesis fork version + zero genesis validators root.
	domain, err := fork.ComputeDomain(
		beaconCfg.DomainBuilderDeposit[:],
		utils.Uint32ToBytes4(uint32(beaconCfg.GenesisForkVersion)),
		[32]byte{},
	)
	if err != nil {
		return nil, fmt.Errorf("epbs/deposit: compute domain: %w", err)
	}

	// Compute signing root = SHA256(message_hash || domain).
	// MessageHash returns HashTreeRoot(pubkey, withdrawal_credentials, amount).
	messageRoot, err := dd.MessageHash()
	if err != nil {
		return nil, fmt.Errorf("epbs/deposit: compute message hash: %w", err)
	}
	signingRoot := utils.Sha256(messageRoot[:], domain)

	sig, err := signer.SignDeposit(ctx, common.Hash(signingRoot))
	if err != nil {
		return nil, fmt.Errorf("epbs/deposit: sign: %w", err)
	}
	dd.Signature = sig

	return dd, nil
}

// ResolveIndex searches the current head state for a builder whose pubkey
// matches the manager's key. It returns the builder index and true if found,
// or (0, false, nil) if the builder is not yet registered.
func (m *BuilderManager) ResolveIndex(sd synced_data.SyncedData) (uint64, bool, error) {
	pubkey := m.Pubkey()
	var idx uint64
	var found bool

	err := sd.ViewHeadState(func(s *state.CachingBeaconState) error {
		builders := s.GetBuilders()
		if builders == nil {
			return nil
		}
		for i := 0; i < builders.Len(); i++ {
			if builders.Get(i).Pubkey == pubkey {
				idx = uint64(i)
				found = true
				break
			}
		}
		return nil
	})
	if err != nil {
		return 0, false, fmt.Errorf("epbs/deposit: resolve index: %w", err)
	}
	return idx, found, nil
}
