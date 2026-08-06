package epbs

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
)

// BuildWithdrawalCredentials constructs withdrawal credentials for a builder deposit.
// Format: BuilderWithdrawalPrefix (0x03) + 11 zero bytes + 20-byte execution address.
func BuildWithdrawalCredentials(feeRecipient common.Address, beaconCfg *clparams.BeaconChainConfig) common.Hash {
	var creds common.Hash
	creds[0] = byte(beaconCfg.BuilderWithdrawalPrefix)
	// bytes 1..11 are zero (zero-value)
	copy(creds[12:], feeRecipient[:])
	return creds
}

func BuildBuilderDepositRequest(
	ctx context.Context,
	signer Signer,
	feeRecipient common.Address,
	amount uint64,
	beaconCfg *clparams.BeaconChainConfig,
) (*solid.BuilderDepositRequest, error) {
	if signer == nil {
		return nil, fmt.Errorf("epbs/deposit: nil signer")
	}
	if beaconCfg == nil {
		return nil, fmt.Errorf("epbs/deposit: nil beacon config")
	}
	if amount < beaconCfg.MinDepositAmount {
		return nil, fmt.Errorf("epbs/deposit: amount %d below minimum %d", amount, beaconCfg.MinDepositAmount)
	}
	pubkey := signer.Pubkey()
	creds := BuildWithdrawalCredentials(feeRecipient, beaconCfg)

	depositData := &cltypes.DepositData{
		PubKey:                pubkey,
		WithdrawalCredentials: creds,
		Amount:                amount,
	}

	domain, err := fork.ComputeDomain(
		beaconCfg.DomainBuilderDeposit[:],
		utils.Uint32ToBytes4(uint32(beaconCfg.GenesisForkVersion)),
		[32]byte{},
	)
	if err != nil {
		return nil, fmt.Errorf("epbs/deposit: compute domain: %w", err)
	}

	messageRoot, err := depositData.MessageHash()
	if err != nil {
		return nil, fmt.Errorf("epbs/deposit: compute message hash: %w", err)
	}
	signingRoot := crypto.Sha256(messageRoot[:], domain)

	sig, err := signer.SignDeposit(ctx, common.Hash(signingRoot))
	if err != nil {
		return nil, fmt.Errorf("epbs/deposit: sign: %w", err)
	}
	return &solid.BuilderDepositRequest{
		PubKey:                pubkey,
		WithdrawalCredentials: creds,
		Amount:                amount,
		Signature:             sig,
	}, nil
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
