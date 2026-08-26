package epbs

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
	log "github.com/erigontech/erigon/common/log/v3"
)

var (
	ErrBuilderIndexMismatch   = errors.New("builder index does not match signing key")
	ErrBuilderVersionMismatch = errors.New("builder version is not supported for payload bids")
)

const (
	balanceCheckInterval = 32 * 12 * time.Second // ~1 epoch
)

// BalanceStatus holds the result of a single balance check.
type BalanceStatus struct {
	Active  bool   // builder is active (deposit finalized, not exiting)
	Balance uint64 // current on-chain builder balance (gwei)
	Slot    uint64
}

// CheckBalance queries the head state for the builder's on-chain status.
// Returns zero-value BalanceStatus and an error if the state is unavailable.
func CheckBalance(sd synced_data.SyncedData, builderIndex uint64, pubkey common.Bytes48) (BalanceStatus, error) {
	var status BalanceStatus
	err := sd.ViewHeadState(func(s *state.CachingBeaconState) error {
		var err error
		status, err = builderStatusAtIndex(s, builderIndex, pubkey)
		return err
	})
	return status, err
}

func builderStatusAtIndex(s *state.CachingBeaconState, builderIndex uint64, pubkey common.Bytes48) (BalanceStatus, error) {
	status := BalanceStatus{Slot: s.Slot()}
	builders := s.GetBuilders()
	if builders == nil || int(builderIndex) >= builders.Len() {
		return status, ErrBuilderIndexMismatch
	}
	builder := builders.Get(int(builderIndex))
	if builder == nil || builder.Pubkey != pubkey {
		return status, ErrBuilderIndexMismatch
	}
	if builder.Version != s.BeaconConfig().PayloadBuilderVersion {
		return status, ErrBuilderVersionMismatch
	}
	status.Active = state.IsActiveBuilder(s, builderIndex)
	pending := state.GetPendingBalanceToWithdrawForBuilder(s, builderIndex)
	if pending > math.MaxUint64-s.BeaconConfig().MinDepositAmount {
		return status, nil
	}
	unavailable := s.BeaconConfig().MinDepositAmount + pending
	if builder.Balance >= unavailable {
		status.Balance = builder.Balance - unavailable
	}
	return status, nil
}

func builderStatusForPubkey(s *state.CachingBeaconState, pubkey common.Bytes48) (uint64, BalanceStatus, bool) {
	builders := s.GetBuilders()
	if builders == nil {
		return 0, BalanceStatus{}, false
	}
	for i := range builders.Len() {
		builder := builders.Get(i)
		if builder == nil || builder.Pubkey != pubkey {
			continue
		}
		status, err := builderStatusAtIndex(s, uint64(i), pubkey)
		return uint64(i), status, err == nil
	}
	return 0, BalanceStatus{}, false
}

// RunBalanceMonitor refreshes builder status from the selected head until cancellation.
func RunBalanceMonitor(ctx context.Context, sd synced_data.SyncedData, manager *BuilderManager) {
	ticker := time.NewTicker(balanceCheckInterval)
	defer ticker.Stop()

	for {
		refreshBuilderBalance(sd, manager)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func refreshBuilderBalance(sd synced_data.SyncedData, manager *BuilderManager) {
	builderIndex, resolved := manager.BuilderIndex()
	if !resolved {
		idx, found, err := manager.ResolveIndex(sd)
		if err != nil {
			log.Debug("ePBS builder: re-resolve index failed", "err", err)
			return
		}
		if !found {
			log.Debug("ePBS builder: pubkey still not in builders registry")
			return
		}
		manager.SetBuilderIndex(idx)
		builderIndex = idx
		log.Info("ePBS builder: index resolved on retry", "builderIndex", idx)
	}
	status, err := CheckBalance(sd, builderIndex, manager.Pubkey())
	if err != nil {
		if errors.Is(err, ErrBuilderIndexMismatch) {
			manager.InvalidateBuilderIndex()
		}
		log.Debug("ePBS builder: balance check failed", "err", err)
		return
	}
	manager.SetBalanceStatus(status)
	log.Info("ePBS builder: balance status", "builderIndex", builderIndex, "active", status.Active, "balance_gwei", status.Balance)
}
