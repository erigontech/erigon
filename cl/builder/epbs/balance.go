package epbs

import (
	"context"
	"time"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	log "github.com/erigontech/erigon/common/log/v3"
)

const (
	balanceCheckInterval = 32 * 12 * time.Second // ~1 epoch
)

// BalanceStatus holds the result of a single balance check.
type BalanceStatus struct {
	Active  bool   // builder is active (deposit finalized, not exiting)
	Balance uint64 // current on-chain builder balance (gwei)
}

// CheckBalance queries the head state for the builder's on-chain status.
// Returns zero-value BalanceStatus and an error if the state is unavailable.
func CheckBalance(sd synced_data.SyncedData, builderIndex uint64) (BalanceStatus, error) {
	var status BalanceStatus
	err := sd.ViewHeadState(func(s *state.CachingBeaconState) error {
		status.Active = state.IsActiveBuilder(s, builderIndex)
		builders := s.GetBuilders()
		if builders != nil && int(builderIndex) < builders.Len() {
			status.Balance = builders.Get(int(builderIndex)).Balance
		}
		return nil
	})
	return status, err
}

// RunBalanceMonitor periodically logs the builder's on-chain balance and
// active status. When the builder index is 0 (failed initial resolve), it
// attempts to re-resolve the index from the head state each tick.
// It exits when ctx is cancelled.
func RunBalanceMonitor(ctx context.Context, sd synced_data.SyncedData, manager *BuilderManager) {
	ticker := time.NewTicker(balanceCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			builderIndex, resolved := manager.BuilderIndex()

			// Attempt re-resolve if index is still unresolved.
			if !resolved {
				idx, found, err := manager.ResolveIndex(sd)
				if err != nil {
					log.Debug("ePBS builder: re-resolve index failed", "err", err)
					continue
				}
				if found {
					manager.SetBuilderIndex(idx)
					builderIndex = idx
					log.Info("ePBS builder: index resolved on retry", "builderIndex", idx)
				} else {
					log.Debug("ePBS builder: pubkey still not in builders registry")
					continue
				}
			}

			status, err := CheckBalance(sd, builderIndex)
			if err != nil {
				log.Debug("ePBS builder: balance check failed", "err", err)
				continue
			}
			log.Info("ePBS builder: balance status",
				"builderIndex", builderIndex,
				"active", status.Active,
				"balance_gwei", status.Balance,
			)
			if !status.Active {
				log.Warn("ePBS builder: builder is NOT active on-chain — bids will be rejected",
					"builderIndex", builderIndex)
			}
		}
	}
}
