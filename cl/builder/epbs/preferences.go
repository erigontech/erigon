package epbs

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/common"
)

const preferencesPollInterval = 10 * time.Millisecond

var (
	ErrPreferencesTimeout = errors.New("timed out waiting for proposer preferences")
	ErrInvalidPreferences = errors.New("invalid proposer preferences")
)

// PreferencesSource supplies validated proposer preferences for one slot and dependent root.
type PreferencesSource interface {
	WaitForPreferences(context.Context, uint64, common.Hash, time.Duration) (*cltypes.SignedProposerPreferences, error)
}

// PooledPreferences reads preferences already validated by the Gloas gossip service.
type PooledPreferences struct {
	pool         *pool.EpbsPool
	pollInterval time.Duration
}

// NewPooledPreferences creates a preference source backed by the shared ePBS pool.
func NewPooledPreferences(epbsPool *pool.EpbsPool) *PooledPreferences {
	return newPooledPreferences(epbsPool, preferencesPollInterval)
}

func newPooledPreferences(epbsPool *pool.EpbsPool, pollInterval time.Duration) *PooledPreferences {
	return &PooledPreferences{pool: epbsPool, pollInterval: pollInterval}
}

func (p *PooledPreferences) WaitForPreferences(
	ctx context.Context,
	slot uint64,
	dependentRoot common.Hash,
	timeout time.Duration,
) (*cltypes.SignedProposerPreferences, error) {
	if p == nil || p.pool == nil {
		return nil, fmt.Errorf("%w: nil pool", ErrInvalidPreferences)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if preference, found, err := p.lookup(slot, dependentRoot); found || err != nil {
		return preference, err
	}

	pollInterval := p.pollInterval
	if pollInterval <= 0 {
		pollInterval = preferencesPollInterval
	}
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			return nil, fmt.Errorf("%w for slot %d", ErrPreferencesTimeout, slot)
		case <-ticker.C:
			if preference, found, err := p.lookup(slot, dependentRoot); found || err != nil {
				return preference, err
			}
		}
	}
}

func (p *PooledPreferences) lookup(
	slot uint64,
	dependentRoot common.Hash,
) (*cltypes.SignedProposerPreferences, bool, error) {
	preference, found := p.pool.GetPreference(slot, dependentRoot)
	if !found {
		return nil, false, nil
	}
	if preference == nil || preference.Message == nil {
		return nil, true, fmt.Errorf("%w: nil message", ErrInvalidPreferences)
	}
	if preference.Message.ProposalSlot != slot || preference.Message.DependentRoot != dependentRoot {
		return nil, true, fmt.Errorf("%w: stored value does not match lookup key", ErrInvalidPreferences)
	}
	return preference, true, nil
}
