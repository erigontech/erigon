package parlia

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/types"
)

const (
	wiggleTimeBeforeFork       = 500 * time.Millisecond // Random delay (per signer) to allow concurrent signers
	fixedBackOffTimeBeforeFork = 200 * time.Millisecond
)

func (p *Parlia) delayForRamanujanFork(snap *Snapshot, header *types.Header) time.Duration {
	delay := time.Until(time.Unix(int64(header.Time), 0)) // nolint: gosimple
	if p.chainConfig.IsRamanujan(header.Number.Uint64()) {
		return delay
	}
	if header.Difficulty.Cmp(diffNoTurn) == 0 {
		// It's not our turn explicitly to sign, delay it a bit
		wiggle := time.Duration(len(snap.Validators)/2+1) * wiggleTimeBeforeFork
		delay += fixedBackOffTimeBeforeFork + time.Duration(rand.Int63n(int64(wiggle))) // nolint
	}
	return delay
}

func (p *Parlia) blockTimeForRamanujanFork(snap *Snapshot, header, parent *types.Header) uint64 {
	blockTime := parent.Time + p.config.Period
	if p.chainConfig.IsRamanujan(header.Number.Uint64()) {
		blockTime = blockTime + backOffTime(snap, p.val)
	}
	return blockTime
}

func (p *Parlia) blockTimeVerifyForRamanujanFork(snap *Snapshot, header, parent *types.Header) error {
	if p.chainConfig.IsRamanujan(header.Number.Uint64()) {
		if header.Time < parent.Time+p.config.Period+backOffTime(snap, header.Coinbase) {
			return fmt.Errorf("header %d, time %d, now %d, period: %d, backof: %d, %w", header.Number.Uint64(), header.Time, time.Now().Unix(), p.config.Period, backOffTime(snap, header.Coinbase), consensus.ErrFutureBlock)
		}
	}
	return nil
}
