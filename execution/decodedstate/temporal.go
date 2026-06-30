package decodedstate

import (
	"context"
	"encoding/binary"

	"github.com/erigontech/erigon/common"
	log "github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
)

// WriteEntriesToDomains writes decoded entries through the decoded storage temporal
// domain using an already-open SharedDomains instance. This advances DomainProgress
// so the aggregator can merge/freeze/prune the decoded data.
//
// This is called from the staged sync executors (serial and parallel) which already
// have an open SharedDomains instance, avoiding the need to create a fresh one.
func WriteEntriesToDomains(sd *execctx.SharedDomains, ttx kv.TemporalRwTx, txNum uint64, entries []DecodedEntry) error {
	sd.SetTxNum(txNum)

	for i := range entries {
		entry := &entries[i]
		sk := makeStoreKey(entry)
		domainKey := encodeStoreKey(sk)
		isZero := entry.Value == (common.Hash{})

		if isZero {
			if err := sd.DomainDel(kv.DecodedStorageDomain, ttx, domainKey, txNum, nil); err != nil {
				return err
			}
		} else {
			if err := sd.DomainPut(kv.DecodedStorageDomain, ttx, domainKey, entry.Value[:], txNum, nil); err != nil {
				return err
			}
		}
	}

	// Advance domain progress past the current step boundary.
	stepSize := sd.StepSize()
	nextStepTxNum := ((txNum / stepSize) + 1) * stepSize
	{
		progressKey := []byte("__decoded_progress__")
		var progressVal [8]byte
		binary.BigEndian.PutUint64(progressVal[:], nextStepTxNum)
		sd.SetTxNum(nextStepTxNum)
		if err := sd.DomainPut(kv.DecodedStorageDomain, ttx, progressKey, progressVal[:], nextStepTxNum, nil); err != nil {
			return err
		}
	}

	return nil
}

// writeTemporalDomainFromTx writes entries through the temporal domain using an
// existing TemporalRwTx by creating a new SharedDomains. Called from WriteEntriesToTx
// when the tx supports temporal writes.
func writeTemporalDomainFromTx(ttx kv.TemporalRwTx, txNum uint64, entries []DecodedEntry) error {
	ctx := context.Background()
	sd, err := execctx.NewSharedDomains(ctx, ttx, log.New())
	if err != nil {
		return err
	}
	defer sd.Close()

	if err := WriteEntriesToDomains(sd, ttx, txNum, entries); err != nil {
		return err
	}

	return sd.Flush(ctx, ttx)
}

// writeTemporalDomainNewSD writes entries through the temporal domain by creating
// a new SharedDomains instance. Used by the Store interface methods which don't
// have access to an existing SharedDomains.
func writeTemporalDomainNewSD(tdb kv.TemporalRwDB, txNum uint64, entries []DecodedEntry) error {
	ctx := context.Background()
	ttx, err := tdb.BeginTemporalRw(ctx)
	if err != nil {
		return err
	}
	defer ttx.Rollback()

	sd, err := execctx.NewSharedDomains(ctx, ttx, log.New())
	if err != nil {
		return err
	}
	defer sd.Close()

	if err := WriteEntriesToDomains(sd, ttx, txNum, entries); err != nil {
		return err
	}

	if err := sd.Flush(ctx, ttx); err != nil {
		return err
	}
	return ttx.Commit()
}
