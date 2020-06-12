package stagedsync

import (
)

// Functionality for inserting headers into the database and handing reorgs

// WriteHeader writes a header into the local chain, given that its parent is
// already known. If the total difficulty of the newly inserted header becomes
// greater than the current known TD, the canonical chain is re-routed.
//
// Note: This method is not concurrent-safe with inserting blocks simultaneously
// into the chain, as side effects caused by reorganisations cannot be emulated
// without the real blocks. Hence, writing headers directly should only be done
// in two scenarios: pure-header mode of operation (light clients), or properly
// separated header/block phases (non-archive clients).
func (hc *HeaderChain) WriteHeader(ctx context.Context, header *types.Header) (status WriteStatus, err error) {
	// Cache some values to prevent constant recalculation
	var (
		hash   = header.Hash()
		number = header.Number.Uint64()
	)
	// Calculate the total difficulty of the header
	ptd := hc.GetTd(hc.chainDb, header.ParentHash, number-1)
	if ptd == nil {
		return NonStatTy, consensus.ErrUnknownAncestor
	}
	head := hc.CurrentHeader().Number.Uint64()
	localTd := hc.GetTd(hc.chainDb, hc.currentHeaderHash, head)
	externTd := new(big.Int).Add(header.Difficulty, ptd)

	// Irrelevant of the canonical status, write the td and header to the database
	headerBatch := hc.chainDb.NewBatch()
	rawdb.WriteTd(headerBatch, hash, number, externTd)
	rawdb.WriteHeader(ctx, headerBatch, header)
	if _, err := headerBatch.Commit(); err != nil {
		log.Crit("Failed to write header into disk", "err", err)
	}
	// If the total difficulty is higher than our known, add it to the canonical chain
	// Second clause in the if statement reduces the vulnerability to selfish mining.
	// Please refer to http://www.cs.cornell.edu/~ie53/publications/btcProcFC.pdf
	reorg := externTd.Cmp(localTd) > 0
	if !reorg && externTd.Cmp(localTd) == 0 {
		if header.Number.Uint64() < head {
			reorg = true
		} else if header.Number.Uint64() == head {
			reorg = mrand.Float64() < 0.5
		}
	}
	if reorg {
		// If the header can be added into canonical chain, adjust the
		// header chain markers(canonical indexes and head header flag).
		//
		// Note all markers should be written atomically.

		// Delete any canonical number assignments above the new head
		markerBatch := hc.chainDb.NewBatch()
		for i := number + 1; ; i++ {
			hash := rawdb.ReadCanonicalHash(hc.chainDb, i)
			if hash == (common.Hash{}) {
				break
			}
			rawdb.DeleteCanonicalHash(markerBatch, i)
		}

		// Overwrite any stale canonical number assignments
		var (
			headHash   = header.ParentHash
			headNumber = header.Number.Uint64() - 1
			headHeader = hc.GetHeader(headHash, headNumber)
		)
		for rawdb.ReadCanonicalHash(hc.chainDb, headNumber) != headHash {
			rawdb.WriteCanonicalHash(markerBatch, headHash, headNumber)

			headHash = headHeader.ParentHash
			headNumber = headHeader.Number.Uint64() - 1
			headHeader = hc.GetHeader(headHash, headNumber)
		}
		// Extend the canonical chain with the new header
		rawdb.WriteCanonicalHash(markerBatch, hash, number)
		rawdb.WriteHeadHeaderHash(markerBatch, hash)
		if _, err := markerBatch.Commit(); err != nil {
			log.Crit("Failed to write header markers into disk", "err", err)
		}
		// Last step update all in-memory head header markers
		hc.currentHeaderHash = hash
		hc.currentHeader.Store(types.CopyHeader(header))
		headHeaderGauge.Update(header.Number.Int64())

		status = CanonStatTy
	} else {
		status = SideStatTy
	}
	hc.tdCache.Add(hash, externTd)
	hc.headerCache.Add(hash, header)
	hc.numberCache.Add(hash, number)
	return
}

func (bc *BlockChain) insertHeaderChainStaged(chain []*types.Header) (int, int, int, bool, uint64, error) {
	// The function below will insert headers, and track the lowest block
	// number that have replace the canonical chain. This number will be
	// used to trigger invalidation of further sync stages
	var newCanonical bool
	var lowestCanonicalNumber uint64
	whFunc := func(header *types.Header) error {
		status, err := bc.hc.WriteHeader(context.Background(), header)
		if err != nil {
			return err
		}
		if status == CanonStatTy {
			number := header.Number.Uint64()
			if !newCanonical || number < lowestCanonicalNumber {
				lowestCanonicalNumber = number
				newCanonical = true
			}
		}
		return nil
	}
	// Collect some import statistics to report on
	stats := struct{ processed, ignored int }{}
	// All headers passed verification, import them into the database
	for i, header := range chain {
		// Short circuit insertion if shutting down
		if bc.hc.procInterrupt() {
			log.Debug("Premature abort during headers import")
			return i, stats.processed, stats.ignored, newCanonical, lowestCanonicalNumber, errors.New("aborted")
		}
		// If the header's already known, skip it, otherwise store
		hash := header.Hash()
		if bc.hc.HasHeader(hash, header.Number.Uint64()) {
			externTd := bc.hc.GetTd(bc.hc.chainDb, hash, header.Number.Uint64())
			localTd := bc.hc.GetTd(bc.hc.chainDb, bc.hc.currentHeaderHash, bc.hc.CurrentHeader().Number.Uint64())
			if externTd == nil || externTd.Cmp(localTd) <= 0 {
				stats.ignored++
				continue
			}
		}
		if err := whFunc(header); err != nil {
			return i, stats.processed, stats.ignored, newCanonical, lowestCanonicalNumber, err
		}
		stats.processed++
	}
	// Everything processed without errors
	return len(chain), stats.processed, stats.ignored, newCanonical, lowestCanonicalNumber, nil
}
