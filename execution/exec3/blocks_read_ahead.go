package exec3

import (
	"context"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/turbo/services"
)

func BlocksReadAhead(ctx context.Context, workers int, db kv.RoDB, engine consensus.Engine, blockReader services.FullBlockReader) (chan uint64, context.CancelFunc) {
	const readAheadBlocks = 100
	readAhead := make(chan uint64, readAheadBlocks)
	g, gCtx := errgroup.WithContext(ctx)
	for workerNum := 0; workerNum < workers; workerNum++ {
		g.Go(func() (err error) {
			var bn uint64
			var ok bool
			var tx kv.Tx
			defer func() {
				if tx != nil {
					tx.Rollback()
				}
			}()

			for i := 0; ; i++ {
				select {
				case bn, ok = <-readAhead:
					if !ok {
						return
					}
				case <-gCtx.Done():
					return gCtx.Err()
				}

				if i%100 == 0 {
					if tx != nil {
						tx.Rollback()
					}
					tx, err = db.BeginRo(ctx)
					if err != nil {
						return err
					}
				}

				if err := blocksReadAheadFunc(gCtx, tx, bn+readAheadBlocks, engine, blockReader); err != nil {
					return err
				}
			}
		})
	}
	return readAhead, func() {
		close(readAhead)
		_ = g.Wait()
	}
}
func blocksReadAheadFunc(ctx context.Context, tx kv.Tx, blockNum uint64, engine consensus.Engine, blockReader services.FullBlockReader) error {
	block, err := blockReader.BlockByNumber(ctx, tx, blockNum)
	if err != nil {
		return err
	}
	if block == nil {
		return nil
	}
	_, _ = engine.Author(block.HeaderNoCopy()) // Bor consensus: this calc is heavy and has cache

	ttx, ok := tx.(kv.TemporalTx)
	if !ok {
		return nil
	}

	stateReader := state.NewReaderV3(ttx)
	senders := block.Body().SendersFromTxs()

	for _, sender := range senders {
		a, _ := stateReader.ReadAccountData(sender)
		if a == nil {
			continue
		}

		//Code domain using .bt index - means no false-positives
		if code, _ := stateReader.ReadAccountCode(sender); len(code) > 0 {
			_, _ = code[0], code[len(code)-1]
		}
	}

	for _, txn := range block.Transactions() {
		to := txn.GetTo()
		if to != nil {
			a, _ := stateReader.ReadAccountData(*to)
			if a == nil {
				continue
			}
			//if account != nil && !bytes.Equal(account.CodeHash, types.EmptyCodeHash.Bytes()) {
			//	reader.Code(*tx.To(), common.BytesToHash(account.CodeHash))
			//}
			if code, _ := stateReader.ReadAccountCode(*to); len(code) > 0 {
				_, _ = code[0], code[len(code)-1]
			}

			for _, list := range txn.GetAccessList() {
				stateReader.ReadAccountData(list.Address)
				if len(list.StorageKeys) > 0 {
					for _, slot := range list.StorageKeys {
						stateReader.ReadAccountStorage(list.Address, slot)
					}
				}
			}
			//TODO: exec txn and pre-fetch commitment keys. see also: `func (p *statePrefetcher) Prefetch` in geth
		}

	}
	_, _ = stateReader.ReadAccountData(block.Coinbase())

	return nil
}
