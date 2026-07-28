package blockreplay

import (
	"context"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/kvmetrics"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// NewWitnessDomains builds a SharedDomains whose in-memory batch is backed by
// the fixture's flat pre-state instead of a temporal source. tx is only a
// construction vehicle (it supplies the domain/index writer shapes); no witness
// data is read from or flushed to it — the whole witness lives in the mem batch
// seam. The returned SharedDomains serves the block's reads as "latest"; the
// caller runs the parallel executor against it and must never Flush.
// WitnessWriteSet exposes the replay's post-Seal write-set so the ephemeral
// verifier can require it to equal the reference output key set exactly.
type WitnessWriteSet struct{ mem *witnessMemBatch }

// Diff returns the key-set differences between the replay's write-set and want,
// in both directions (extra and missing writes). Empty when they match exactly.
func (ws *WitnessWriteSet) Diff(want *Outputs) []string { return ws.mem.writeSetDiff(want) }

func NewWitnessDomains(ctx context.Context, tx kv.TemporalRwTx, fx *Fixture, seedTxNum uint64, logger log.Logger) (*execctx.SharedDomains, *WitnessWriteSet, error) {
	metrics := &kvmetrics.DomainMetrics{Domains: map[kv.Domain]*kvmetrics.DomainIOMetrics{}}
	wmem := newWitnessMemBatch(tx.Debug().NewMemBatch(metrics))

	doms, err := execctx.NewSharedDomains(ctx, tx, logger, execctx.WithMemBatch(wmem))
	if err != nil {
		return nil, nil, err
	}
	doms.SetTxNum(seedTxNum)

	w := state.NewWriter(doms.AsPutDel(tx), nil, seedTxNum)
	for a, d := range fx.Accounts {
		if !d.Present {
			continue
		}
		orig := accounts.NewAccount()
		if err := w.UpdateAccountData(accounts.InternAddress(common.Address(a)), &orig, d.toAccount()); err != nil {
			doms.Close()
			return nil, nil, err
		}
	}
	for a, code := range fx.Code {
		if len(code) == 0 {
			continue
		}
		d := fx.Accounts[a]
		ch := accounts.InternCodeHash(common.BytesToHash(d.CodeHash[:]))
		if err := w.UpdateAccountCode(accounts.InternAddress(common.Address(a)), d.Incarnation, ch, code); err != nil {
			doms.Close()
			return nil, nil, err
		}
	}
	for a, slots := range fx.Storage {
		addr := accounts.InternAddress(common.Address(a))
		inc := fx.Accounts[a].Incarnation
		for k, v := range slots {
			var val uint256.Int
			val.SetBytes(v[:])
			if err := w.WriteAccountStorage(addr, inc, accounts.InternKey(common.Hash(k)), uint256.Int{}, val); err != nil {
				doms.Close()
				return nil, nil, err
			}
		}
	}

	wmem.Seal()
	return doms, &WitnessWriteSet{mem: wmem}, nil
}
