package blockreplay

import (
	"context"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// SeedDomains writes the fixture's pre-state into db's domains at txNum via the
// production Writer, so a SharedDomains opened afterward serves the block's
// reads as "latest" — the substrate the parallel executor needs, with no frozen
// files behind it (read cost zeroized).
func SeedDomains(ctx context.Context, db kv.TemporalRwDB, fx *Fixture, txNum uint64, logger log.Logger) error {
	rwTx, err := db.BeginTemporalRw(ctx)
	if err != nil {
		return err
	}
	defer rwTx.Rollback()

	doms, err := execctx.NewSharedDomains(ctx, rwTx, logger)
	if err != nil {
		return err
	}
	defer doms.Close()
	doms.SetTxNum(txNum)

	w := state.NewWriter(doms.AsPutDel(rwTx), nil, txNum)

	for a, d := range fx.Accounts {
		if !d.Present {
			continue
		}
		orig := accounts.NewAccount()
		if err := w.UpdateAccountData(accounts.InternAddress(common.Address(a)), &orig, d.toAccount()); err != nil {
			return err
		}
	}
	for a, code := range fx.Code {
		if len(code) == 0 {
			continue
		}
		d := fx.Accounts[a]
		ch := accounts.InternCodeHash(common.BytesToHash(d.CodeHash[:]))
		if err := w.UpdateAccountCode(accounts.InternAddress(common.Address(a)), d.Incarnation, ch, code); err != nil {
			return err
		}
	}
	for a, slots := range fx.Storage {
		addr := accounts.InternAddress(common.Address(a))
		inc := fx.Accounts[a].Incarnation
		for k, v := range slots {
			var val uint256.Int
			val.SetBytes(v[:])
			if err := w.WriteAccountStorage(addr, inc, accounts.InternKey(common.Hash(k)), uint256.Int{}, val); err != nil {
				return err
			}
		}
	}

	if err := doms.Flush(ctx, rwTx); err != nil {
		return err
	}
	return rwTx.Commit()
}
