package stagedsync

import (
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// csWrite is one buffered per-tx domain write (new value; nil = delete) captured
// at feed time; prevs are resolved later, at settled compute time.
type csWrite struct {
	domain kv.Domain
	key    []byte
	val    []byte
	txNum  uint64
}

// bufferChangesetWrites buffers an owned block's per-tx writes (new values captured
// now, from the post-tx state) so the changeset can be replayed and its prevs
// resolved at settled compute time — matching exec's block-end flush timing rather
// than the racy per-tx moment. Non-owned blocks need no changeset.
func (cc *commitmentCalculator) bufferChangesetWrites(r *txResult) {
	if !cc.ownsChangeset(r.blockNum) {
		return
	}
	if cc.csBuilderBlock != r.blockNum {
		cc.csPending = cc.csPending[:0]
		cc.csBuilderBlock = r.blockNum
	}
	cc.feedChangeset(r.writes, r.txNum)
}

// asOfPrevReader supplies the pre-block value of an account/storage/code key as
// raw stored bytes via GetAsOf(txNum) — the same bytes exec's DomainPut resolves
// as prevVal through GetLatest at flush time. Only the first write to a key in a
// block consults it; the builder chains later prevs in memory.
type asOfPrevReader struct {
	sd   *execctx.SharedDomains
	roTx kv.TemporalTx
}

func (r *asOfPrevReader) prevValue(domain kv.Domain, key []byte, txNum uint64) ([]byte, error) {
	enc, ok, err := r.sd.GetAsOf(domain, key, txNum)
	if err != nil {
		return nil, err
	}
	if ok {
		return enc, nil
	}
	enc, ok, err = r.roTx.GetAsOf(domain, key, txNum)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return enc, nil
}

func (cc *commitmentCalculator) appendCS(domain kv.Domain, key, val []byte, txNum uint64) {
	w := csWrite{domain: domain, key: common.Copy(key), txNum: txNum}
	if val != nil {
		w.val = common.Copy(val)
	}
	cc.csPending = append(cc.csPending, w)
}

// feedChangeset buffers one tx's account/storage/code writes, mirroring exec's
// per-tx BlockStateCache.Flush op emission: the account is serialized once per tx
// from the calc's post-tx state (a Deleted+all-zero account is a delete),
// storage/code puts carry their new bytes (empty → delete), and a self-destruct
// cascades a storage-subtree wipe. Prevs are resolved later (at settled compute
// time) so the replayed ChangeSets3 bytes match exec's.
func (cc *commitmentCalculator) feedChangeset(writes *state.WriteSet, txNum uint64) {
	touched := map[accounts.Address]struct{}{}
	for addr := range writes.Balances() {
		touched[addr] = struct{}{}
	}
	for addr := range writes.Nonces() {
		touched[addr] = struct{}{}
	}
	for addr := range writes.CodeHashes() {
		touched[addr] = struct{}{}
	}
	for addr := range writes.Codes() {
		touched[addr] = struct{}{}
	}
	for addr := range writes.Incarnations() {
		touched[addr] = struct{}{}
	}
	for addr, vw := range writes.SelfDestructs() {
		if vw.Val {
			touched[addr] = struct{}{}
		}
	}

	for addr := range touched {
		addrVal := addr.Value()
		acc := cc.state.accounts[addr]
		if acc == nil {
			continue
		}
		if acc.Deleted && acc.Balance.IsZero() && acc.Nonce == 0 && acc.CodeHash == empty.CodeHash {
			cc.appendCS(kv.AccountsDomain, addrVal[:], nil, txNum)
		} else {
			a := accounts.NewAccount()
			a.Balance = acc.Balance
			a.Nonce = acc.Nonce
			a.Incarnation = acc.Incarnation
			a.CodeHash = accounts.InternCodeHash(common.Hash(acc.CodeHash))
			cc.appendCS(kv.AccountsDomain, addrVal[:], accounts.SerialiseV3(&a), txNum)
		}
	}

	for addr, vw := range writes.Codes() {
		addrVal := addr.Value()
		if vw.Val.Len() == 0 {
			cc.appendCS(kv.CodeDomain, addrVal[:], nil, txNum)
		} else {
			cc.appendCS(kv.CodeDomain, addrVal[:], vw.Val.Bytes, txNum)
		}
	}

	for addr, inner := range writes.Storages() {
		addrVal := addr.Value()
		for key, vw := range inner {
			keyVal := key.Value()
			composite := make([]byte, 20+32)
			copy(composite, addrVal[:])
			copy(composite[20:], keyVal[:])
			vb := vw.Val.Bytes()
			if len(vb) == 0 {
				cc.appendCS(kv.StorageDomain, composite, nil, txNum)
			} else {
				cc.appendCS(kv.StorageDomain, composite, vb, txNum)
			}
		}
	}

	// Self-destruct wipes the whole storage subtree (exec's DomainDelPrefix) and
	// deletes the code — reproduce both as deletes over the as-of subtree.
	for addr, vw := range writes.SelfDestructs() {
		if !vw.Val {
			continue
		}
		addrVal := addr.Value()
		cc.appendCS(kv.CodeDomain, addrVal[:], nil, txNum)
		if cc.state.storageEnum == nil {
			continue
		}
		_ = cc.state.storageEnum.EachStorageSlot(addr, func(key accounts.StorageKey) error {
			keyVal := key.Value()
			composite := make([]byte, 20+32)
			copy(composite, addrVal[:])
			copy(composite[20:], keyVal[:])
			cc.appendCS(kv.StorageDomain, composite, nil, txNum)
			return nil
		})
	}
}

// buildResultLocalChangeset replays the buffered per-tx writes through a fresh
// builder AT COMPUTE TIME — resolving prevs when block N-1 is settled in sd.mem
// (matching exec's block-end flush) — and returns the account/storage/code diffs
// (domains 0..2) as a StateChangeSet. The commitment domain (3) is left empty for
// the caller's compute to fill. Returns nil if no writes were buffered for the
// block or a prev-read failed.
func (cc *commitmentCalculator) buildResultLocalChangeset(blockNum uint64) *changeset.StateChangeSet {
	if cc.csBuilderBlock != blockNum {
		return nil
	}
	b := newChangesetBuilder(cc.prevReader, cc.doms.StepSize())
	for _, w := range cc.csPending {
		b.record(w.domain, w.key, w.val, w.txNum)
	}
	if err := b.err(); err != nil {
		cc.logger.Warn("["+cc.logPrefix+"] changeset reconstruct: reader error", "block", blockNum, "err", err)
		return nil
	}
	return b.result()
}
