package state

import (
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// LayeredDomainReader composes the multi-block versionMap window (the finished-
// but-not-yet-committed prior blocks) over the shared domain (sd.mem) and the
// domain files. It is the single place the executor and the commitment
// calculator share the "map layered over sd" read model: a value written by a
// prior block that the apply goroutine has not yet flushed to sd.mem is served
// from that block's versionMap instead of a stale domain read.
//
// The window is selected by txNum (matching the per-tx apply): a read at ts sees
// every prior block whose block-end txNum is < ts. Account/Storage value reads
// are layered over sd.mem + files; the commitment / code domains and history
// read the base directly (the map holds account/storage state only).
//
// roTx is the per-user files-fallback tx: clone the reader (CloneWithTx) for each
// concurrent user rather than sharing one.
type LayeredDomainReader struct {
	sd         execctx.DomainReader
	roTx       kv.TemporalTx
	prevBlocks *PrevBlockList
}

// NewLayeredDomainReader composes the window + sd + files. prevBlocks nil layers
// nothing (byte-identical base reads), so callers get the plain sd+files path
// when prev-block reads are disabled.
func NewLayeredDomainReader(sd execctx.DomainReader, roTx kv.TemporalTx, prevBlocks *PrevBlockList) *LayeredDomainReader {
	return &LayeredDomainReader{sd: sd, roTx: roTx, prevBlocks: prevBlocks}
}

// CloneWithTx returns a copy bound to tx — each concurrent user carries its own
// files-fallback tx rather than sharing the base reader's.
func (l *LayeredDomainReader) CloneWithTx(tx kv.TemporalTx) *LayeredDomainReader {
	return &LayeredDomainReader{sd: l.sd, roTx: tx, prevBlocks: l.prevBlocks}
}

// baseGetAsOf reads sd.mem then the domain files — the committed base without the
// prior-block window.
func (l *LayeredDomainReader) baseGetAsOf(name kv.Domain, k []byte, ts uint64) ([]byte, bool, error) {
	enc, ok, err := l.sd.GetAsOf(name, k, ts)
	if err != nil {
		return nil, false, err
	}
	if ok {
		return enc, true, nil
	}
	if l.roTx == nil {
		return nil, false, nil
	}
	return l.roTx.GetAsOf(name, k, ts)
}

// ReadDomain resolves a domain read for the commitment calculator: account/
// storage layer the window over the base; everything else reads the base.
func (l *LayeredDomainReader) ReadDomain(name kv.Domain, k []byte, ts uint64) ([]byte, bool, error) {
	if l.prevBlocks != nil && (name == kv.AccountsDomain || name == kv.StorageDomain) {
		return l.readLayered(name, k, ts)
	}
	return l.baseGetAsOf(name, k, ts)
}

// readLayered composes the versionMap window over the base (sd.mem + files) and
// re-encodes the decoded result to the domain encoding. Authoritative: a zero /
// absent layered result IS the answer (a prior block's self-destruct or delete
// must shadow a stale committed value), so it does not fall through to the base.
func (l *LayeredDomainReader) readLayered(name kv.Domain, k []byte, ts uint64) ([]byte, bool, error) {
	layered := PrevBlockBaseTxNum(&domainDecodedBase{l: l, ts: ts}, l.prevBlocks, ts)
	switch name {
	case kv.AccountsDomain:
		acc, err := layered.ReadAccountData(accounts.InternAddress(common.BytesToAddress(k)))
		if err != nil || acc == nil {
			return nil, false, err
		}
		return accounts.SerialiseV3(acc), true, nil
	case kv.StorageDomain:
		addr := accounts.InternAddress(common.BytesToAddress(k[:20]))
		key := accounts.InternKey(common.BytesToHash(k[20:]))
		val, ok, err := layered.ReadAccountStorage(addr, key)
		if err != nil || !ok || val.IsZero() {
			return nil, false, err
		}
		return val.Bytes(), true, nil
	}
	return l.baseGetAsOf(name, k, ts)
}

// domainDecodedBase adapts the base (sd.mem + files) encoded reads into a decoded
// StateReader so the versionMap layers compose over it. Only account/storage/code
// are exercised by the fold; the rest return zero values.
type domainDecodedBase struct {
	l  *LayeredDomainReader
	ts uint64
}

func (b *domainDecodedBase) ReadAccountData(addr accounts.Address) (*accounts.Account, error) {
	av := addr.Value()
	enc, ok, err := b.l.baseGetAsOf(kv.AccountsDomain, av[:], b.ts)
	if err != nil || !ok || len(enc) == 0 {
		return nil, err
	}
	acc := new(accounts.Account)
	if err := accounts.DeserialiseV3(acc, enc); err != nil {
		return nil, err
	}
	return acc, nil
}

func (b *domainDecodedBase) ReadAccountDataForDebug(addr accounts.Address) (*accounts.Account, error) {
	return b.ReadAccountData(addr)
}

func (b *domainDecodedBase) ReadAccountStorage(addr accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	av := addr.Value()
	kh := key.Value()
	composite := make([]byte, 20+32)
	copy(composite, av[:])
	copy(composite[20:], kh[:])
	enc, ok, err := b.l.baseGetAsOf(kv.StorageDomain, composite, b.ts)
	if err != nil || !ok || len(enc) == 0 {
		return uint256.Int{}, false, err
	}
	var val uint256.Int
	val.SetBytes(enc)
	return val, true, nil
}

func (b *domainDecodedBase) HasStorage(addr accounts.Address) (bool, error) { return false, nil }

func (b *domainDecodedBase) ReadAccountCode(addr accounts.Address) ([]byte, error) {
	av := addr.Value()
	enc, ok, err := b.l.baseGetAsOf(kv.CodeDomain, av[:], b.ts)
	if err != nil || !ok {
		return nil, err
	}
	return enc, nil
}

func (b *domainDecodedBase) ReadAccountCodeSize(addr accounts.Address) (int, error) {
	code, err := b.ReadAccountCode(addr)
	return len(code), err
}

func (b *domainDecodedBase) ReadAccountIncarnation(addr accounts.Address) (uint64, error) {
	return 0, nil
}

func (b *domainDecodedBase) SetTrace(bool, string) {}
func (b *domainDecodedBase) Trace() bool           { return false }
func (b *domainDecodedBase) TracePrefix() string   { return "" }
