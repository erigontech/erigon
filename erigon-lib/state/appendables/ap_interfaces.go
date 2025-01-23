package appendables

import (
	"bytes"
	"context"
	"fmt"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/core/types"
	"github.com/ethereum/go-ethereum/common"
)

// 3 appendable categories:
// 1. CanonicalAppendable: only valsTbl; only canonical data stored (unwind removes all non-canonical data)
// 2. MarkedAppendables: canonicalMarkerTbl and valsTbl
// 3. IncrementingAppendable: tsId always increments...provenance coming from some other structure;
//            (useful when you don't want data to be lost on unwind..e.g. transactions)

// --------------------------------------------------------------------

// vals table k,v types
type VKType []byte
type VVType []byte
type TsNum uint64
type TsId uint64

type Collector func(values []byte) error

// pattern is SetCollector ; and then call Freeze
type Freezer interface {
	// stepKeyFrom/To represent tsNum which the snapshot should range
	// this doesn't check if the snapshot can be created or not. It's the responsibilty of the caller
	// to ensure this.
	Freeze(ctx context.Context, baseTsNumFrom, baseTsNumTo TsNum, tx kv.Tx) error
	SetCollector(coll Collector)
}

type Appendable interface {
	SetFreezer(Freezer)
	SetIndexBuilders([]AccessorIndexBuilder)
	DirtySegmentsMaxTsNum() TsNum
	VisibleSegmentsMaxTsNum() TsNum
	RecalcVisibleFiles(baseTsNumTo TsNum)
	Prune(ctx context.Context, baseTsNumTo TsNum, limit uint64, rwTx kv.RwTx) error
	Unwind(ctx context.Context, baseTsNumFrom TsNum, rwTx kv.RwTx) error

	// don't put BeginFilesRo here, since it returns PointAppendableRo, RangedAppendableRo etc. (each has different query patterns)
	// so anyway aggregator has to recover concrete type, and then it can
	// call BeginFilesRo on that
}

// appendableRoTx extensions...providing different query patterns
// idea is that aggregator "return" a *Queries interface, and user can do Get/Put/Range on that.
// alternate is to expose eveything, but that means exposing tsId/forkId etc. even for appendables
// for which it is not relevant. Plus, sometimes base appendable tsNum should also be managed...

// each appendable kind has a different query pattern

// 1:1
type PointQueries interface {
	Get(tsNum TsNum, tx kv.Tx) (VVType, error)
	Put(tsNum TsNum, value VVType, tx kv.RwTx) error
}

// many:1
type RangedQueries interface {
	Get(tsNum TsNum, tx kv.Tx) (VVType, error)
	Put(tsNum TsNum, value VVType, tx kv.RwTx) error
	PutEntityEnd(startBaseTsNum TsNum, tsNum TsNum, tx kv.RwTx) error
}

// 1:many
type BindQueries interface {
	Get(tsNum TsNum, tx kv.Tx) (VVType, error)
	Put(tsId TsId, value VVType, tx kv.RwTx) error
	PutMaxTsNum(baseTsNum TsNum, maxTsNum TsNum, tx kv.RwTx) error
}

// canonicalTbl + valTbl
// headers, bodies, beaconblocks
type MarkedQueries interface {
	Get(tsNum TsNum, tx kv.Tx) (VVType, error)
	GetNc(tsNum TsNum, forkId []byte, tx kv.Tx) (VVType, error)
	Put(tsNum TsNum, forkId []byte, value VVType, tx kv.RwTx) error
}

type AppEnum string

const (
	Headers      AppEnum = "headers"
	Bodies       AppEnum = "bodies"
	Transactions AppEnum = "transactions"
)

type TemporalRwTx interface {
	kv.RwTx
	kv.TemporalTx
	AggTx(baseAppendable AppEnum) AggTx // gets aggtx for entity-set represented by baseAppendable
}

type AggTx interface {
	// pick out the right one; else runtime failure
	// user needs to be anyway aware of what set of queries
	// he can interact with. So is fine.
	RangedQueries(app AppEnum) RangedQueries
	PointQueries(app AppEnum) PointQueries
	MarkedQueries(app AppEnum) MarkedQueries
	BindQueries(app AppEnum) BindQueries
}

func WriteRawBody(db TemporalRwTx, hash common.Hash, number uint64, body *types.RawBody) error {
	baseTxnID, err := db.IncrementSequence(kv.EthTx, uint64(types.TxCountToTxAmount(len(body.Transactions))))
	if err != nil {
		return err
	}

	data := types.BodyForStorage{
		BaseTxnID:   types.BaseTxnID(baseTxnID),
		TxCount:     types.TxCountToTxAmount(len(body.Transactions)), /*system txs*/
		Uncles:      body.Uncles,
		Withdrawals: body.Withdrawals,
	}

	if err = WriteBodyForStorage(db, hash, number, &data); err != nil {
		return fmt.Errorf("WriteBodyForStorage: %w", err)
	}
	if err = WriteRawTransactions(db, body.Transactions, data.BaseTxnID.First()); err != nil {
		return fmt.Errorf("WriteRawTransactions: %w", err)
	}
	return nil
}

func WriteBodyForStorage(db TemporalRwTx, hash common.Hash, number uint64, body *types.BodyForStorage) error {
	aggTx := db.AggTx(Headers) // or temporalTx.AggTx(baseAppendableEnum); gives aggtx for entityset

	b := bytes.Buffer{}
	if err := body.EncodeRLP(&b); err != nil {
		panic(err)
	}

	markedQueries := aggTx.MarkedQueries(Bodies)

	// write bodies
	markedQueries.Put(TsNum(number), hash, b.Bytes(), db)
	return nil
}

func WriteRawTransactions(db TemporalRwTx, txs [][]byte, baseTxnID uint64) error {
	aggTx := db.AggTx(Headers) // or temporalTx.AggTx(baseAppendableEnum); gives aggtx for entityset
	stx := baseTxnID
	txq := aggTx.BindQueries(Transactions)

	for _, txn := range txs {
		txq.Put(TsId(stx), VVType(txn), db)
		stx++
	}
	return nil
}
