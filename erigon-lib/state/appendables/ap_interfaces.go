package appendables

import (
	"bytes"
	"context"
	"fmt"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/core/types"
)

// --------------------------------------------------------------------

// vals table k,v types
type VKType []byte
type VVType []byte
type Num uint64
type Id uint64
type BaseNum uint64

type Collector func(values []byte) error

// pattern is SetCollector ; and then call Freeze
type Freezer interface {
	// baseNumFrom/To represent num which the snapshot should range
	// this doesn't check if the snapshot can be created or not. It's the responsibilty of the caller
	// to ensure this.
	Freeze(ctx context.Context, from, to BaseNum, tx kv.Tx) error
	SetCollector(coll Collector)
}

// primary key : num (or canonical entity identifier)
// secondary key : baseNum (this is a secondary key, which is used as sharding key for the entity snapshots + have some apis be specified in terms of baseNum
//
//	like prune/unwind/freeze)
type Appendable interface {
	// might have more methods...
	SetFreezer(Freezer)
	SetIndexBuilders(...AccessorIndexBuilder)
	DirtySegmentsMaxNum() Num
	VisibleSegmentsMaxNum() Num
	DirtySegmentsMaxBaseNum() BaseNum
	VisibleSegmentsMaxBaseNum() BaseNum
	RecalcVisibleFiles(to BaseNum)
	// don't put BeginFilesRo here, since it returns different kinds of Ro etc. (each has different query patterns)
	// so anyway aggregator has to recover concrete type, and then it can
	// call BeginFilesRo on that
}

var (
	_ Appendable = &MarkedAppendable{}
	_ Appendable = &RelationalAppendable{}
	//_ MarkedQueries     = &MarkedAppendableRoTx{}
	// _ RelationalRoQueries = &RelationalAppendableRoTx{}
	// _ RelationalRwQueries = &RelationalAppendableRwTx{}
)

type AppEnum string

const (
	Headers      AppEnum = "headers"
	Bodies       AppEnum = "bodies"
	Transactions AppEnum = "transactions"
)

// Marked(enum) gives MarkedAppendableTx -- use it to put/append/read etc.
// for write ops like put, temporalrwtx must be used.
// temporaldb holds multiple aggregators, and appendables are under those.
// so tx.Marked(enum) search and find the right MarkedAppendableTx.
// same thing applicable for RelationalAppendableTx
type TemporalTx interface {
	kv.TemporalTx
	Marked(app AppEnum) *MarkedAppendableTx         // read/write queries for marked appendable (write query only when TemporalRwTx is used)
	Relational(app AppEnum) *RelationalAppendableTx // read/write queries for relational appendable (write query only when TemporalRwTx is used)
	AggRoTx(baseAppendable AppEnum) *AggregatorRoTx // aggregate queries like prune/unwind
}

type TemporalRwTx interface {
	kv.RwTx
	TemporalTx
}

// use Marked(enum) or Relational(enum) to get the right ro/rw tx.
// each enum is either marked or relational, so must choose the right one
// else code should panic.
type AggregatorRoTx struct{}

func (a *AggregatorRoTx) Marked(app AppEnum) *MarkedAppendableTx         { return nil }
func (a *AggregatorRoTx) Relational(app AppEnum) *RelationalAppendableTx { return nil }

type Aggregator struct{}

func (a *Aggregator) ViewSingleFile(ap AppEnum, num BaseNum) (segment *VisibleSegment, ok bool, close func()) {
	// TODO
	// similar to RoSnapshots#ViewSingleFile etc.
	return nil, false, nil
}

// /
func WriteRawBody(tx TemporalRwTx, hash common.Hash, number uint64, body *types.RawBody) error {
	txq := tx.Relational(Transactions)
	baseTxnID, err := txq.IncrementSequence(uint64(types.TxCountToTxAmount(len(body.Transactions))), tx)
	if err != nil {
		return err
	}

	data := types.BodyForStorage{
		BaseTxnID:   types.BaseTxnID(baseTxnID),
		TxCount:     types.TxCountToTxAmount(len(body.Transactions)), /*system txs*/
		Uncles:      body.Uncles,
		Withdrawals: body.Withdrawals,
	}

	if err = WriteBodyForStorage(tx, hash, number, &data); err != nil {
		return fmt.Errorf("WriteBodyForStorage: %w", err)
	}
	if err = WriteRawTransactions(tx, body.Transactions, data.BaseTxnID.First()); err != nil {
		return fmt.Errorf("WriteRawTransactions: %w", err)
	}
	return nil
}

func WriteBodyForStorage(tx TemporalRwTx, hash common.Hash, number uint64, body *types.BodyForStorage) error {
	b := bytes.Buffer{}
	if err := body.EncodeRLP(&b); err != nil {
		panic(err)
	}

	markedTx := tx.Marked(Bodies)
	// write bodies
	return markedTx.Put(Num(number), hash.Bytes(), b.Bytes(), tx)
}

func WriteRawTransactions(tx TemporalRwTx, txs [][]byte, baseTxnID uint64) error {
	txq := tx.Relational(Transactions)
	stx := baseTxnID

	for _, txn := range txs {
		txq.Append(Id(stx), VVType(txn), tx)
		stx++
	}
	return nil
}

// graveyard

// // canonicalTbl + valTbl
// // headers, bodies, beaconblocks
// type maintenanceQueries interface {
// 	Prune(ctx context.Context, baseKeyTo Num, limit uint64, rwTx kv.RwTx) error
// 	Unwind(ctx context.Context, baseKeyFrom Num, limit uint64, rwTx kv.RwTx) error
// }

// // the following queries are satisfied by RelationalAppendableRoTx, RelationalAppendableRwTx
// // similarly, MarkedAppendableRoTx, MarkedAppendableRwTx

// type MarkedRoQueries interface {
// 	Get(num Num, tx kv.Tx) (VVType, error)                // db + snapshots
// 	GetNc(num Num, hash []byte, tx kv.Tx) (VVType, error) // db only
// }

// type MarkedRwQueries interface {
// 	maintenanceQueries
// 	MarkedRoQueries
// 	Put(num Num, hash []byte, value VVType, tx kv.RwTx) error
// }

// // in queries, it's eitther MarkedQueries (for marked appendables) or RelationalQueries
// type RelationalRoQueries interface {
// 	Get(num Num, tx kv.Tx) (VVType, error) // db + snapshots
// 	GetNc(id Id, tx kv.Tx) (VVType, error) // db only
// }

// type RelationalRwQueries interface {
// 	maintenanceQueries
// 	RelationalRoQueries
// 	Put(id Id, value VVType, tx kv.RwTx) error
// }
// type AggTx[R1 MarkedRoQueries, R2 RelationalRoQueries] interface {
// 	// pick out the right one; else runtime failure
// 	// user needs to be anyway aware of what set of queries
// 	// he can interact with. So is fine.
// 	RelationalQueries(app AppEnum) R2
// 	MarkedQueries(app AppEnum) R1

// 	// more methods on level of aggtx
// }

// type AggregatorRwTx struct {
// 	*AggregatorRoTx
// }

// func (a *AggregatorRwTx) Marked(app AppEnum) *MarkedAppendableRwTx         { return nil }
// func (a *AggregatorRwTx) Relational(app AppEnum) *RelationalAppendableRwTx { return nil }
