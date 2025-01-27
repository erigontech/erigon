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

type Collector func(values []byte) error

// pattern is SetCollector ; and then call Freeze
type Freezer interface {
	// stepKeyFrom/To represent num which the snapshot should range
	// this doesn't check if the snapshot can be created or not. It's the responsibilty of the caller
	// to ensure this.
	Freeze(ctx context.Context, baseNumFrom, baseNumTo Num, tx kv.Tx) error
	SetCollector(coll Collector)
}

type Appendable interface {
	SetFreezer(Freezer)
	SetIndexBuilders([]AccessorIndexBuilder)
	DirtySegmentsMaxNum() Num
	VisibleSegmentsMaxNum() Num
	RecalcVisibleFiles(baseNumTo Num)
	Prune(ctx context.Context, baseNumTo Num, limit uint64, rwTx kv.RwTx) error
	Unwind(ctx context.Context, baseNumFrom Num, rwTx kv.RwTx) error
	// don't put BeginFilesRo here, since it returns PointAppendableRo, RangedAppendableRo etc. (each has different query patterns)
	// so anyway aggregator has to recover concrete type, and then it can
	// call BeginFilesRo on that
}

var (
	_ Appendable = &MarkedAppendable{}
	_ Appendable = &RelationalAppendable{}
	//_ MarkedQueries     = &MarkedAppendableRoTx{}
	//_ RelationalQueries = &RelationalAppendableRoTx{}
)

// canonicalTbl + valTbl
// headers, bodies, beaconblocks
type MarkedRoQueries interface {
	Get(num Num, tx kv.Tx) (VVType, error)                // db + snapshots
	GetNc(num Num, hash []byte, tx kv.Tx) (VVType, error) // db only
}

type MarkedRwQueries interface {
	MarkedRoQueries
	Put(num Num, hash []byte, value VVType, tx kv.RwTx) error
}

// in queries, it's eitther MarkedQueries (for marked appendables) or RelationalQueries
type RelationalRoQueries interface {
	Get(num Num, tx kv.Tx) (VVType, error) // db + snapshots
	GetNc(id Id, tx kv.Tx) (VVType, error) // db only
}

type RelationalRwQueries interface {
	RelationalRoQueries
	Put(id Id, value VVType, tx kv.RwTx) error
}

type AppEnum string

const (
	Headers      AppEnum = "headers"
	Bodies       AppEnum = "bodies"
	Transactions AppEnum = "transactions"
)

// ro
type TemporalTx interface {
	kv.TemporalTx
	AggRoTx(baseAppendable AppEnum) AggTx[MarkedRoQueries, RelationalRoQueries]
}

type TemporalRwTx interface {
	kv.RwTx
	TemporalTx
	AggRwTx(baseAppendable AppEnum) AggTx[MarkedRwQueries, RelationalRwQueries] // gets aggtx for entity-set represented by baseAppendable
}

type AggTx[R1 MarkedRoQueries, R2 RelationalRoQueries] interface {
	// pick out the right one; else runtime failure
	// user needs to be anyway aware of what set of queries
	// he can interact with. So is fine.
	RelationalQueries(app AppEnum) R2
	MarkedQueries(app AppEnum) R1
}

func WriteRawBody(tx TemporalRwTx, hash common.Hash, number uint64, body *types.RawBody) error {
	baseTxnID, err := tx.IncrementSequence(kv.EthTx, uint64(types.TxCountToTxAmount(len(body.Transactions))))
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
	aggTx := tx.AggRwTx(Headers) // or temporalTx.AggTx(baseAppendableEnum); gives aggtx for entityset

	b := bytes.Buffer{}
	if err := body.EncodeRLP(&b); err != nil {
		panic(err)
	}

	markedQueries := aggTx.MarkedQueries(Bodies)

	// write bodies
	return markedQueries.Put(Num(number), hash.Bytes(), b.Bytes(), tx)
}

func WriteRawTransactions(tx TemporalRwTx, txs [][]byte, baseTxnID uint64) error {
	aggTx := tx.AggRwTx(Headers) // or temporalTx.AggTx(baseAppendableEnum); gives aggtx for entityset
	stx := baseTxnID
	txq := aggTx.RelationalQueries(Transactions)

	for _, txn := range txs {
		txq.Put(Id(stx), VVType(txn), tx)
		stx++
	}
	return nil
}
