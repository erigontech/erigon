package appendables

import (
	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/stream"
	"github.com/tidwall/btree"
)

// appendables which don't store non-canonical data
// or which tsId = tsNum always
/// forget about blobs right now

// 1. a valsTable stores the value simply

type PlainAppendable struct {
	BaseAppendable[[]byte, []byte]
	valsTable string
}

func NewPlainAppendable(valsTable string) *PlainAppendable {
	ap := &PlainAppendable{
		valsTable: valsTable,
	}
	// ap.gen = &SpanSourceKeyGenerator{}
	ap.fet = &PlainFetcher{}
	ap.proc = &PlainProcessor{}
	ap.put = &PlainPutter{}
	// ap.canFreeze = &CanFreeze{}

	ap.freezer = &SimpleFreezer{
		//gen:
		fet:  ap.fet,
		proc: ap.proc,
		//config:
	}
	ap.rosnapshot = &RoSnapshots[*AppendableCollation]{
		enums: []ApEnum{BorSpans},
		dirty: map[ApEnum]*btree.BTreeG[*SDirtySegment]{},
		visible: map[ApEnum]SVisibleSegments{
			BorSpans: {},
		},
		freezers: map[ApEnum]Freezer[*AppendableCollation]{
			BorSpans: &CanFreeze{},
		},
	}
	ap.enum = BorSpans
	return ap
}

// two things to set
// 1. source key generator
// 2. Can freeze

type PlainPutter struct {
	valsTable string
}

func (p *PlainPutter) Put(tsId uint64, forkId []byte, value []byte, tx kv.RwTx) error {
	return tx.Put(p.valsTable, hexutility.EncodeTs(tsId), value)
}

type PlainProcessor struct{}

func (p *PlainProcessor) Process(sourceKey []byte, value []byte) (data []byte, shouldSkip bool, err error) {
	return value, false, nil
}

type PlainFetcher struct {
	valsTable string
}

func (f *PlainFetcher) GetValues(sourceKey []byte, tx kv.Tx) (value []byte, shouldSkip bool, found bool, err error) {
	found = true
	shouldSkip = false
	value, err = tx.GetOne(f.valsTable, sourceKey)
	if err != nil {
		return nil, false, false, err
	}
	return value, shouldSkip, found, nil
}

func NewSequentialStream(from uint64, to uint64) stream.Uno[[]byte] {
	return &SequentialStream{
		from:    from,
		to:      to,
		current: from,
	}
}
