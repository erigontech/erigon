package appendables

import (
	"context"

	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon-lib/kv"
)

type ValueTransformer interface {
	Transform(sourceKey VKType, value VVType) (data VVType, shouldSkip bool, err error)
}

type BaseFreezer struct {
	gen     SourceKeyGenerator
	proc    ValueTransformer
	coll    Collector
	valsTbl string
}

// what does this do?
func (sf *BaseFreezer) Freeze(ctx context.Context, stepKeyFrom, stepKeyTo uint64, tx kv.Tx) (lastKeyValue uint64, err error) {
	can_stream := sf.gen.FromStepKey(stepKeyFrom, stepKeyTo, tx)
	for can_stream.HasNext() {
		key, err := can_stream.Next()
		if err != nil {
			return 0, err
		}

		key_bytes := hexutility.EncodeTs(uint64(key)) // check assumption: key_bytes is always big endian of uint64 key
		value, err := tx.GetOne(sf.valsTbl, key_bytes)
		if err != nil {
			return 0, err
		}

		data, shouldSkip, err := sf.proc.Transform(key, value)

		if err != nil {
			return 0, err
		}
		if shouldSkip {
			continue
		}
		if err := sf.coll(data); err != nil {
			return 0, err
		}
	}

	return 0, nil
}

func (sf *BaseFreezer) SetCollector(coll Collector) {
	sf.coll = coll
}

func (sf *BaseFreezer) SetSourceKeyGenerator(gen SourceKeyGenerator) {
	sf.gen = gen
}

func (sf *BaseFreezer) SetValueProcessor(proc ValueTransformer) {
	sf.proc = proc
}
