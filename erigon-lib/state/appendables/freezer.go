package appendables

import (
	"context"

	"github.com/erigontech/erigon-lib/kv"
)

type ValueProcessor interface {
	Process(sourceKey VKType, value VVType) (data VVType, shouldSkip bool, err error)
}

type BaseFreezer struct {
	gen  SourceKeyGenerator
	fet  ValueFetcher
	proc ValueProcessor
	coll Collector
}

// what does this do?
func (sf *BaseFreezer) Freeze(ctx context.Context, stepKeyFrom, stepKeyTo uint64, tx kv.Tx) (lastKeyValue uint64, err error) {
	can_stream := sf.gen.FromStepKey(stepKeyFrom, stepKeyTo, tx)
	for can_stream.HasNext() {
		key, err := can_stream.Next()
		if err != nil {
			return 0, err
		}

		value, shouldSkip, _, err := sf.fet.GetValues(key, tx)
		if err != nil {
			return 0, err
		}
		if shouldSkip {
			continue
		}

		data, shouldSkip, err := sf.proc.Process(key, value)

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

// GetCompressorWorkers() uint64
func (sf *BaseFreezer) GetCompressorWorkers() uint64 {
	return 0
}

// SetCompressorWorkers(uint64)
func (sf *BaseFreezer) SetCompressorWorkers(uint64) {}

func (sf *BaseFreezer) SetCollector(coll Collector) {
	sf.coll = coll
}

func (sf *BaseFreezer) SetSourceKeyGenerator(gen SourceKeyGenerator) {
	sf.gen = gen
}

func (sf *BaseFreezer) SetValueFetcher(fet ValueFetcher) {
	sf.fet = fet
}

func (sf *BaseFreezer) SetValueProcessor(proc ValueProcessor) {
	sf.proc = proc
}
