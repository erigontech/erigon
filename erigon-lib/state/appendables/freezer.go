package appendables

import (
	"context"

	"github.com/erigontech/erigon-lib/kv"
)

type SimpleFreezer struct {
	gen    SourceKeyGenerator[[]byte]
	fet    ValueFetcher[[]byte, []byte]
	proc   ValueProcessor[[]byte, []byte]
	config *SnapshotConfig
	coll   Collector
}

// what does this do?
func (sf *SimpleFreezer) Freeze(ctx context.Context, stepKeyFrom, stepKeyTo uint64, tx kv.Tx) (lastKeyValue uint64, err error) {
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
func (sf *SimpleFreezer) GetCompressorWorkers() uint64 {
	return 0
}

// SetCompressorWorkers(uint64)
func (sf *SimpleFreezer) SetCompressorWorkers(uint64) {}

func (sf *SimpleFreezer) SetCollector(coll Collector) {
	sf.coll = coll
}
