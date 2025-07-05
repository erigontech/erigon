package state

import (
	"github.com/erigontech/erigon-lib/config3"
)

var e3MergeStages = []uint64{2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768}
var e3MergeStagesLimited = []uint64{2, 4, 8, 16, 32, 64}

func E3SnapCreationConfig(limited bool) SnapshotCreationConfig {
	stepSize := uint64(config3.DefaultStepSize)
	var mergeStages []uint64
	if limited {
		mergeStages = e3MergeStagesLimited
	} else {
		mergeStages = e3MergeStages
	}
	for i := range mergeStages {
		mergeStages[i] *= stepSize
	}
	return SnapshotCreationConfig{
		RootNumPerStep: stepSize,
		SafetyMargin:   0,
		MergeStages:    mergeStages,
		MinimumSize:    stepSize,
	}
}

func E3SnapshotConfigII(cfg iiCfg) SnapshotConfig {
	scfg := E3SnapCreationConfig(false)
	return SnapshotConfig{
		SnapshotCreationConfig: &scfg,
		RootAligned:            true,
		Integrity:              nil,
		Schema:                 E3SnapSchemaForII(cfg),
	}
}

func E3SnapSchemaForII(cfg iiCfg) SnapNameSchema {
	b := NewE3SnapSchemaBuilder(cfg.Accessors, config3.DefaultStepSize).
		Data(cfg.dirs.SnapIdx, cfg.filenameBase, DataExtensionEf, cfg.Compression).
		Accessor(cfg.dirs.SnapAccessors)
	if cfg.Accessors.Has(AccessorExistence) {
		b.Existence()
	}
	return b.Build()
}
