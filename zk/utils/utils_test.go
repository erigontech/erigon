package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ledgerwatch/erigon/zk/constants"
)

type SimpleForkReader struct {
	BatchForks   map[constants.ForkId]uint64
	LowestBlocks map[uint64]uint64
}

func (s *SimpleForkReader) GetLowestBatchByFork(forkId uint64) (uint64, error) {
	found, _ := s.BatchForks[constants.ForkId(forkId)]
	return found, nil
}

func (s *SimpleForkReader) GetLowestBlockInBatch(batchNo uint64) (uint64, bool, error) {
	found, ok := s.LowestBlocks[batchNo]
	return found, ok, nil
}

type TestConfig struct {
	setCalls map[constants.ForkId]uint64
}

func NewTestConfig() *TestConfig {
	return &TestConfig{
		setCalls: make(map[constants.ForkId]uint64),
	}
}

func (tc *TestConfig) SetForkIdBlock(forkId constants.ForkId, blockNum uint64) error {
	tc.setCalls[forkId] = blockNum
	return nil
}

type testScenario struct {
	name          string
	batchForks    map[constants.ForkId]uint64
	lowestBlocks  map[uint64]uint64
	expectedCalls map[constants.ForkId]uint64
}

func TestUpdateZkEVMBlockCfg(t *testing.T) {
	scenarios := []testScenario{
		{
			name: "HigherForkEnabled",
			batchForks: map[constants.ForkId]uint64{
				constants.ForkID9Elderberry2: 900,
			},
			lowestBlocks: map[uint64]uint64{
				900: 900,
			},
			expectedCalls: map[constants.ForkId]uint64{
				constants.ForkID9Elderberry2: 900,
				constants.ForkID8Elderberry:  900,
				constants.ForkID7Etrog:       900,
				constants.ForkID6IncaBerry:   900,
				constants.ForkID5Dragonfruit: 900,
				constants.ForkID4:            900,
			},
		},
		{
			name: "MiddleForksExplicitlyEnabled",
			batchForks: map[constants.ForkId]uint64{
				constants.ForkID7Etrog:     700,
				constants.ForkID6IncaBerry: 600,
			},
			lowestBlocks: map[uint64]uint64{
				700: 700,
				600: 600,
			},
			expectedCalls: map[constants.ForkId]uint64{
				constants.ForkID7Etrog:       700,
				constants.ForkID6IncaBerry:   600,
				constants.ForkID5Dragonfruit: 600,
				constants.ForkID4:            600,
			},
		},
		{
			name: "MissingEnablements",
			batchForks: map[constants.ForkId]uint64{
				constants.ForkID4:          100,
				constants.ForkID6IncaBerry: 600,
			},
			lowestBlocks: map[uint64]uint64{
				100: 100,
				600: 600,
			},
			expectedCalls: map[constants.ForkId]uint64{
				constants.ForkID6IncaBerry:   600,
				constants.ForkID5Dragonfruit: 600,
				constants.ForkID4:            100,
			},
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			cfg := NewTestConfig()
			reader := &SimpleForkReader{BatchForks: scenario.batchForks, LowestBlocks: scenario.lowestBlocks}

			err := UpdateZkEVMBlockCfg(cfg, reader, "TestPrefix")
			assert.NoError(t, err, "should not return an error")

			assert.Equal(t, scenario.expectedCalls, cfg.setCalls, "SetForkIdBlock calls mismatch")
		})
	}
}
