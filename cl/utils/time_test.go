package utils_test

import (
	"testing"
	"time"

	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/stretchr/testify/assert"
)

func TestGetCurrentSlot(t *testing.T) {
	now := uint64(time.Now().Unix())
	timePerSlot := 12
	genesisTime := now - 12*10
	assert.Equal(t, utils.GetCurrentSlot(genesisTime, uint64(timePerSlot)), uint64(10))
}

func TestGetCurrentEpoch(t *testing.T) {
	now := uint64(time.Now().Unix())
	timePerSlot := 12
	genesisTime := now - 86*10
	assert.Equal(t, utils.GetCurrentEpoch(genesisTime, uint64(timePerSlot), 32), uint64(2))
}

func TestSlotToPeriod(t *testing.T) {
	assert.Equal(t, utils.SlotToPeriod(20000), uint64(2))
}
