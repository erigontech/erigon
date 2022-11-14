package commands

import (
	"testing"

	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon/common"
	"github.com/stretchr/testify/assert"
)

// Test case for https://github.com/ethereum/execution-apis/pull/217 responses
func TestZeroLatestValidHash(t *testing.T) {
	payloadStatus := remote.EnginePayloadStatus{Status: remote.EngineStatus_INVALID, LatestValidHash: gointerfaces.ConvertHashToH256(common.Hash{})}
	json := convertPayloadStatus(&payloadStatus)
	assert.Equal(t, "INVALID", json["status"])
	assert.Equal(t, common.Hash{}, json["latestValidHash"])
}
