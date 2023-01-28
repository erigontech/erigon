package commands

import (
	"context"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test case for https://github.com/ethereum/execution-apis/pull/217 responses
func TestZeroLatestValidHash(t *testing.T) {
	payloadStatus := remote.EnginePayloadStatus{Status: remote.EngineStatus_INVALID, LatestValidHash: gointerfaces.ConvertHashToH256(common.Hash{})}
	json, err := convertPayloadStatus(context.TODO(), nil, &payloadStatus)
	require.NoError(t, err)
	assert.Equal(t, "INVALID", json["status"])
	assert.Equal(t, common.Hash{}, json["latestValidHash"])
}
