package observer

import (
	"context"
	"testing"
	"time"

	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/ethconfig/estimate"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/assert"
)

func TestKeygen(t *testing.T) {
	targetKeyPair, err := crypto.GenerateKey()
	assert.NotNil(t, targetKeyPair)
	assert.Nil(t, err)

	targetKey := &targetKeyPair.PublicKey
	keys := keygen(context.Background(), targetKey, 50*time.Millisecond, uint(estimate.AlmostAllCPUs()), log.Root())

	assert.NotNil(t, keys)
	assert.GreaterOrEqual(t, len(keys), 4)
}
