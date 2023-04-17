package jsonrpc

import (
	"encoding/json"
	"testing"

	"github.com/0xPolygonHermez/zkevm-node"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClientVersion(t *testing.T) {
	s, _, _ := newSequencerMockedServer(t)
	defer s.Stop()

	res, err := s.JSONRPCCall("web3_clientVersion")
	require.NoError(t, err)

	assert.Equal(t, float64(1), res.ID)
	assert.Equal(t, "2.0", res.JSONRPC)
	assert.Nil(t, res.Error)

	var result string
	err = json.Unmarshal(res.Result, &result)
	require.NoError(t, err)

	assert.Equal(t, zkevm.Version, result)
}

func TestSha3(t *testing.T) {
	s, _, _ := newSequencerMockedServer(t)
	defer s.Stop()

	res, err := s.JSONRPCCall("web3_sha3", "0x68656c6c6f20776f726c64")
	require.NoError(t, err)

	assert.Equal(t, float64(1), res.ID)
	assert.Equal(t, "2.0", res.JSONRPC)
	assert.Nil(t, res.Error)

	var result string
	err = json.Unmarshal(res.Result, &result)
	require.NoError(t, err)

	assert.Equal(t, "0x47173285a8d7341e5e972fc677286384f802f8ef42a5ec5f03bbfa254cb01fad", result)
}
