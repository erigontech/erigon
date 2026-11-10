// Copyright 2026 The Erigon Authors
// This file is part of Erigon.

package engineapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/execution/abi/bind"
	"github.com/erigontech/erigon/execution/engineapi"
	enginetypes "github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/engineapi/engineapitester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/state/contracts"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/jsonrpc"
)

func newPayloadWitnessTester(t *testing.T, args engineapitester.EngineApiTesterInitArgs) engineapitester.EngineApiTester {
	genesis, coinbaseKey, err := engineapitester.DefaultEngineApiTesterGenesis()
	require.NoError(t, err)
	args.Logger = testlog.Logger(t, log.LvlDebug)
	args.DataDir = t.TempDir()
	args.Genesis = genesis
	args.CoinbaseKey = coinbaseKey
	eat, err := engineapitester.InitialiseEngineApiTester(t.Context(), args)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, eat.Close()) })
	return eat
}

// buildChangerPayload returns an unimported payload that calls a contract deployed in its parent.
func buildChangerPayload(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) (*engineapitester.MockClPayload, common.Address) {
	transactOpts, err := bind.NewKeyedTransactorWithChainID(eat.CoinbaseKey, eat.ChainId())
	require.NoError(t, err)
	transactOpts.GasLimit = params.MaxTxnGasLimit
	changerAddr, txn, changer, err := contracts.DeployChanger(transactOpts, eat.ContractBackend)
	require.NoError(t, err)
	parent, err := eat.MockCl.BuildCanonicalBlock(ctx)
	require.NoError(t, err)
	require.NoError(t, eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, parent.ExecutionPayload, txn.Hash()))
	_, err = changer.Change(transactOpts)
	require.NoError(t, err)
	payload, err := eat.MockCl.BuildNewPayload(ctx)
	require.NoError(t, err)
	require.Len(t, payload.ExecutionPayload.Transactions, 1)
	return payload, changerAddr
}

func postPayloadWitness(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester, p *engineapitester.MockClPayload) (int, []byte) {
	payload := *p.ExecutionPayload
	payload.SSZVersion = clparams.GloasVersion
	body, err := engineapi.EncodeNewPayloadEnvelope(clparams.GloasVersion, &payload, *p.ParentBeaconBlockRoot, p.ExecutionRequests)
	require.NoError(t, err)
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"iat": time.Now().Unix()}).SignedString(eat.JwtSecret)
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, eat.EngineApiUrl+"/engine/v1/payloads/witness", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Eth-Execution-Version", "amsterdam")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, out
}

func requirePayloadWitness(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester, p *engineapitester.MockClPayload) *jsonrpc.ExecutionWitnessResult {
	code, body := postPayloadWitness(ctx, t, eat, p)
	require.Equal(t, http.StatusOK, code, string(body))
	status, witness, err := engineapi.DecodePayloadStatusWithWitness(body)
	require.NoError(t, err)
	require.Equal(t, enginetypes.ValidStatus, status.Status, status.ValidationError)
	require.NotNil(t, witness)
	return witness
}

// The reference is debug_executionWitness in canonical mode, rebuilt from commitment
// history after the payload is committed.
func TestEngineApiPayloadWitness(t *testing.T) {
	eat := newPayloadWitnessTester(t, engineapitester.EngineApiTesterInitArgs{
		EthConfigTweaker: func(cfg *ethconfig.Config) { cfg.KeepExecutionProofs = true },
		EnableDebugAPI:   true,
	})
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		payload, changerAddr := buildChangerPayload(ctx, t, eat)

		witness := requirePayloadWitness(ctx, t, eat, payload)

		code, err := eat.ContractBackend.CodeAt(ctx, changerAddr, nil)
		require.NoError(t, err)
		require.Contains(t, witness.Codes, hexutil.Bytes(code))
		require.NotEmpty(t, witness.Headers)
		var parent types.Header
		require.NoError(t, rlp.DecodeBytes(witness.Headers[len(witness.Headers)-1], &parent))
		require.Equal(t, payload.ExecutionPayload.ParentHash, parent.Hash())

		require.NoError(t, eat.MockCl.UpdateForkChoice(ctx, payload))
		client, err := rpc.DialContext(ctx, eat.JsonRpcUrl, log.Root())
		require.NoError(t, err)
		defer client.Close()
		var want jsonrpc.ExecutionWitnessResult
		require.NoError(t, client.CallContext(ctx, &want, "debug_executionWitness", payload.ExecutionPayload.BlockHash, "canonical"))
		require.Equal(t, want.State, witness.State)
		require.Equal(t, want.Codes, witness.Codes)
		require.Equal(t, want.Headers, witness.Headers)
	})
}

func TestEngineApiPayloadWitnessForKnownPayload(t *testing.T) {
	eat := newPayloadWitnessTester(t, engineapitester.EngineApiTesterInitArgs{})
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		payload, _ := buildChangerPayload(ctx, t, eat)
		witness := requirePayloadWitness(ctx, t, eat, payload)
		require.NoError(t, eat.MockCl.UpdateForkChoice(ctx, payload))

		require.Equal(t, witness, requirePayloadWitness(ctx, t, eat, payload))
	})
}

// A payload imported without a witness request leaves no state to build its witness
// from once a forkchoice update commits it.
func TestEngineApiPayloadWitnessUnavailable(t *testing.T) {
	eat := newPayloadWitnessTester(t, engineapitester.EngineApiTesterInitArgs{})
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		payload, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)

		code, body := postPayloadWitness(ctx, t, eat, payload)

		require.Equal(t, http.StatusInternalServerError, code, string(body))
		var problem struct {
			Type string `json:"type"`
		}
		require.NoError(t, json.Unmarshal(body, &problem))
		require.Equal(t, "/engine-api/errors/internal", problem.Type)
	})
}
