// Copyright 2026 The Erigon Authors
// This file is part of Erigon.

package engineapi_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/abi/bind"
	"github.com/erigontech/erigon/execution/commitment"
	witnesstypes "github.com/erigontech/erigon/execution/commitment/witness"
	"github.com/erigontech/erigon/execution/engineapi"
	enginetypes "github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/engineapi/engineapitester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/protocol/rules/merge"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/state/contracts"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/jsonrpc"
)

func newPayloadWitnessTester(t *testing.T, args engineapitester.EngineApiTesterInitArgs) engineapitester.EngineApiTester {
	return newPayloadWitnessTesterWithGenesis(t, args, func(*types.Genesis) {})
}

func newPayloadWitnessTesterWithGenesis(t *testing.T, args engineapitester.EngineApiTesterInitArgs, tweak func(*types.Genesis)) engineapitester.EngineApiTester {
	genesis, coinbaseKey, err := engineapitester.DefaultEngineApiTesterGenesis()
	require.NoError(t, err)
	tweak(genesis)
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
	return postForkPayloadWitness(ctx, t, eat, p, clparams.GloasVersion, "amsterdam", nil)
}

func postForkPayloadWitness(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester, p *engineapitester.MockClPayload, version clparams.StateVersion, fork string, inclusionList []hexutil.Bytes) (int, []byte) {
	payload := *p.ExecutionPayload
	payload.SSZVersion = version
	body, err := engineapi.EncodeNewPayloadEnvelope(version, &payload, *p.ParentBeaconBlockRoot, p.ExecutionRequests, inclusionList)
	require.NoError(t, err)
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"iat": time.Now().Unix()}).SignedString(eat.JwtSecret)
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, eat.EngineApiUrl+"/engine/v1/payloads/witness", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Eth-Execution-Version", fork)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, out
}

func requirePayloadWitness(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester, p *engineapitester.MockClPayload) *jsonrpc.ExecutionWitnessResult {
	code, body := postPayloadWitness(ctx, t, eat, p)
	_, witness := requireValidWithWitness(t, code, body, clparams.GloasVersion)
	requireEngineCommitmentVariant(t, eat, statecfg.ExperimentalCommitmentV3)
	requirePayloadWitnessParentRoot(ctx, t, eat, p, witness)
	return witness
}

func requireValidWithWitness(t *testing.T, code int, body []byte, version clparams.StateVersion) (*enginetypes.PayloadStatus, *jsonrpc.ExecutionWitnessResult) {
	t.Helper()
	require.Equal(t, http.StatusOK, code, string(body))
	status, witness, err := engineapi.DecodePayloadStatusWithWitness(body, version)
	require.NoError(t, err)
	require.Equal(t, enginetypes.ValidStatus, status.Status, status.ValidationError)
	require.NotNil(t, witness)
	return status, witness
}

func requirePayloadWitnessParentRoot(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester, p *engineapitester.MockClPayload, witness *jsonrpc.ExecutionWitnessResult) {
	t.Helper()
	parentHash := p.ExecutionPayload.ParentHash
	parent, err := eat.ExecutionModule.GetHeader(ctx, &parentHash, nil)
	require.NoError(t, err)
	require.NotNil(t, parent)
	txns := make([][]byte, len(p.ExecutionPayload.Transactions))
	for i, tx := range p.ExecutionPayload.Transactions {
		txns[i] = tx
	}
	transactions, err := types.DecodeTransactions(txns)
	require.NoError(t, err)
	header := engineapitester.MockClPayloadToHeader(p)
	signer := types.MakeSigner(eat.ChainConfig, header.Number.Uint64(), header.Time)
	for _, txn := range transactions {
		sender, err := txn.Sender(*signer)
		require.NoError(t, err)
		proof, account, err := payloadWitnessAccountProof(parent, witness, sender.Value())
		require.NoError(t, err)
		require.NotEmpty(t, proof)
		require.NotEmpty(t, account)
	}
	block := types.NewBlockFromNetwork(header, &types.Body{Transactions: transactions, Withdrawals: p.ExecutionPayload.Withdrawals}, nil)
	root, err := jsonrpc.ReplayExecutionWitness(witness, block, eat.ChainConfig, merge.New(ethash.NewFaker()))
	require.NoError(t, err)
	require.Equal(t, p.ExecutionPayload.StateRoot, root, "witness post-state root")
}

func payloadWitnessAccountProof(parent *types.Header, witness *jsonrpc.ExecutionWitnessResult, addr common.Address) ([][]byte, []byte, error) {
	nodes := make(map[string][]byte, len(witness.State))
	for _, node := range witness.State {
		nodes[string(crypto.Keccak256(node))] = node
	}
	return witnesstypes.ProofFromNodes(nodes, parent.Root[:], crypto.Keccak256(addr[:]))
}

func TestEngineWitnessChecksRejectMissingSender(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(true))
	eat := newPayloadWitnessTester(t, engineapitester.EngineApiTesterInitArgs{})
	ctx := t.Context()
	payload := buildCommittedChangerPayload(ctx, t, eat)
	code, body := postPayloadWitness(ctx, t, eat, payload)
	require.Equal(t, http.StatusOK, code, string(body))
	_, witness, err := engineapi.DecodePayloadStatusWithWitness(body, clparams.GloasVersion)
	require.NoError(t, err)
	parent, err := eat.ExecutionModule.GetHeader(ctx, &payload.ExecutionPayload.ParentHash, nil)
	require.NoError(t, err)
	require.NotNil(t, parent)
	sender := crypto.PubkeyToAddress(eat.CoinbaseKey.PublicKey)
	proof, account, err := payloadWitnessAccountProof(parent, witness, sender)
	require.NoError(t, err)
	require.NotEmpty(t, account)
	require.Greater(t, len(proof), 1)
	leaf := proof[len(proof)-1]
	index := bytes.Index(body, leaf)
	require.NotEqual(t, -1, index)
	require.Equal(t, 1, bytes.Count(body, leaf))
	body[index+len(leaf)-1] ^= 1
	_, changedWitness, err := engineapi.DecodePayloadStatusWithWitness(body, clparams.GloasVersion)
	require.NoError(t, err)
	_, _, err = payloadWitnessAccountProof(parent, changedWitness, sender)
	require.Error(t, err, "changed payload response must fail the sender proof check")
}

func requireEngineCommitmentVariant(t *testing.T, eat engineapitester.EngineApiTester, wantV3 bool) {
	t.Helper()
	tx, err := eat.ChainDB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	domains, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	wantVariant := commitment.VariantHexPatriciaTrie
	wantKey := commitment.KeyCommitmentState
	otherKey := commitment.KeyCommitmentV3State
	if wantV3 {
		wantVariant = commitment.VariantCommitmentV3
		wantKey, otherKey = otherKey, wantKey
	}
	require.Equal(t, wantVariant, domains.GetCommitmentCtx().Trie().Variant())
	state, _, err := tx.GetLatest(kv.CommitmentDomain, wantKey, kv.GetLatestOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, state)
	otherState, _, err := tx.GetLatest(kv.CommitmentDomain, otherKey, kv.GetLatestOptions{})
	require.NoError(t, err)
	require.Empty(t, otherState)
	if wantV3 {
		_, _, _, err = commitment.DecodeCommitmentV3State(state)
		require.NoError(t, err)
	}
}

func runCommitmentVariants(t *testing.T, test func(*testing.T)) {
	t.Helper()
	for _, tc := range []struct {
		name string
		v3   bool
	}{
		{name: "v3-off", v3: false},
		{name: "v3-on", v3: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Cleanup(statecfg.SetCommitmentV3(tc.v3))
			test(t)
		})
	}
}

// The reference is debug_executionWitness in canonical mode, rebuilt from commitment
// history after the payload is committed.
func TestEngineApiPayloadWitness(t *testing.T) {
	runCommitmentVariants(t, testEngineApiPayloadWitness)
}

func testEngineApiPayloadWitness(t *testing.T) {
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
		requireDebugExecutionWitness(ctx, t, eat, payload, witness)
	})
}

func requireDebugExecutionWitness(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester, p *engineapitester.MockClPayload, witness *jsonrpc.ExecutionWitnessResult) {
	client, err := rpc.DialContext(ctx, eat.JsonRpcUrl, log.Root())
	require.NoError(t, err)
	defer client.Close()
	var want jsonrpc.ExecutionWitnessResult
	require.NoError(t, client.CallContext(ctx, &want, "debug_executionWitness", p.ExecutionPayload.BlockHash, "canonical"))
	require.Equal(t, want.State, witness.State)
	require.Equal(t, want.Codes, witness.Codes)
	require.Equal(t, want.Headers, witness.Headers)
}

// A payload validated by a plain newPayload gets its witness from the held state when it is resubmitted before
// the forkchoice update.
func TestEngineApiPayloadWitnessForValidatedPayload(t *testing.T) {
	runCommitmentVariants(t, testEngineApiPayloadWitnessForValidatedPayload)
}

func testEngineApiPayloadWitnessForValidatedPayload(t *testing.T) {
	eat := newPayloadWitnessTester(t, engineapitester.EngineApiTesterInitArgs{
		EthConfigTweaker: func(cfg *ethconfig.Config) { cfg.KeepExecutionProofs = true },
		EnableDebugAPI:   true,
	})
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		payload, _ := buildChangerPayload(ctx, t, eat)
		status, err := eat.MockCl.InsertNewPayload(ctx, payload)
		require.NoError(t, err)
		require.Equal(t, enginetypes.ValidStatus, status.Status)

		witness := requirePayloadWitness(ctx, t, eat, payload)
		require.NoError(t, eat.MockCl.UpdateForkChoice(ctx, payload))
		requireDebugExecutionWitness(ctx, t, eat, payload, witness)
	})
}

// A validated Bogota payload resubmitted with a new inclusion list gets that list checked and its witness, while the
// fork validator still retains its state.
func TestEngineApiPayloadWitnessChecksNewInclusionListOfValidatedPayload(t *testing.T) {
	eat := newPayloadWitnessTesterWithGenesis(t, engineapitester.EngineApiTesterInitArgs{}, func(genesis *types.Genesis) {
		genesis.Config.BogotaTime = common.NewUint64(0)
	})
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		payload, _ := buildChangerPayload(ctx, t, eat)
		status, err := eat.MockCl.InsertNewPayload(ctx, payload)
		require.NoError(t, err)
		require.Equal(t, enginetypes.ValidStatus, status.Status)

		unfundedKey, err := crypto.GenerateKey()
		require.NoError(t, err)
		txn, err := types.SignTx(
			types.NewTransaction(0, common.Address{1}, uint256.NewInt(1), params.TxGas, uint256.NewInt(common.GWei), nil),
			*types.LatestSignerForChainID(eat.ChainId()),
			unfundedKey,
		)
		require.NoError(t, err)
		var encoded bytes.Buffer
		require.NoError(t, txn.MarshalBinary(&encoded))

		code, body := postForkPayloadWitness(ctx, t, eat, payload, clparams.HezeVersion, "bogota", []hexutil.Bytes{encoded.Bytes()})
		status, witness := requireValidWithWitness(t, code, body, clparams.HezeVersion)
		require.NotNil(t, status.InclusionListSatisfied)
		require.NotEmpty(t, witness.State)
	})
}

// buildCommittedChangerPayload returns a committed payload that deploys a contract, with a committed block on top
// that calls it from the same sender.
func buildCommittedChangerPayload(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) *engineapitester.MockClPayload {
	transactOpts, err := bind.NewKeyedTransactorWithChainID(eat.CoinbaseKey, eat.ChainId())
	require.NoError(t, err)
	transactOpts.GasLimit = params.MaxTxnGasLimit
	_, txn, changer, err := contracts.DeployChanger(transactOpts, eat.ContractBackend)
	require.NoError(t, err)
	payload, err := eat.MockCl.BuildCanonicalBlock(ctx)
	require.NoError(t, err)
	require.NoError(t, eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, payload.ExecutionPayload, txn.Hash()))
	txn, err = changer.Change(transactOpts)
	require.NoError(t, err)
	child, err := eat.MockCl.BuildCanonicalBlock(ctx)
	require.NoError(t, err)
	require.NoError(t, eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, child.ExecutionPayload, txn.Hash()))
	return payload
}

// A payload committed before its witness was requested gets the witness rebuilt from the committed state.
func TestEngineApiPayloadWitnessForCommittedPayload(t *testing.T) {
	runCommitmentVariants(t, testEngineApiPayloadWitnessForCommittedPayload)
}

func testEngineApiPayloadWitnessForCommittedPayload(t *testing.T) {
	eat := newPayloadWitnessTester(t, engineapitester.EngineApiTesterInitArgs{
		EthConfigTweaker: func(cfg *ethconfig.Config) { cfg.KeepExecutionProofs = true },
		EnableDebugAPI:   true,
	})
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		payload := buildCommittedChangerPayload(ctx, t, eat)

		requireDebugExecutionWitness(ctx, t, eat, payload, requirePayloadWitness(ctx, t, eat, payload))
	})
}

func TestEngineApiPayloadWitnessForCommittedPayloadWithoutCommitmentHistory(t *testing.T) {
	runCommitmentVariants(t, testEngineApiPayloadWitnessForCommittedPayloadWithoutCommitmentHistory)
}

func testEngineApiPayloadWitnessForCommittedPayloadWithoutCommitmentHistory(t *testing.T) {
	eat := newPayloadWitnessTester(t, engineapitester.EngineApiTesterInitArgs{})
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		payload := buildCommittedChangerPayload(ctx, t, eat)

		require.NotEmpty(t, requirePayloadWitness(ctx, t, eat, payload).State)
	})
}

// A resubmitted payload whose cached witness was evicted gets the same witness again.
func TestEngineApiPayloadWitnessAfterCacheEviction(t *testing.T) {
	runCommitmentVariants(t, testEngineApiPayloadWitnessAfterCacheEviction)
}

func testEngineApiPayloadWitnessAfterCacheEviction(t *testing.T) {
	eat := newPayloadWitnessTester(t, engineapitester.EngineApiTesterInitArgs{})
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		first, _ := buildChangerPayload(ctx, t, eat)
		witness := requirePayloadWitness(ctx, t, eat, first)
		require.NoError(t, eat.MockCl.UpdateForkChoice(ctx, first))
		for range engineapi.PayloadWitnessCacheSize {
			payload, err := eat.MockCl.BuildNewPayload(ctx)
			require.NoError(t, err)
			requirePayloadWitness(ctx, t, eat, payload)
			require.NoError(t, eat.MockCl.UpdateForkChoice(ctx, payload))
		}

		require.Equal(t, witness, requirePayloadWitness(ctx, t, eat, first))
	})
}

func TestEngineApiPayloadWitnessForKnownPayload(t *testing.T) {
	runCommitmentVariants(t, testEngineApiPayloadWitnessForKnownPayload)
}

func testEngineApiPayloadWitnessForKnownPayload(t *testing.T) {
	eat := newPayloadWitnessTester(t, engineapitester.EngineApiTesterInitArgs{})
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		payload, _ := buildChangerPayload(ctx, t, eat)
		witness := requirePayloadWitness(ctx, t, eat, payload)
		require.NoError(t, eat.MockCl.UpdateForkChoice(ctx, payload))

		require.Equal(t, witness, requirePayloadWitness(ctx, t, eat, payload))
	})
}

// A forkchoice update back to the committed head unwinds a payload, which leaves it a known VALID payload with no
// state and its child a child of an uncommitted payload.
func TestEngineApiPayloadWitnessForAnUnwoundPayloadAndItsChild(t *testing.T) {
	runCommitmentVariants(t, testEngineApiPayloadWitnessForAnUnwoundPayloadAndItsChild)
}

func testEngineApiPayloadWitnessForAnUnwoundPayloadAndItsChild(t *testing.T) {
	eat := newPayloadWitnessTester(t, engineapitester.EngineApiTesterInitArgs{
		EthConfigTweaker: func(cfg *ethconfig.Config) { cfg.KeepExecutionProofs = true },
		EnableDebugAPI:   true,
	})
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		parent, changerAddr := buildChangerPayload(ctx, t, eat)
		committedHead := parent.ExecutionPayload.ParentHash
		status, err := eat.MockCl.InsertNewPayload(ctx, parent)
		require.NoError(t, err)
		require.Equal(t, enginetypes.ValidStatus, status.Status)
		require.NoError(t, eat.MockCl.UpdateForkChoice(ctx, parent))
		transactOpts, err := bind.NewKeyedTransactorWithChainID(eat.CoinbaseKey, eat.ChainId())
		require.NoError(t, err)
		transactOpts.GasLimit = params.MaxTxnGasLimit
		changer, err := contracts.NewChanger(changerAddr, eat.ContractBackend)
		require.NoError(t, err)
		_, err = changer.Change(transactOpts)
		require.NoError(t, err)
		child, err := eat.MockCl.BuildNewPayload(ctx)
		require.NoError(t, err)
		require.Len(t, child.ExecutionPayload.Transactions, 1)
		require.NoError(t, eat.MockCl.UpdateForkChoiceByHash(ctx, committedHead))

		parentWitness := requirePayloadWitness(ctx, t, eat, parent)
		childWitness := requirePayloadWitness(ctx, t, eat, child)

		require.NoError(t, eat.MockCl.UpdateForkChoiceByHash(ctx, child.ExecutionPayload.BlockHash))
		requireDebugExecutionWitness(ctx, t, eat, parent, parentWitness)
		requireDebugExecutionWitness(ctx, t, eat, child, childWitness)
	})
}
