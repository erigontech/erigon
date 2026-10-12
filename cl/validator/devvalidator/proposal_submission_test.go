// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package devvalidator

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

func TestGloasProposalPublishesBlockThenSignedEnvelope(t *testing.T) {
	cfg, key, block, envelope := proposalFixture(t, clparams.GloasVersion)
	slot := block.Slot
	bid := block.Body.SignedExecutionPayloadBid.Message
	blockRoot := envelope.BeaconBlockRoot
	genesisRoot := common.Hash{3}
	var submitted []string
	mux := http.NewServeMux()
	mux.HandleFunc("GET /eth/v3/validator/blocks/1", func(w http.ResponseWriter, r *http.Request) {
		assert.NotEmpty(t, r.URL.Query().Get("randao_reveal"))
		assert.NoError(t, json.NewEncoder(w).Encode(map[string]any{"data": block, "execution_payload_envelope": envelope}))
	})
	mux.HandleFunc("POST /eth/v2/beacon/blocks", func(w http.ResponseWriter, r *http.Request) {
		decoded := cltypes.NewSignedBeaconBlock(cfg, clparams.GloasVersion)
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(decoded); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		assert.Equal(t, "gloas", r.Header.Get("Eth-Consensus-Version"))
		got, err := decoded.Block.HashSSZ()
		assert.NoError(t, err)
		assert.Equal(t, common.Hash(blockRoot), common.Hash(got))
		submitted = append(submitted, "block")
	})
	mux.HandleFunc("POST /eth/v1/beacon/execution_payload_envelopes", func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, []string{"block"}, submitted)
		assert.Equal(t, "gloas", r.Header.Get("Eth-Consensus-Version"))
		assert.Equal(t, "false", r.Header.Get("Eth-Blob-Data-Included"))
		signed := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
		assert.NoError(t, json.NewDecoder(r.Body).Decode(signed))
		assert.Equal(t, common.Hash(blockRoot), signed.Message.BeaconBlockRoot)
		assert.Equal(t, block.ParentRoot, signed.Message.ParentBeaconBlockRoot)
		assert.Equal(t, bid.BlockHash, signed.Message.Payload.BlockHash)
		domainType := common.Bytes4{0x0b, 0, 0, 0}
		domain, err := fork.ComputeDomain(domainType[:], utils.Uint32ToBytes4(uint32(cfg.GloasForkVersion)), genesisRoot)
		assert.NoError(t, err)
		signingRoot, err := fork.ComputeSigningRoot(signed.Message, domain)
		assert.NoError(t, err)
		valid, err := bls.Verify(signed.Signature[:], signingRoot[:], key.PubKeyBytes[:])
		assert.NoError(t, err)
		assert.True(t, valid, "envelope must use the proposer key and beacon builder domain")
		submitted = append(submitted, "envelope")
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	genesisTime := uint64(time.Now().Unix()) - slot*cfg.SecondsPerSlot
	service := &Service{client: NewBeaconClient(server.URL), cfg: cfg, genesisTime: genesisTime, genesisValidatorsRoot: genesisRoot, logger: log.New()}
	require.NoError(t, service.proposeBlock(context.Background(), slot, key))
	require.Equal(t, []string{"block", "envelope"}, submitted)
}

func TestGloasProposalRetriesSameEnvelopeWithoutRepublishingBlock(t *testing.T) {
	cfg, key, block, envelope := proposalFixture(t, clparams.GloasVersion)
	var blockPosts atomic.Int32
	var envelopePosts atomic.Int32
	envelopeBodies := make(chan []byte, 2)
	firstEnvelopeStarted := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /eth/v3/validator/blocks/1", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"data": block, "execution_payload_envelope": envelope}))
	})
	mux.HandleFunc("POST /eth/v2/beacon/blocks", func(http.ResponseWriter, *http.Request) {
		blockPosts.Add(1)
	})
	mux.HandleFunc("POST /eth/v1/beacon/execution_payload_envelopes", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		// Count before queueing: the test waits on the bodies, so a post the
		// counter has not recorded yet would read as one post short.
		first := envelopePosts.Add(1) == 1
		envelopeBodies <- body
		if first {
			close(firstEnvelopeStarted)
			<-r.Context().Done()
		}
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	service := &Service{
		client:                NewBeaconClient(server.URL),
		cfg:                   cfg,
		genesisTime:           uint64(time.Now().Unix()) - block.Slot*cfg.SecondsPerSlot,
		genesisValidatorsRoot: common.Hash{3},
		logger:                log.New(),
	}

	ctx, cancel := context.WithCancel(t.Context())
	proposalDone := make(chan error, 1)
	go func() { proposalDone <- service.proposeBlock(ctx, block.Slot, key) }()
	<-firstEnvelopeStarted
	cancel()
	require.NoError(t, <-proposalDone)
	var bodies [][]byte
	for range 2 {
		select {
		case body := <-envelopeBodies:
			bodies = append(bodies, body)
		case <-time.After(time.Second):
			t.Fatal("signed envelope retry did not complete")
		}
	}
	require.Equal(t, int32(1), blockPosts.Load())
	require.Equal(t, int32(2), envelopePosts.Load())
	require.Equal(t, bodies[0], bodies[1])
}

func TestGloasProposalStopsEnvelopeRetryWithService(t *testing.T) {
	cfg, key, block, envelope := proposalFixture(t, clparams.GloasVersion)
	var envelopePosts atomic.Int32
	firstEnvelopeDone := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /eth/v3/validator/blocks/1", func(w http.ResponseWriter, _ *http.Request) {
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"data": block, "execution_payload_envelope": envelope}))
	})
	mux.HandleFunc("POST /eth/v2/beacon/blocks", func(http.ResponseWriter, *http.Request) {})
	mux.HandleFunc("POST /eth/v1/beacon/execution_payload_envelopes", func(w http.ResponseWriter, _ *http.Request) {
		if envelopePosts.Add(1) == 1 {
			close(firstEnvelopeDone)
		}
		http.Error(w, "temporary failure", http.StatusServiceUnavailable)
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	_, serviceCancel := context.WithCancel(t.Context())
	service := &Service{
		client:                NewBeaconClient(server.URL),
		cfg:                   cfg,
		genesisTime:           uint64(time.Now().Unix()) - block.Slot*cfg.SecondsPerSlot,
		genesisValidatorsRoot: common.Hash{3},
		logger:                log.New(),
		cancel:                serviceCancel,
	}

	require.NoError(t, service.proposeBlock(t.Context(), block.Slot, key))
	<-firstEnvelopeDone
	service.Stop()
	time.Sleep(envelopeSubmissionRetryInterval + 150*time.Millisecond)
	require.Equal(t, int32(1), envelopePosts.Load())
}

func TestGloasProposalRejectsInvalidEnvelopeBeforePublishing(t *testing.T) {
	for _, name := range []string{"missing", "null", "payload", "requests", "block root", "parent root", "builder", "payload hash", "block body", "block slot", "payload slot", "requests root", "bid", "bid message", "external builder envelope", "nil withdrawal", "nil deposit request", "nil withdrawal request", "nil consolidation request", "nil builder deposit request", "nil builder exit request", "nil transaction", "nested BAL"} {
		t.Run(name, func(t *testing.T) {
			cfg, key, block, envelope := proposalFixture(t, clparams.GloasVersion)
			response := map[string]any{"data": block, "execution_payload_envelope": envelope}
			switch name {
			case "missing":
				delete(response, "execution_payload_envelope")
			case "null":
				response["execution_payload_envelope"] = nil
			case "payload":
				envelope.Payload = nil
			case "requests":
				envelope.ExecutionRequests = nil
			case "nil withdrawal":
				envelope.Payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(cfg.MaxWithdrawalsPerPayload), 44)
				envelope.Payload.Withdrawals.Append(nil)
			case "nil deposit request":
				envelope.ExecutionRequests.Deposits.Append(nil)
			case "nil withdrawal request":
				envelope.ExecutionRequests.Withdrawals.Append(nil)
			case "nil consolidation request":
				envelope.ExecutionRequests.Consolidations.Append(nil)
			case "nil builder deposit request":
				envelope.ExecutionRequests.BuilderDeposits.Append(nil)
			case "nil builder exit request":
				envelope.ExecutionRequests.BuilderExits.Append(nil)
			case "nil transaction", "nested BAL":
				encoded, err := json.Marshal(envelope)
				require.NoError(t, err)
				var malformed map[string]any
				require.NoError(t, json.Unmarshal(encoded, &malformed))
				payload := malformed["payload"].(map[string]any)
				if name == "nil transaction" {
					payload["transactions"] = []any{nil}
				} else {
					payload["block_access_list"] = []any{nil}
				}
				response["execution_payload_envelope"] = malformed
			case "block root":
				envelope.BeaconBlockRoot[0] ^= 1
			case "parent root":
				envelope.ParentBeaconBlockRoot[0] ^= 1
			case "builder":
				envelope.BuilderIndex = 1
			case "payload hash":
				envelope.Payload.BlockHash[0] ^= 1
			case "block body":
				block.Body = nil
			case "block slot":
				block.Slot++
			case "payload slot":
				envelope.Payload.SlotNumber++
			case "requests root":
				block.Body.SignedExecutionPayloadBid.Message.ExecutionRequestsRoot[0] ^= 1
				root, err := block.HashSSZ()
				require.NoError(t, err)
				envelope.BeaconBlockRoot = root
			case "bid":
				block.Body.SignedExecutionPayloadBid = nil
			case "bid message":
				block.Body.SignedExecutionPayloadBid.Message = nil
			case "external builder envelope":
				block.Body.SignedExecutionPayloadBid.Message.BuilderIndex = 1
			}
			gets, posts := 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					gets++
					assert.NoError(t, json.NewEncoder(w).Encode(response))
				} else {
					posts++
				}
			}))
			defer server.Close()
			service := &Service{client: NewBeaconClient(server.URL), cfg: cfg, logger: log.New()}
			var err error
			require.NotPanics(t, func() { err = service.proposeBlock(context.Background(), 1, key) })
			require.Error(t, err)
			require.Equal(t, 1, gets)
			require.Zero(t, posts, "invalid self-build template must not be published")
		})
	}
}

func TestProposalSubmissionPreservesForkAndErrorBoundaries(t *testing.T) {
	for _, name := range []string{"fulu", "external builder", "block failure"} {
		t.Run(name, func(t *testing.T) {
			version := clparams.GloasVersion
			if name == "fulu" {
				version = clparams.FuluVersion
			}
			cfg, key, block, envelope := proposalFixture(t, version)
			response := map[string]any{"data": block}
			switch {
			case version == clparams.FuluVersion:
				response["data"] = &cltypes.DenebBeaconBlock{Block: block, KZGProofs: solid.NewStaticListSSZ[*cltypes.KZGProof](0, cltypes.BYTES_KZG_PROOF), Blobs: solid.NewStaticListSSZ[*cltypes.Blob](0, int(cltypes.BYTES_PER_BLOB))}
			case name == "external builder":
				block.Body.SignedExecutionPayloadBid.Message.BuilderIndex = 1
			default:
				response["execution_payload_envelope"] = envelope
			}
			var requests []string
			envelopePosts := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/eth/v3/validator/blocks/1":
					assert.Equal(t, http.MethodGet, r.Method)
					requests = append(requests, "template")
					assert.NoError(t, json.NewEncoder(w).Encode(response))
				case "/eth/v2/beacon/blocks":
					assert.Equal(t, version.String(), r.Header.Get("Eth-Consensus-Version"))
					requests = append(requests, "block")
					var decoded any = cltypes.NewSignedBeaconBlock(cfg, version)
					if version == clparams.FuluVersion {
						decoded = cltypes.NewDenebSignedBeaconBlock(cfg, version)
					}
					decoder := json.NewDecoder(r.Body)
					decoder.DisallowUnknownFields()
					assert.NoError(t, decoder.Decode(decoded))
					if wrapped, ok := decoded.(*cltypes.DenebSignedBeaconBlock); ok {
						assert.Equal(t, uint64(1), wrapped.SignedBlock.Block.Slot)
					}
					if name == "block failure" {
						http.Error(w, "block rejected", http.StatusBadRequest)
						return
					}
				case "/eth/v1/beacon/execution_payload_envelopes":
					requests = append(requests, "envelope")
					envelopePosts++
					if envelopePosts == 1 {
						http.Error(w, "retry later", http.StatusServiceUnavailable)
						return
					}
				default:
					t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			service := &Service{client: NewBeaconClient(server.URL), cfg: cfg, logger: log.New()}
			err := service.proposeBlock(context.Background(), 1, key)
			if name == "block failure" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, []string{"template", "block"}, requests)
		})
	}
}

func proposalFixture(t *testing.T, version clparams.StateVersion) (*clparams.BeaconChainConfig, *ValidatorKey, *cltypes.BeaconBlock, *cltypes.ExecutionPayloadEnvelope) {
	t.Helper()
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch, cfg.BellatrixForkEpoch, cfg.CapellaForkEpoch = 0, 0, 0
	cfg.DenebForkEpoch, cfg.ElectraForkEpoch, cfg.FuluForkEpoch = 0, 0, 0
	if version == clparams.GloasVersion {
		cfg.GloasForkEpoch = 0
	}
	keys, err := LoadKeys("proposal-submission", 1)
	require.NoError(t, err)
	block := cltypes.NewBeaconBlock(&cfg, version)
	block.Slot = 1
	block.ParentRoot = common.Hash{1}
	if version != clparams.GloasVersion {
		return &cfg, keys[0], block, nil
	}
	envelope := cltypes.NewExecutionPayloadEnvelope(&cfg)
	bid := block.Body.SignedExecutionPayloadBid.Message
	bid.BuilderIndex = clparams.BuilderIndexSelfBuild
	bid.BlockHash = common.Hash{2}
	bid.Slot = block.Slot
	requestsRoot, err := envelope.ExecutionRequests.HashSSZ()
	require.NoError(t, err)
	bid.ExecutionRequestsRoot = requestsRoot
	root, err := block.HashSSZ()
	require.NoError(t, err)
	envelope.BuilderIndex = clparams.BuilderIndexSelfBuild
	envelope.BeaconBlockRoot = root
	envelope.ParentBeaconBlockRoot = block.ParentRoot
	envelope.Payload.BlockHash = bid.BlockHash
	envelope.Payload.SlotNumber = block.Slot
	return &cfg, keys[0], block, envelope
}
