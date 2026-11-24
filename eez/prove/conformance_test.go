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

package prove

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/internal/testfixtures"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/eez/prove/provepb"
)

// transcripts are recorded ProveStream exchanges with the reference signer
// over window 84 (eez/testdata/SOURCES.md).
type transcripts struct {
	Format string `json:"format"`
	Oracle struct {
		Attester         common.Address `json:"attester"`
		PublicInputsHash common.Hash    `json:"public_inputs_hash"`
	} `json:"oracle"`
	Cases []struct {
		Name    string `json:"name"`
		Profile string `json:"profile"`
		Steps   []struct {
			Action string `json:"action"`
			Kind   string `json:"kind"`
			Stream string `json:"stream"`
			Hex    string `json:"protobuf_hex"`
		} `json:"steps"`
	} `json:"cases"`
}

func loadTranscripts(t *testing.T, name string) *transcripts {
	t.Helper()
	data, err := testfixtures.ReadGzip("conformance", name)
	require.NoError(t, err)
	var tr transcripts
	require.NoError(t, json.Unmarshal(data, &tr))
	require.Equal(t, "eez-prove-v2-transcript-1", tr.Format)
	return &tr
}

// TestConformanceFrames: every recorded frame decodes into the vendored
// types and encodes back to the same bytes.
func TestConformanceFrames(t *testing.T) {
	var requests, replies int
	for _, name := range []string{"rollup0-transcripts.json.gz", "rollup0-limits-transcripts.json.gz"} {
		for _, c := range loadTranscripts(t, name).Cases {
			for i, s := range c.Steps {
				var m proto.Message
				switch s.Action {
				case "send":
					m, requests = new(provepb.ClientFrame), requests+1
				case "receive":
					m, replies = new(provepb.ServerFrame), replies+1
				default:
					continue
				}
				raw, err := hex.DecodeString(s.Hex)
				require.NoError(t, err, "%s step %d", c.Name, i)
				require.NoError(t, proto.Unmarshal(raw, m), "%s step %d", c.Name, i)
				enc, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
				require.NoError(t, err, "%s step %d", c.Name, i)
				require.Equal(t, raw, enc, "%s step %d", c.Name, i)
			}
		}
	}
	// The counts the conformance guide states.
	require.Equal(t, 339, requests)
	require.Equal(t, 338, replies)
}

// binding clears a frame's session binding and request ID, and a
// Finalize's proofs: the transcripts send the mined calldata, which the
// signer reads without them.
func binding(f *provepb.ClientFrame) []byte {
	f = proto.Clone(f).(*provepb.ClientFrame)
	f.SessionId, f.RequestId, f.Epoch = nil, 0, 0
	if s := f.GetFinalize().GetSettlement(); s != nil {
		if b, err := protocol.DecodePostAndVerifyBatch(s.AbiCalldata); err == nil {
			b.Proofs = nil
			if s.AbiCalldata, err = protocol.EncodePostAndVerifyBatch(b); err != nil {
				panic(err)
			}
		}
	}
	enc, err := proto.MarshalOptions{Deterministic: true}.Marshal(f)
	if err != nil {
		panic(err)
	}
	return enc
}

// scripted answers each request with the reply the reference signer gave
// the same request in a baseline transcript.
type scripted struct {
	provepb.UnimplementedProverServer
	t       *testing.T
	replies map[string]*provepb.ServerFrame
}

func newScripted(t *testing.T, tr *transcripts) *scripted {
	s := &scripted{t: t, replies: map[string]*provepb.ServerFrame{}}
	for _, c := range tr.Cases {
		if c.Profile != "baseline" {
			continue
		}
		for i := 0; i+1 < len(c.Steps); i++ {
			req, rep := c.Steps[i], c.Steps[i+1]
			if req.Action != "send" || rep.Action != "receive" || req.Stream != rep.Stream {
				continue
			}
			switch rep.Kind {
			case "ready", "validated", "proof":
			default:
				continue
			}
			var f provepb.ClientFrame
			var r provepb.ServerFrame
			require.NoError(t, proto.Unmarshal(common.FromHex(req.Hex), &f))
			require.NoError(t, proto.Unmarshal(common.FromHex(rep.Hex), &r))
			if _, ok := s.replies[string(binding(&f))]; !ok {
				s.replies[string(binding(&f))] = &r
			}
		}
	}
	return s
}

func (s *scripted) ProveStream(stream provepb.Prover_ProveStreamServer) error {
	for {
		f, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		r, ok := s.replies[string(binding(f))]
		if !ok {
			s.t.Errorf("no recorded reply to request %d, %T", f.RequestId, f.Kind)
			return status.Error(codes.Unimplemented, "not in the transcripts")
		}
		r = proto.Clone(r).(*provepb.ServerFrame)
		r.RequestId = f.RequestId
		if err := stream.Send(r); err != nil {
			return err
		}
		if r.GetProof() != nil {
			return nil
		}
	}
}

// TestConformanceSession: every request of a session over window 84 is one
// the reference signer answered, and its recorded proof attests the batch.
func TestConformanceSession(t *testing.T) {
	tr := loadTranscripts(t, "rollup0-transcripts.json.gz")
	f := loadFixture(t, "captured-devnet-window-84")
	require.Equal(t, tr.Oracle.Attester, f.Attester)
	require.Equal(t, tr.Oracle.PublicInputsHash, f.Hash)
	sig, err := dial(t, serve(t, newScripted(t, tr)), f).Prove(t.Context(), f.window())
	require.NoError(t, err)
	got, err := attest(sig, f.Hash[:], f.Attester)
	require.NoError(t, err)
	require.Equal(t, f.Hash, got)
}
