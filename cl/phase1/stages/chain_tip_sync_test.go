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

package stages

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
)

type recoveredEnvelopeProcessorStub struct {
	envelopes []*cltypes.SignedExecutionPayloadEnvelope
	validate  []bool
}

func (s *recoveredEnvelopeProcessorStub) ProcessRecoveredEnvelope(_ context.Context, envelope *cltypes.SignedExecutionPayloadEnvelope, validate bool) error {
	s.envelopes = append(s.envelopes, envelope)
	s.validate = append(s.validate, validate)
	return nil
}

func TestApplyRecoveredEnvelopesUsesRetryingProcessor(t *testing.T) {
	processor := &recoveredEnvelopeProcessorStub{}
	envelope := &cltypes.SignedExecutionPayloadEnvelope{
		Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig),
	}
	cfg := &Cfg{recoveredEnvelopeProcessor: processor}

	applyRecoveredEnvelopes(t.Context(), cfg, map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{
		common.HexToHash("0x1234"): envelope,
	})

	require.Equal(t, []*cltypes.SignedExecutionPayloadEnvelope{envelope}, processor.envelopes)
	require.Equal(t, []bool{false}, processor.validate)
}

func TestApplyRecoveredEnvelopesIgnoresNilEnvelope(t *testing.T) {
	processor := &recoveredEnvelopeProcessorStub{}
	cfg := &Cfg{recoveredEnvelopeProcessor: processor}

	applyRecoveredEnvelopes(t.Context(), cfg, map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{
		common.HexToHash("0x1234"): nil,
	})

	require.Empty(t, processor.envelopes)
}
