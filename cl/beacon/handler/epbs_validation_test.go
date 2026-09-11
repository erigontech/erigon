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

package handler

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common/log/v3"
)

func TestEnvelopeContentsAuthenticateBeforeBlobProcessing(t *testing.T) {
	_, _, _, _, _, handler, _, _, forkchoice, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	want := errors.New("invalid envelope signature")
	forkchoice.ValidateExecutionPayloadEnvelopeErr = want
	contents := cltypes.NewSignedExecutionPayloadEnvelopeContents(handler.beaconChainCfg, 0)

	err := handler.validateAndStoreExecutionPayloadEnvelopeContents(t.Context(), contents)

	require.ErrorIs(t, err, want)
}
