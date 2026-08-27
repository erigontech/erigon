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

package network

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	blob_mock_services "github.com/erigontech/erigon/cl/persistence/blob_storage/mock_services"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

type recordingHandler struct{ records []*log.Record }

func (h *recordingHandler) Log(r *log.Record) error {
	h.records = append(h.records, r)
	return nil
}

func (h *recordingHandler) Enabled(_ context.Context, _ log.Lvl) bool { return true }

func (h *recordingHandler) find(msg string) *log.Record {
	for _, r := range h.records {
		if r.Msg == msg {
			return r
		}
	}
	return nil
}

// VerifyAgainstIdentifiersAndInsertIntoTheBlobStore stops at the first identifier a
// response does not match and returns a nil error reporting how many it stored, so a
// caller that checks only the error cannot tell a full insert from an empty one. A
// backfill that treats a zero insert as success leaves the slot permanently short with
// nothing in the log to say so.
func TestStoreDenebBlobsReportsWhenTheStoreTookFewerThanRequested(t *testing.T) {
	ctrl := gomock.NewController(t)
	handler := &recordingHandler{}
	logger := log.New()
	logger.SetHandler(handler)

	b := &BlobHistoryDownloader{
		ctx:         context.Background(),
		beaconCfg:   &clparams.MainnetBeaconConfig,
		blobStorage: blob_mock_services.NewMockBlobStorage(ctrl),
		logger:      logger,
	}

	req := solid.NewStaticListSSZ[*cltypes.BlobIdentifier](8, 40)
	req.Append(&cltypes.BlobIdentifier{BlockRoot: common.HexToHash("0xaa"), Index: 0})

	// A sidecar whose header hashes to some other root: the insert loop breaks on the
	// first identifier mismatch, stores nothing, and reports no error.
	sidecar := &cltypes.BlobSidecar{
		SignedBlockHeader: &cltypes.SignedBeaconBlockHeader{
			Header: &cltypes.BeaconBlockHeader{Slot: 1},
		},
	}

	b.storeDenebBlobs(nil, req, []*cltypes.BlobSidecar{sidecar})

	rec := handler.find("[BlobHistoryDownloader] Store took fewer blobs than requested")
	require.NotNil(t, rec, "a zero insert must not pass silently")
	require.Contains(t, rec.Ctx, 1, "the log must name how many were requested")
	require.Contains(t, rec.Ctx, uint64(0), "the log must name how many were stored")
}
