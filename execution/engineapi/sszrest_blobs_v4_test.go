// Copyright 2026 The Erigon Authors
// This file is part of Erigon.

package engineapi

import (
	"bytes"
	"encoding/binary"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes/solid"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/txnprovider/txpool"
)

func blobsV4RequestBytes(hashes []common.Hash, mask []byte) []byte {
	body := binary.LittleEndian.AppendUint32(nil, 20)
	body = append(body, mask...)
	for _, hash := range hashes {
		body = append(body, hash[:]...)
	}
	return body
}

func decodeBlobsV4TestResponse(t *testing.T, buf []byte) []*engine_types.BlobCellsAndProofsV1 {
	t.Helper()
	entries := solid.NewTransactionsSSZWithLimits(128, 1<<20)
	require.NoError(t, ssz2.UnmarshalSSZStrict(buf, 0, entries))
	result := make([]*engine_types.BlobCellsAndProofsV1, entries.Len())
	for i, entry := range entries.UnderlyngReference() {
		var available bool
		contents := solid.NewByteListSSZ(1 << 20)
		require.NoError(t, ssz2.UnmarshalSSZStrict(entry, 0, &available, contents))
		cells := solid.NewTransactionsSSZWithLimits(128, 2048)
		proofs := solid.NewTransactionsSSZWithLimits(128, 48)
		require.NoError(t, ssz2.UnmarshalSSZStrict(contents.Bytes(), 0, cells, proofs))
		if !available {
			require.Zero(t, cells.Len())
			require.Zero(t, proofs.Len())
			continue
		}
		require.Equal(t, 128, cells.Len())
		require.Equal(t, 128, proofs.Len())
		result[i] = &engine_types.BlobCellsAndProofsV1{
			BlobCells: make([]*hexutil.Bytes, 128),
			Proofs:    make([]*hexutil.Bytes, 128),
		}
		for j, cell := range cells.UnderlyngReference() {
			proof := proofs.UnderlyngReference()[j]
			if len(cell) == 0 {
				require.Empty(t, proof)
				continue
			}
			require.Len(t, cell, 2048)
			require.Len(t, proof, 48)
			cellBytes, proofBytes := hexutil.Bytes(cell), hexutil.Bytes(proof)
			result[i].BlobCells[j] = &cellBytes
			result[i].Proofs[j] = &proofBytes
		}
	}
	return result
}

func TestSSZRESTBlobsV4SelectedCells(t *testing.T) {
	hash, bundle, cells := getBlobsV4Fixture(t, 7)
	srv := newTestEngineServer(allForksConfig(), true)
	srv.blobGetter = blobGetterMap{hash: bundle}
	for _, mask := range []hexutil.Bytes{
		hexutil.MustDecodeHex("0x81010000000000800100000000000080"),
		bytes.Repeat([]byte{0xff}, 16),
		make([]byte, 16),
	} {
		t.Run(mask.String(), func(t *testing.T) {
			hashes := []common.Hash{{}, hash, hash}
			rec := httptest.NewRecorder()
			srv.SSZRESTHandler().ServeHTTP(rec, newSSZRequest(t.Context(), http.MethodPost, "/engine/v1/blobs/v4", "ignored", blobsV4RequestBytes(hashes, mask)))
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			require.Equal(t, sszRestContentType, rec.Header().Get("Content-Type"))
			result := decodeBlobsV4TestResponse(t, rec.Body.Bytes())
			require.Len(t, result, 3)
			require.Nil(t, result[0])
			for _, blob := range result[1:] {
				require.NotNil(t, blob)
				for index := range 128 {
					if mask[index/8]&(1<<(index%8)) == 0 {
						require.Nil(t, blob.BlobCells[index])
						require.Nil(t, blob.Proofs[index])
						continue
					}
					require.NotNil(t, blob.BlobCells[index])
					require.NotNil(t, blob.Proofs[index])
					require.Equal(t, hexutil.Bytes(cells[index][:]), *blob.BlobCells[index])
					require.Equal(t, hexutil.Bytes(bundle.Proofs[index][:]), *blob.Proofs[index])
				}
			}
		})
	}
}

func TestSSZRESTBlobsV4Availability(t *testing.T) {
	for _, tc := range []struct {
		name   string
		getter txpool.BlobGetter
		hashes []common.Hash
		code   int
	}{
		{"disabled pool", nil, []common.Hash{{1}}, http.StatusNoContent},
		{"unavailable pool", blobGetterFunc(func([]common.Hash) []txpool.PoolBlobBundle { return nil }), []common.Hash{{1}}, http.StatusNoContent},
		{"missing blob", blobGetterMap{}, []common.Hash{{1}}, http.StatusOK},
		{"empty request", blobGetterMap{}, nil, http.StatusOK},
		{"maximum hashes", blobGetterMap{}, make([]common.Hash, 128), http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := newTestEngineServer(allForksConfig(), true)
			srv.blobGetter = tc.getter
			rec := httptest.NewRecorder()
			body := blobsV4RequestBytes(tc.hashes, make([]byte, 16))
			srv.SSZRESTHandler().ServeHTTP(rec, newSSZRequest(t.Context(), http.MethodPost, "/engine/v1/blobs/v4", "", body))
			require.Equal(t, tc.code, rec.Code, rec.Body.String())
			if tc.code == http.StatusNoContent {
				require.Empty(t, rec.Body.Bytes())
				return
			}
			result := decodeBlobsV4TestResponse(t, rec.Body.Bytes())
			require.Len(t, result, len(tc.hashes))
			for _, blob := range result {
				require.Nil(t, blob)
			}
		})
	}
}

func TestSSZRESTBlobsV4MalformedRequest(t *testing.T) {
	valid := blobsV4RequestBytes([]common.Hash{{1}}, make([]byte, 16))
	for _, tc := range []struct {
		name string
		body []byte
	}{
		{"empty body", nil},
		{"missing mask", []byte{20, 0, 0, 0}},
		{"short mask", blobsV4RequestBytes(nil, make([]byte, 15))},
		{"long mask", blobsV4RequestBytes(nil, make([]byte, 17))},
		{"offset into fixed section", append([]byte{4, 0, 0, 0}, valid[4:]...)},
		{"offset gap", append([]byte{21, 0, 0, 0}, valid[4:]...)},
		{"offset past end", append([]byte{255, 255, 255, 255}, valid[4:]...)},
		{"partial hash", valid[:len(valid)-1]},
		{"trailing byte", append(bytes.Clone(valid), 0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := newTestEngineServer(allForksConfig(), true)
			srv.blobGetter = blobGetterFunc(func([]common.Hash) []txpool.PoolBlobBundle {
				t.Fatal("malformed request reached the blob pool")
				return nil
			})
			rec := httptest.NewRecorder()
			srv.SSZRESTHandler().ServeHTTP(rec, newSSZRequest(t.Context(), http.MethodPost, "/engine/v1/blobs/v4", "", tc.body))
			require.Equal(t, http.StatusBadRequest, rec.Code)
			require.Equal(t, problemSSZDecodeError, problemType(t, rec))
		})
	}
}

func TestSSZRESTBlobsV4PartialCells(t *testing.T) {
	cell, proof := hexutil.Bytes(bytes.Repeat([]byte{1}, 2048)), hexutil.Bytes(bytes.Repeat([]byte{2}, 48))
	mask := [16]byte{0: 2, 8: 1, 15: 0x80}
	blob := &engine_types.BlobCellsAndProofsV1{
		BlobCells: []*hexutil.Bytes{nil, &cell, nil},
		Proofs:    []*hexutil.Bytes{nil, &proof, nil},
	}
	encoded, err := encodeBlobsV4Response(engine_types.BlobsBundleV3{blob}, mask)
	require.NoError(t, err)
	decoded := decodeBlobsV4TestResponse(t, encoded)
	require.Len(t, decoded, 1)
	for index := range 128 {
		if index == 64 {
			require.Equal(t, &cell, decoded[0].BlobCells[index])
			require.Equal(t, &proof, decoded[0].Proofs[index])
		} else {
			require.Nil(t, decoded[0].BlobCells[index])
			require.Nil(t, decoded[0].Proofs[index])
		}
	}
}
