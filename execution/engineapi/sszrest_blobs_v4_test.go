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

func decodeBlobsV4TestResponse(t *testing.T, buf []byte) *solid.ListSSZ[*sszBlobV4Entry] {
	t.Helper()
	entries := solid.NewDynamicListSSZ[*sszBlobV4Entry](maxGetBlobHashes)
	require.NoError(t, ssz2.UnmarshalSSZStrict(buf, 0, entries))
	return entries
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
			require.Equal(t, 3, result.Len())
			require.False(t, result.Get(0).Available)
			require.Zero(t, result.Get(0).Contents.cells.Len())
			require.Zero(t, result.Get(0).Contents.proofs.Len())
			for i := 1; i < result.Len(); i++ {
				blob := result.Get(i)
				require.True(t, blob.Available)
				blobCells, proofs := blob.Contents.cells.UnderlyngReference(), blob.Contents.proofs.UnderlyngReference()
				require.Len(t, blobCells, 128)
				require.Len(t, proofs, 128)
				for index := range 128 {
					if mask[index/8]&(1<<(index%8)) == 0 {
						require.Empty(t, blobCells[index])
						require.Empty(t, proofs[index])
						continue
					}
					require.Equal(t, cells[index][:], blobCells[index])
					require.Equal(t, bundle.Proofs[index][:], proofs[index])
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
			require.Equal(t, len(tc.hashes), result.Len())
			for i := range result.Len() {
				blob := result.Get(i)
				require.False(t, blob.Available)
				require.Zero(t, blob.Contents.cells.Len())
				require.Zero(t, blob.Contents.proofs.Len())
			}
		})
	}
}

func TestSSZRESTBlobsV4DecodeRejectsTooManyHashes(t *testing.T) {
	body := blobsV4RequestBytes(make([]common.Hash, 129), make([]byte, 16))
	_, _, err := decodeBlobsV4Request(body)
	require.ErrorContains(t, err, "too many blob hashes: 129")
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
	// Pin the nested container offsets independently of the decoder.
	prefix := hexutil.MustDecodeHex("0x0400000004000000010500000008000000080a0000")
	require.GreaterOrEqual(t, len(encoded), len(prefix))
	require.Equal(t, prefix, encoded[:len(prefix)])
	decoded := decodeBlobsV4TestResponse(t, encoded)
	require.Equal(t, 1, decoded.Len())
	require.True(t, decoded.Get(0).Available)
	cells, proofs := decoded.Get(0).Contents.cells.UnderlyngReference(), decoded.Get(0).Contents.proofs.UnderlyngReference()
	require.Len(t, cells, 128)
	require.Len(t, proofs, 128)
	for index := range 128 {
		if index == 64 {
			require.Equal(t, []byte(cell), cells[index])
			require.Equal(t, []byte(proof), proofs[index])
		} else {
			require.Empty(t, cells[index])
			require.Empty(t, proofs[index])
		}
	}
}
