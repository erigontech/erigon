// Copyright 2024 The Erigon Authors
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

package downloader

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTorrentClientStatusHandler(t *testing.T) {
	const status = "Torrent client status\n"
	for _, encoding := range []string{"identity", "gzip"} {
		t.Run(encoding, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			handler := torrentClientStatusHandler(func(w io.Writer) {
				require.IsType(t, &bytes.Buffer{}, w, "status must be collected before compression")
				_, err := io.WriteString(w, status)
				require.NoError(t, err)
				require.Zero(t, recorder.Body.Len(), "response writes run while collecting status")
			})
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, TorrentClientStatusPath, nil)
			req.Header.Set("Accept-Encoding", encoding)
			handler.ServeHTTP(recorder, req)

			require.Equal(t, http.StatusOK, recorder.Code)
			if encoding == "identity" {
				require.Empty(t, recorder.Header().Get("Content-Encoding"))
				require.Equal(t, status, recorder.Body.String())
				return
			}

			require.Equal(t, "gzip", recorder.Header().Get("Content-Encoding"))
			reader, err := gzip.NewReader(recorder.Body)
			require.NoError(t, err)
			defer reader.Close()
			body, err := io.ReadAll(reader)
			require.NoError(t, err)
			require.Equal(t, status, string(body))
		})
	}
}
