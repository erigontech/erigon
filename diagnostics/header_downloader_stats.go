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

package diagnostics

import (
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/erigontech/erigon/execution/dataflow"
)

func SetupHeaderDownloadStats(metricsMux *http.ServeMux) {
	if metricsMux == nil {
		return
	}

	metricsMux.HandleFunc("/headers_download", func(w http.ResponseWriter, r *http.Request) {
		writeHeaderDownload(w, r)
	})
}

func writeHeaderDownload(w io.Writer, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		fmt.Fprintf(w, "ERROR: parsing arguments: %v\n", err)
		return
	}
	sinceTickStr := r.Form.Get("sincetick")
	var tick int64
	if sinceTickStr != "" {
		var err error
		if tick, err = strconv.ParseInt(sinceTickStr, 10, 64); err != nil {
			fmt.Fprintf(w, "ERROR: parsing sincemilli: %v\n", err)
		}
	}
	fmt.Fprintf(w, "SUCCESS\n")
	// fmt.Fprintf(w, "%d,%d\n", p2p.ingressTrafficMeter, )
	dataflow.HeaderDownloadStates.ChangesSince(int(tick), w)
}
