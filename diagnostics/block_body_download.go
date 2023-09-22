package diagnostics

import (
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/ledgerwatch/erigon/dataflow"
)

func SetupBlockBodyDownload(metricsMux *http.ServeMux) {
	metricsMux.HandleFunc("/debug/metrics/block_body_download", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		writeBlockBodyDownload(w, r)
	})
}

func writeBlockBodyDownload(w io.Writer, r *http.Request) {
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
	dataflow.BlockBodyDownloadStates.ChangesSince(int(tick), w)
}
