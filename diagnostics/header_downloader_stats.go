package diagnostics

import (
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/ledgerwatch/erigon/dataflow"
)

func SetupHeaderDownloadStats(metricsMux *http.ServeMux) {
	metricsMux.HandleFunc("/debug/metrics/headers_download", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
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
