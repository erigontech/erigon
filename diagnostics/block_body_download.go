package diagnostics

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/ledgerwatch/erigon/dataflow"
)

func SetupBlockBodyDownload() {
	http.HandleFunc("/debug/metrics/block_body_download", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		writeBlockBodyDownload(w, r)
	})
}

func writeBlockBodyDownload(w io.Writer, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		fmt.Fprintf(w, "ERROR: parsing arguments: %v\n", err)
		return
	}
	sinceMilliStr := r.Form.Get("sincemilli")
	var sinceMilli int64
	if sinceMilliStr != "" {
		var err error
		if sinceMilli, err = strconv.ParseInt(sinceMilliStr, 10, 64); err != nil {
			fmt.Fprintf(w, "ERROR: parsing sincemilli: %v\n", err)
		}
	}
	fmt.Fprintf(w, "SUCCESS\n")
	dataflow.BlockBodyDownloadStates.ChangesSince(time.UnixMilli(sinceMilli), w)
}
