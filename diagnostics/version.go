package diagnostics

import (
	"encoding/json"
	"net/http"

	"github.com/ledgerwatch/erigon/params"
)

const Version = 3

func SetupVersionAccess(metricsMux *http.ServeMux) {
	metricsMux.HandleFunc("/version", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(struct {
			Node int    `json:"nodeVersion"`
			Code string `json:"codeVersion"`
			Git  string `json:"gitCommit"`
		}{
			Node: Version,
			Code: params.VersionWithMeta,
			Git:  params.GitCommit,
		})
	})
}
