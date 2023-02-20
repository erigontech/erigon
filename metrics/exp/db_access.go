package exp

import (
	"io"
	"net/http"

	"github.com/urfave/cli/v2"
)

func SetupDbAccess(ctx *cli.Context) {
	http.HandleFunc("/debug/metrics/db/list", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		writeDbList(w)
	})
	http.HandleFunc("/debug/metrics/db/read", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		writeDbRead(w, r)
	})
}

func writeDbList(w io.Writer) {

}

func writeDbRead(w io.Writer, r *http.Request) {

}
