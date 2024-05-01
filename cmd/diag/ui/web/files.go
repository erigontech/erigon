package web

import (
	"net/http"

	"github.com/ledgerwatch/erigon/cmd/diag/ui/web/dist"
)

var UI = http.FileServer(http.FS(dist.FS))
