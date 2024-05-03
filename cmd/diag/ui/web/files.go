package web

import (
	"embed"
	"io/fs"
	"net/http"

	"github.com/ledgerwatch/erigon/cmd/diag/ui/web/dist"
)

//go:embed all:dist
var assets embed.FS

func Assets() (fs.FS, error) {
	return fs.Sub(assets, "dist")
}

var UI = http.FileServer(http.FS(dist.FS))
