package trackers

import (
	_ "embed"
	"strings"
)

//go:embed trackerslist/trackers_best.txt
var best string
var Best = strings.Split(best, "\n\n")

//go:embed trackerslist/trackers_all_https.txt
var https string
var Https = strings.Split(https, "\n\n")

//go:embed trackerslist/trackers_all_http.txt
var http string
var Http = strings.Split(http, "\n\n")

//go:embed trackerslist/trackers_all_udp.txt
var udp string
var Udp = strings.Split(udp, "\n\n")

//go:embed trackerslist/trackers_all_ws.txt
var ws string
var Ws = strings.Split(ws, "\n\n")

func First(n int, in []string) (res []string) {
	if n <= len(in) {
		return in[:n]
	}
	return in
}
