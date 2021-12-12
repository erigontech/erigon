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
var Https = withoutBest(strings.Split(https, "\n\n"))

//go:embed trackerslist/trackers_all_http.txt
var http string
var Http = withoutBest(strings.Split(http, "\n\n"))

//go:embed trackerslist/trackers_all_udp.txt
var udp string
var Udp = withoutBest(strings.Split(udp, "\n\n"))

//go:embed trackerslist/trackers_all_ws.txt
var ws string
var Ws = withoutBest(strings.Split(ws, "\n\n"))

func withoutBest(in []string) (res []string) {
Loop:
	for _, tracker := range in {
		for _, bestItem := range Best {
			if tracker == bestItem {
				continue Loop
			}
		}
		res = append(res, tracker)
	}
	return res
}
