package trackers

import (
	_ "embed"
	"strings"
)

//go:embed trackerslist/trackers_best.txt
var best string
var Best = first5(strings.Split(best, "\n\n"))

//go:embed trackerslist/trackers_all_https.txt
var https string
var Https = first5(strings.Split(https, "\n\n"))

//go:embed trackerslist/trackers_all_http.txt
var http string
var Http = first5(strings.Split(http, "\n\n"))

//go:embed trackerslist/trackers_all_udp.txt
var udp string
var Udp = first5(strings.Split(udp, "\n\n"))

//go:embed trackerslist/trackers_all_ws.txt
var ws string
var Ws = first5(strings.Split(ws, "\n\n"))

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
func first5(in []string) (res []string) {
	for i, tracker := range in {
		if i >= 5 {
			break
		}
		res = append(res, tracker)
	}
	return res
}
