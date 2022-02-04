package trackers

import (
	_ "embed"
	"strings"
)

//go:embed trackerslist/trackers_best.txt
var best string
var Best = first10(secure(strings.Split(best, "\n\n")))

//go:embed trackerslist/trackers_all_https.txt
var https string
var Https = first10(withoutBest(secure(strings.Split(https, "\n\n"))))

//go:embed trackerslist/trackers_all_udp.txt
var udp string
var Udp = first10(withoutBest(secure(strings.Split(udp, "\n\n"))))

//go:embed trackerslist/trackers_all_ws.txt
var ws string
var Ws = first10(withoutBest(secure(strings.Split(ws, "\n\n"))))

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
func first10(in []string) (res []string) {
	for i, tracker := range in {
		if i >= 10 {
			break
		}
		res = append(res, tracker)
	}
	return res
}
func secure(in []string) (res []string) {
	for _, tracker := range in {
		//skip unsecure protocols
		if strings.HasPrefix(tracker, "ws://") || strings.HasPrefix(tracker, "http://") {
			continue
		}
		res = append(res, tracker)
	}
	return res
}
