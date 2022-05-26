package trackers

import (
	"bufio"
	_ "embed"
	"strings"
)

//go:embed trackerslist/trackers_best.txt
var best string
var Best = split(best)

//go:embed trackerslist/trackers_all_https.txt
var https string
var Https = split(https)

//go:embed trackerslist/trackers_all_http.txt
var http string
var Http = split(http)

//go:embed trackerslist/trackers_all_udp.txt
var udp string
var Udp = split(udp)

//go:embed trackerslist/trackers_all_ws.txt
var ws string
var Ws = split(ws)

func split(txt string) (lines []string) {
	sc := bufio.NewScanner(strings.NewReader(txt))
	for sc.Scan() {
		l := sc.Text()
		l = strings.TrimSpace(l)
		if len(l) == 0 {
			continue
		}
		lines = append(lines, sc.Text())
	}

	if err := sc.Err(); err != nil {
		panic(err)
	}
	return lines
}

func First(n int, in []string) (res []string) {
	if n <= len(in) {
		return in[:n]
	}
	return in
}
