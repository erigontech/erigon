package trackers

import (
	"bufio"
	"strings"

	"github.com/ledgerwatch/trackerslist"
)

var (
	Best  = split(trackerslist.Best)
	Https = split(trackerslist.Https)
	Http  = split(trackerslist.Http)
	Udp   = split(trackerslist.Udp)
	Ws    = split(trackerslist.Ws)
)

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
