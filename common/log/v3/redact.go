package log

import (
	"regexp"
	"strings"
)

// Precompiled regexes for redaction
var (
	reHTTP    = regexp.MustCompile(`(?i)http://\S+`)
	reHTTPS   = regexp.MustCompile(`(?i)https://\S+`)
	reWS      = regexp.MustCompile(`(?i)ws://\S+`)
	reWSS     = regexp.MustCompile(`(?i)wss://\S+`)
	reIPv4    = regexp.MustCompile(`(^|[^\w-])((?:\d{1,3}\.){3}\d{1,3}(?::\d{1,5})?)\b`)
	reIPv6    = regexp.MustCompile(`(^|[^\w-])(\[[0-9a-fA-F:]+\](?::\d{1,5})?)\b`)
	reDatadir = regexp.MustCompile(`(-{1,2}datadir[=\s]+)\S+`)
)

// RedactArgs redacts sensitive information like HTTP(S), WS(S) urls and IP addresses from command line arguments
func RedactArgs(args []string) string {
	if len(args) == 0 {
		return ""
	}

	// Make a copy to avoid modifying the original slice
	redacted := make([]string, len(args))
	copy(redacted, args)

	// Replace args[0] (executable path) with just "erigon" to avoid exposing sensitive paths
	redacted[0] = "erigon"

	s := strings.Join(redacted, " ")
	return RedactString(s)
}

// RedactString redacts sensitive substrings in the provided string.
func RedactString(s string) string {
	// Redact URLs
	s = reHTTP.ReplaceAllString(s, "http://<redacted>")
	s = reHTTPS.ReplaceAllString(s, "https://<redacted>")
	s = reWS.ReplaceAllString(s, "ws://<redacted>")
	s = reWSS.ReplaceAllString(s, "wss://<redacted>")

	// Redact datadir paths
	s = reDatadir.ReplaceAllString(s, "${1}<redacted-dir>")

	// redact IPs
	s = reIPv6.ReplaceAllString(s, "$1<redacted-ipv6>")
	s = reIPv4.ReplaceAllString(s, "$1<redacted-ip>")
	return s
}
