package metrics

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

func parseMetric(s string) (string, prometheus.Labels, error) {
	if len(s) == 0 {
		return "", nil, fmt.Errorf("metric cannot be empty")
	}
	n := strings.IndexByte(s, '{')
	if n < 0 {
		if err := validateIdent(s); err != nil {
			return "", nil, err
		}

		return s, nil, nil
	}
	ident := s[:n]
	s = s[n+1:]
	if err := validateIdent(ident); err != nil {
		return "", nil, err
	}
	if len(s) == 0 || s[len(s)-1] != '}' {
		return "", nil, fmt.Errorf("missing closing curly brace at the end of %q", ident)
	}

	tags, err := parseTags(s[:len(s)-1])

	if err != nil {
		return "", nil, err
	}

	return ident, tags, nil
}

func parseTags(s string) (prometheus.Labels, error) {
	if len(s) == 0 {
		return nil, nil
	}

	var labels prometheus.Labels

	for {
		n := strings.IndexByte(s, '=')
		if n < 0 {
			return nil, fmt.Errorf("missing `=` after %q", s)
		}
		ident := s[:n]
		s = s[n+1:]
		if err := validateIdent(ident); err != nil {
			return nil, err
		}
		if len(s) == 0 || s[0] != '"' {
			return nil, fmt.Errorf("missing starting `\"` for %q value; tail=%q", ident, s)
		}
		s = s[1:]

		value := ""

		for {
			n = strings.IndexByte(s, '"')
			if n < 0 {
				return nil, fmt.Errorf("missing trailing `\"` for %q value; tail=%q", ident, s)
			}
			m := n
			for m > 0 && s[m-1] == '\\' {
				m--
			}
			if (n-m)%2 == 1 {
				value = value + s[:n]
				s = s[n+1:]
				continue
			}
			value = value + s[:n]
			if labels == nil {
				labels = prometheus.Labels{}
			}
			labels[ident] = value
			s = s[n+1:]
			if len(s) == 0 {
				return labels, nil
			}
			if !strings.HasPrefix(s, ",") {
				return nil, fmt.Errorf("missing `,` after %q value; tail=%q", ident, s)
			}
			s = skipSpace(s[1:])
			break
		}
	}
}

func skipSpace(s string) string {
	for len(s) > 0 && s[0] == ' ' {
		s = s[1:]
	}
	return s
}

func validateIdent(s string) error {
	if !identRegexp.MatchString(s) {
		return fmt.Errorf("invalid identifier %q", s)
	}
	return nil
}

var identRegexp = regexp.MustCompile("^[a-zA-Z_:.][a-zA-Z0-9_:.]*$")
