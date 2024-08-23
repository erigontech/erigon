package utils

import (
	"strings"
	"github.com/ledgerwatch/erigon/params"
	"fmt"
)

func GetVersion() string {
	var version string
	if strings.Contains(params.GitTag, "tags") || strings.Contains(params.GitTag, "release") {
		version = params.GitTag[strings.LastIndex(params.GitTag, "/")+1:]
	} else {
		if params.GitBranch != "" {
			version = fmt.Sprintf("2.0-%s-%s", params.GitBranch, params.GitCommit)
		} else {
			version = "2.0-dev"
		}
	}
	return version
}
