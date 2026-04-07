package snapcfg

import (
	"fmt"
	"maps"
	"net/http"
)

// cloudflareHeaders are required for R2 CDN access.
// TODO: Copied from github.com/erigontech/erigon-snapshot/embed.go (cloudflareHeaders).
// Remove the copy in erigon-snapshot once this is the canonical location.
var cloudflareHeaders = http.Header{
	"lsjdjwcush6jbnjj3jnjscoscisoc5s": []string{"I%OSJDNFKE783DDHHJD873EFSIVNI7384R78SSJBJBCCJBC32JABBJCBJK45"},
}

// InsertCloudflareHeaders copies the R2 CDN headers into req.
func InsertCloudflareHeaders(req *http.Request) {
	maps.Copy(req.Header, cloudflareHeaders)
}

// ChainTomlR2URL returns the R2 CDN URL for a chain's snapshot TOML.
func ChainTomlR2URL(branch, chain string) string {
	return fmt.Sprintf("https://erigon-snapshots.erigon.network/%s/%s.toml", branch, chain)
}

// ChainTomlGitHubURL returns the GitHub raw URL for a chain's snapshot TOML.
func ChainTomlGitHubURL(branch, chain string) string {
	return fmt.Sprintf("https://raw.githubusercontent.com/erigontech/erigon-snapshot/%s/%s.toml", branch, chain)
}
