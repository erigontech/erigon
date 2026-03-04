package snapcfg

import (
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
