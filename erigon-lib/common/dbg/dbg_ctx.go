package dbg

import (
	"context"
)

type debugContextKey struct{}

// Enabling detailed debugging logs for given context
func ContextWithDebug(ctx context.Context, v bool) context.Context {
	return context.WithValue(ctx, debugContextKey{}, v)
}
func Enabled(ctx context.Context) bool {
	v := ctx.Value(debugContextKey{})
	if v == nil {
		return false
	}
	return v.(bool)
}

// https://stackoverflow.com/a/3561399 -> https://www.rfc-editor.org/rfc/rfc6648
// https://stackoverflow.com/a/65241869 -> https://www.odata.org/documentation/odata-version-3-0/abnf/ -> https://docs.oasis-open.org/odata/odata/v4.01/cs01/abnf/odata-abnf-construction-rules.txt
var HTTPHeader = "dbg" // curl --header "dbg: true" www.google.com
