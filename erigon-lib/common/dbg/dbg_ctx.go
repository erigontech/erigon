// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

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
