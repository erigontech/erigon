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

package cli

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseSocketUrl(t *testing.T) {
	t.Run("sock", func(t *testing.T) {
		socketUrl, err := url.Parse("unix:///some/file/path.sock")
		require.NoError(t, err)
		require.Equal(t, "/some/file/path.sock", socketUrl.Host+socketUrl.EscapedPath())
	})
	t.Run("sock", func(t *testing.T) {
		socketUrl, err := url.Parse("tcp://localhost:1234")
		require.NoError(t, err)
		require.Equal(t, "localhost:1234", socketUrl.Host+socketUrl.EscapedPath())
	})
}
