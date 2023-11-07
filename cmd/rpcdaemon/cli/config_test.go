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
		require.EqualValues(t, "/some/file/path.sock", socketUrl.Host+socketUrl.EscapedPath())
	})
	t.Run("sock", func(t *testing.T) {
		socketUrl, err := url.Parse("tcp://localhost:1234")
		require.NoError(t, err)
		require.EqualValues(t, "localhost:1234", socketUrl.Host+socketUrl.EscapedPath())
	})
}
