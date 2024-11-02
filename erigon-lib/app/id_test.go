package app_test

import (
	"strconv"
	"testing"

	"github.com/erigontech/erigon-lib/app"
	"github.com/stretchr/testify/require"
)

func TestCreateDomains(t *testing.T) {
	for i := 0; i < 10; i++ {
		d, err := app.NewDomain[int]()
		require.NoError(t, err)
		require.NotNil(t, d)
		require.Equal(t, d.Id().String(), strconv.Itoa(i))
	}

	d, err := app.NewNamedDomain[int]("test")
	require.NoError(t, err)
	require.NotNil(t, d)
	require.Equal(t, d.Id().String(), "test")
	d1, err := app.NewNamedDomain[int]("test")
	require.NoError(t, err)
	require.NotNil(t, d1)
	require.Equal(t, d.Id().String(), "test")
	require.True(t, d == d1)
}
