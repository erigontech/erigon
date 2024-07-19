package diagnostics_test

import (
	"encoding/json"
	"testing"

	"github.com/erigontech/erigon-lib/diagnostics"
	"github.com/stretchr/testify/require"
)

func TestParseData(t *testing.T) {
	var data []byte
	var v diagnostics.RAMInfo
	diagnostics.ParseData(data, v)
	require.Equal(t, diagnostics.RAMInfo{}, v)

	newv := diagnostics.RAMInfo{
		Total: 1,
		Free:  2,
	}

	data, err := json.Marshal(newv)
	require.NoError(t, err)

	diagnostics.ParseData(data, &v)
	require.Equal(t, newv, v)
}
