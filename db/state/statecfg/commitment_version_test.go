package statecfg

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/version"
)

func TestCommitmentKVWriteVersion(t *testing.T) {
	t.Parallel()

	require.Equal(t, version.V2_2, commitmentKVWriteVersion(&DomainCfg{ReplaceKeysInValues: false}),
		"plain commitment must stamp v2.2")
	require.Equal(t, version.V2_1, commitmentKVWriteVersion(&DomainCfg{ReplaceKeysInValues: true}),
		"referenced commitment must stamp v2.1")

	require.NotNil(t, Schema.CommitmentDomain.KVWriteVersion, "commitment domain must wire the KVWriteVersion hook")
	require.Equal(t, version.V2_2, Schema.CommitmentDomain.KVWriteVersion(&Schema.CommitmentDomain),
		"default (plain) commitment must stamp v2.2")
}

func TestCommitmentKVReadCeilingAcceptsBoth(t *testing.T) {
	t.Parallel()

	dataKV := Schema.CommitmentDomain.FileVersion.DataKV
	require.Equal(t, version.V2_2, dataKV.Current, "read ceiling must be v2.2")
	require.True(t, dataKV.Supports(version.V2_1), "must still read v2.1 referenced files")
	require.True(t, dataKV.Supports(version.V2_2), "must read v2.2 plain files")
}
