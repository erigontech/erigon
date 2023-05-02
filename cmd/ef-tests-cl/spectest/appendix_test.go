package spectest

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadTestCases(t *testing.T) {
	root := os.DirFS("./data_faketest/tests")
	cases, err := ReadTestCases(root)
	require.Nil(t, err)
	require.Len(t, cases.Slice(), 20)

	require.Len(t, cases.Filter(func(t TestCase) bool {
		return t.HandlerName == "eth_aggregate_pubkeys"
	}).Slice(), 8)
	require.Len(t, cases.Filter(func(t TestCase) bool {
		return t.HandlerName == "eth_fast_aggregate_verify"
	}).Slice(), 12)
}
