package jsonrpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestArbAPIImpl_Coinbase(t *testing.T) {
	api := &ArbAPIImpl{}
	_, err := api.Coinbase(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "eth_coinbase")
	require.Contains(t, err.Error(), "not available")
}

func TestArbAPIImpl_Mining(t *testing.T) {
	api := &ArbAPIImpl{}
	mining, err := api.Mining(context.Background())
	require.Error(t, err)
	require.False(t, mining)
	require.Contains(t, err.Error(), "eth_mining")
}

func TestArbAPIImpl_Hashrate(t *testing.T) {
	api := &ArbAPIImpl{}
	hashrate, err := api.Hashrate(context.Background())
	require.Error(t, err)
	require.Equal(t, uint64(0), hashrate)
	require.Contains(t, err.Error(), "eth_hashrate")
}

func TestArbAPIImpl_GetWork(t *testing.T) {
	api := &ArbAPIImpl{}
	work, err := api.GetWork(context.Background())
	require.Error(t, err)
	require.Equal(t, [4]string{}, work)
	require.Contains(t, err.Error(), "eth_getWork")
}

func TestArbAPIImpl_SubmitWork(t *testing.T) {
	api := &ArbAPIImpl{}
	ok, err := api.SubmitWork(context.Background(), [8]byte{}, [32]byte{}, [32]byte{})
	require.Error(t, err)
	require.False(t, ok)
	require.Contains(t, err.Error(), "eth_submitWork")
}

func TestArbAPIImpl_SubmitHashrate(t *testing.T) {
	api := &ArbAPIImpl{}
	ok, err := api.SubmitHashrate(context.Background(), 0, [32]byte{})
	require.Error(t, err)
	require.False(t, ok)
	require.Contains(t, err.Error(), "eth_submitHashrate")
}

func TestArbAPIImpl_ProtocolVersion(t *testing.T) {
	api := &ArbAPIImpl{}
	ver, err := api.ProtocolVersion(context.Background())
	require.Error(t, err)
	require.Equal(t, uint(0), uint(ver))
	require.Contains(t, err.Error(), "eth_protocolVersion")
}

func TestArbAPIImpl_AllMethodsReturnNotAvailable(t *testing.T) {
	api := &ArbAPIImpl{}
	ctx := context.Background()

	methods := []struct {
		name string
		fn   func() error
	}{
		{"Coinbase", func() error { _, err := api.Coinbase(ctx); return err }},
		{"Mining", func() error { _, err := api.Mining(ctx); return err }},
		{"Hashrate", func() error { _, err := api.Hashrate(ctx); return err }},
		{"GetWork", func() error { _, err := api.GetWork(ctx); return err }},
		{"SubmitWork", func() error { _, err := api.SubmitWork(ctx, [8]byte{}, [32]byte{}, [32]byte{}); return err }},
		{"SubmitHashrate", func() error { _, err := api.SubmitHashrate(ctx, 0, [32]byte{}); return err }},
		{"ProtocolVersion", func() error { _, err := api.ProtocolVersion(ctx); return err }},
	}

	for _, m := range methods {
		t.Run(m.name, func(t *testing.T) {
			err := m.fn()
			require.Error(t, err)
			require.Contains(t, err.Error(), "not available")
		})
	}
}
