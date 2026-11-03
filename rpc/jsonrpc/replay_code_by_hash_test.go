package jsonrpc

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/execution/state"
	tracersConfig "github.com/erigontech/erigon/execution/tracing/tracers/config"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/jsonstream"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

type codeStore map[string][]byte

func (s codeStore) GetCodeByHash(h []byte) ([]byte, bool)  { c, ok := s[string(h)]; return c, ok }
func (s codeStore) PutCodeByHash(h, code []byte, _ uint64) { s[string(h)] = code }

type codeSharingCache struct {
	kvcache.Cache
	store codeStore
}

func (c codeSharingCache) CodeByHash() state.CodeByHash { return c.store }

func TestTraceBlockFillsSharedCodeByHash(t *testing.T) {
	m, chain, _ := rpcdaemontest.CreateTestExecModule(t)
	cache := codeSharingCache{Cache: kvcache.New(kvcache.DefaultCoherentConfig), store: codeStore{}}
	baseApi := NewBaseApi(nil, cache, m.BlockReader, m.Engine, &rpccfg.BaseApiConfig{Dirs: m.Dirs})
	api := NewPrivateDebugAPI(baseApi, m.DB, nil, &rpccfg.DebugApiConfig{})

	for n := rpc.BlockNumber(1); n <= rpc.BlockNumber(chain.Length()); n++ {
		var buf bytes.Buffer
		s := jsonstream.New(&buf)
		require.NoError(t, api.TraceBlockByNumber(m.Ctx, n, &tracersConfig.TraceConfig{}, s))
		require.NoError(t, s.Flush())
	}
	require.NotEmpty(t, cache.store, "a block replay must fill the shared code store")
}
