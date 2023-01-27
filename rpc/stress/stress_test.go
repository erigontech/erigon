package stress

import (
	"context"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ledgerwatch/erigon/rpc"
	"github.com/stretchr/testify/assert"
)

type stressService struct {
}

func (s *stressService) Echo(ctx context.Context, xs string) string {
	return xs
}

func newTestServer() *rpc.Server {
	server := rpc.NewServer(50, false /* traceRequests */, true)
	if err := server.RegisterName("test", new(stressService)); err != nil {
		panic(err)
	}
	return server
}

func stressServer(t *testing.T, cl *rpc.Client, cnt int) {
	wg := sync.WaitGroup{}
	var d uint64
	wg.Add(cnt)
	for i := 0; i < cnt; i++ {
		go func() {
			defer wg.Done()
			err := cl.Call(nil, "test_echo", "foo")
			assert.NoError(t, err)
			atomic.AddUint64(&d, 1)
		}()
	}
	wg.Wait()
	assert.EqualValues(t, cnt, d)
}

func TestStressServerInProc(t *testing.T) {
	srv := newTestServer()
	cl := rpc.DialInProc(srv)
	stressServer(t, cl, 50_000)
}

func TestStressServerSocket(t *testing.T) {
	srv := newTestServer()
	http_srv := httptest.NewServer(srv)
	cl, err := rpc.Dial(http_srv.URL)
	assert.NoError(t, err)
	stressServer(t, cl, 50_000)
}
