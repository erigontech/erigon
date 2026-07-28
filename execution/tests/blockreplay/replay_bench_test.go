package blockreplay_test

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/protocol/rules/merge"
	"github.com/erigontech/erigon/execution/tests/blockreplay"
)

func loadFixture(tb testing.TB, block string) *blockreplay.Fixture {
	tb.Helper()
	// The fixture is committed to testdata/; a missing file is a packaging
	// regression, so fail loudly rather than skip (skips also fall outside the
	// repo's allowed test-skip cases).
	p := filepath.Join("testdata", "block-"+block+".gob")
	fx, err := blockreplay.Load(p)
	require.NoError(tb, err)
	return fx
}

func TestReplayMainnetBlock(t *testing.T) {
	fx := loadFixture(t, "25604144")
	engine := merge.New(ethash.NewFaker())
	defer engine.Close()
	res, err := blockreplay.Replay(fx, chainspec.Mainnet.Config, engine, 0, log.New())
	require.NoError(t, err)
	require.NotNil(t, res)
}

// BenchmarkReplayMainnetBlock is an isolated, repeatable per-block exec
// benchmark: no DB, no commitment. BLOCKREPLAY_READ_NS models a per-storage-read
// latency, since the in-mem reader is otherwise free.
func BenchmarkReplayMainnetBlock(b *testing.B) {
	fx := loadFixture(b, "25604144")
	engine := merge.New(ethash.NewFaker())
	defer engine.Close()
	logger := log.New()

	var readNanos int64
	if v := os.Getenv("BLOCKREPLAY_READ_NS"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		require.NoError(b, err)
		readNanos = n
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := blockreplay.Replay(fx, chainspec.Mainnet.Config, engine, readNanos, logger); err != nil {
			b.Fatal(err)
		}
	}
}
