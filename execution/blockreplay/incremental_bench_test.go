package blockreplay

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/protocol/rules/merge"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

type incrementalReplay struct {
	block    *types.Block
	fixture  *Fixture
	engine   rules.Engine
	state    *state.IntraBlockState
	reader   state.StateReader
	writer   state.StateWriter
	chain    rules.ChainReader
	gas      *protocol.GasUsed
	pool     *protocol.GasPool
	receipts types.Receipts
	sealed   *types.Block
}

func newIncrementalReplay(tb testing.TB, fx *Fixture) *incrementalReplay {
	tb.Helper()
	block, err := fx.Block()
	require.NoError(tb, err)
	chainReader, err := newFixtureChainReader(chainspec.Mainnet.Config, fx)
	require.NoError(tb, err)
	engine := merge.New(ethash.NewFaker())
	writer := state.NewNoopWriter()
	reader := NewInMemReader(fx)
	ibs := state.New(reader)
	require.NoError(tb, protocol.InitializeBlockExecution(
		engine,
		chainReader,
		block.Header(),
		chainspec.Mainnet.Config,
		ibs,
		writer,
		log.New(),
		nil,
	))

	pool := new(protocol.GasPool)
	pool.AddGas(block.GasLimit()).AddBlobGas(chainspec.Mainnet.Config.GetMaxBlobGasPerBlock(block.Time()))
	return &incrementalReplay{
		block: block, fixture: fx, engine: engine, state: ibs, reader: reader,
		writer: writer, chain: chainReader, gas: new(protocol.GasUsed), pool: pool,
		receipts: make(types.Receipts, 0, block.Transactions().Len()),
	}
}

func (r *incrementalReplay) close() {
	r.state.Close()
	r.engine.Close()
}

func (r *incrementalReplay) execute(from, to int) error {
	txs := r.block.Transactions()
	blockHashFunc := func(n uint64) (common.Hash, error) {
		return common.Hash(r.fixture.Ancestors[n]), nil
	}
	for i := from; i < to; i++ {
		tx := txs[i]
		r.state.SetTxContext(r.block.NumberU64(), i)
		receipt, err := protocol.ApplyTransaction(
			chainspec.Mainnet.Config,
			blockHashFunc,
			r.engine,
			accounts.NilAddress,
			r.pool,
			r.state,
			r.writer,
			r.block.Header(),
			tx,
			r.gas,
			vm.Config{},
		)
		if err != nil {
			return err
		}
		r.receipts = append(r.receipts, receipt)
	}
	return nil
}

func (r *incrementalReplay) seal() error {
	block, _, err := protocol.FinalizeBlockExecution(
		r.engine,
		r.reader,
		r.block.Header(),
		r.block.Transactions(),
		r.block.Uncles(),
		r.writer,
		chainspec.Mainnet.Config,
		r.state,
		r.receipts,
		r.block.Withdrawals(),
		r.chain,
		true,
		log.New(),
		nil,
	)
	r.sealed = block
	return err
}

func (r *incrementalReplay) verify(tb testing.TB) {
	tb.Helper()
	require.Equal(tb, r.block.GasUsed(), r.gas.BlockGasUsed())
	if r.sealed != nil {
		require.Equal(tb, r.block.Hash(), r.sealed.Hash())
	}
}

func TestIncrementalReplayMatchesBlockGas(t *testing.T) {
	fx, err := Load("testdata/block-25604144.gob")
	require.NoError(t, err)
	r := newIncrementalReplay(t, fx)
	defer r.close()

	cut := r.block.Transactions().Len() - 8
	require.NoError(t, r.execute(0, cut))
	require.NoError(t, r.execute(cut, r.block.Transactions().Len()))
	require.NoError(t, r.seal())
	r.verify(t)
}

func BenchmarkIncrementalReplay(b *testing.B) {
	fx, err := Load("testdata/block-25604144.gob")
	require.NoError(b, err)
	block, err := fx.Block()
	require.NoError(b, err)
	txCount := block.Transactions().Len()

	b.Run("cold-full-block", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			b.StopTimer()
			r := newIncrementalReplay(b, fx)
			b.StartTimer()
			require.NoError(b, r.execute(0, txCount))
			b.StopTimer()
			r.verify(b)
			r.close()
		}
	})

	for _, tail := range []int{1, 8, 32} {
		b.Run("warm-tail-"+strconv.Itoa(tail), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				b.StopTimer()
				r := newIncrementalReplay(b, fx)
				require.NoError(b, r.execute(0, txCount-tail))
				b.StartTimer()
				require.NoError(b, r.execute(txCount-tail, txCount))
				b.StopTimer()
				r.verify(b)
				r.close()
			}
		})
	}

	b.Run("candidate", func(b *testing.B) {
		for _, tail := range []int{txCount, 1, 8, 32} {
			b.Run("tail-"+strconv.Itoa(tail), func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					b.StopTimer()
					r := newIncrementalReplay(b, fx)
					require.NoError(b, r.execute(0, txCount-tail))
					b.StartTimer()
					require.NoError(b, r.execute(txCount-tail, txCount))
					require.NoError(b, r.seal())
					b.StopTimer()
					r.verify(b)
					r.close()
				}
			})
		}
	})
}
