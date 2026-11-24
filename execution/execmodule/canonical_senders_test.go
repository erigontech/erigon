package execmodule_test

import (
	"context"
	"crypto/ecdsa"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/types"
)

func TestAnAgreedBlockInsertedBeforeItsRoundsStoresNoSendersOfAPartialRound(t *testing.T) {
	replica := newRoundHarness(t)
	proposer := newRoundHarnessWithKeys(t, replica.keys)
	genesis, err := replica.m.ExecModule.CurrentHeader(replica.ctx)
	require.NoError(t, err)

	rounds := [][][]byte{
		{replica.signed(0, 0, nil, 100_000, emitsALog)},
		{replica.signed(0, 1, nil, 100_000, emitsALog), replica.signed(1, 0, nil, 100_000, emitsALog)},
	}
	paramsP, inP := proposer.attrs(genesis)
	proposed := proposer.sealRounds(paramsP, inP, rounds)

	status, err := insertBlocks(replica.ctx, replica.m.ExecModule, []*types.Block{proposed})
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)

	params, in := replica.attrs(genesis)
	for _, round := range rounds {
		_, _, vr, err := replica.m.ExecModule.PreExecuteFlashblock(replica.ctx, in, round)
		require.NoError(t, err)
		require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, vr.ValidationError)
	}
	br, err := replica.m.ExecModule.SealBlock(replica.ctx, params, false)
	require.NoError(t, err)
	require.Equal(t, proposed.Hash(), br.Block.Hash())
	require.NoError(t, insertValidateAndUfc1By1(replica.ctx, replica.m.ExecModule, []*types.Block{proposed}))

	require.NoError(t, replica.m.DB.View(replica.ctx, func(tx kv.Tx) error {
		senders, err := rawdb.ReadSenders(tx, proposed.Hash(), proposed.NumberU64())
		require.NoError(t, err)
		if len(senders) > 0 {
			require.Len(t, senders, len(proposed.Transactions()))
		}
		return nil
	}))
}

func TestBlocksPreExecutedBeforeTheirPredecessorIsAcceptedStoreWholeSenders(t *testing.T) {
	replica := newRoundHarness(t)
	proposer := newRoundHarnessWithKeys(t, replica.keys)
	parent, err := replica.m.ExecModule.CurrentHeader(replica.ctx)
	require.NoError(t, err)

	var proposed []*types.Block
	var rounds [][][][]byte
	for n := uint64(0); n < 3; n++ {
		blockRounds := [][][]byte{
			{replica.signed(0, 2*n, nil, 100_000, emitsALog)},
			{replica.signed(0, 2*n+1, nil, 100_000, emitsALog), replica.signed(1, n, nil, 100_000, emitsALog)},
		}
		paramsP, inP := proposer.attrs(parent)
		b := proposer.sealRounds(paramsP, inP, blockRounds)
		proposed, rounds, parent = append(proposed, b), append(rounds, blockRounds), b.Header()
	}

	parent, err = replica.m.ExecModule.CurrentHeader(replica.ctx)
	require.NoError(t, err)
	for i, b := range proposed {
		params, in := replica.attrs(parent)
		for _, round := range rounds[i] {
			_, _, vr, err := replica.m.ExecModule.PreExecuteFlashblock(replica.ctx, in, round)
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, vr.ValidationError)
		}
		br, err := replica.m.ExecModule.SealBlock(replica.ctx, params, false)
		require.NoError(t, err)
		require.Equal(t, b.Hash(), br.Block.Hash())
		if i > 0 {
			require.NoError(t, insertValidateAndUfc1By1(replica.ctx, replica.m.ExecModule, []*types.Block{proposed[i-1]}))
		}
		parent = b.Header()
	}
	require.NoError(t, insertValidateAndUfc1By1(replica.ctx, replica.m.ExecModule, proposed[len(proposed)-1:]))

	signer := types.LatestSignerForChainID(replica.m.ChainConfig.ChainID)
	require.NoError(t, replica.m.DB.View(replica.ctx, func(tx kv.Tx) error {
		for _, b := range proposed {
			senders, err := rawdb.ReadSenders(tx, b.Hash(), b.NumberU64())
			require.NoError(t, err)
			if len(senders) == 0 {
				continue
			}
			require.Len(t, senders, len(b.Transactions()), "block %d", b.NumberU64())
			for i, txn := range b.Transactions() {
				want, err := txn.Sender(*signer)
				require.NoError(t, err)
				require.Equal(t, want.Value(), senders[i], "block %d txn %d", b.NumberU64(), i)
			}
		}
		return nil
	}))
}

// missingHeaders records every header lookup that came back empty for a hash it watches.
type missingHeaders struct {
	dbservices.FullBlockReader
	mu      sync.Mutex
	watched map[common.Hash]bool
	missed  []uint64
}

func (r *missingHeaders) Header(ctx context.Context, tx kv.Getter, hash common.Hash, number uint64) (*types.Header, error) {
	h, err := r.FullBlockReader.Header(ctx, tx, hash, number)
	r.mu.Lock()
	defer r.mu.Unlock()
	if h == nil && err == nil && r.watched[hash] {
		r.missed = append(r.missed, number)
	}
	return h, err
}

func TestARoundTwoGenerationsAboveTheDurableHeadReadsEveryCanonicalHeader(t *testing.T) {
	reader := &missingHeaders{watched: map[common.Hash]bool{}}
	var keys [2]*ecdsa.PrivateKey
	for i := range keys {
		k, err := crypto.GenerateKey()
		require.NoError(t, err)
		keys[i] = k
	}
	replica := newRoundHarnessWithOptions(t, keys, execmoduletester.WithBlockReaderWrap(func(br dbservices.FullBlockReader) dbservices.FullBlockReader {
		reader.FullBlockReader = br
		return reader
	}))
	parent, err := replica.m.ExecModule.CurrentHeader(replica.ctx)
	require.NoError(t, err)
	for n := uint64(0); n < 3; n++ {
		params, in := replica.attrs(parent)
		_, _, vr, err := replica.m.ExecModule.PreExecuteFlashblock(replica.ctx, in, [][]byte{replica.signed(0, n, nil, 100_000, emitsALog)})
		require.NoError(t, err)
		require.Equal(t, execmodule.ExecutionStatusSuccess, vr.ValidationStatus, vr.ValidationError)
		if n == 2 {
			break
		}
		br, err := replica.m.ExecModule.SealBlock(replica.ctx, params, false)
		require.NoError(t, err)
		reader.mu.Lock()
		reader.watched[br.Block.Hash()] = true
		reader.mu.Unlock()
		parent = br.Block.Header()
	}
	reader.mu.Lock()
	defer reader.mu.Unlock()
	require.Empty(t, reader.missed, "a round could not read the header of a canonical block below it")
}
