// package stages

import (
	"context"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/memdb"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/eth/stagedsync"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	"github.com/erigontech/erigon/smt/pkg/db"
	"github.com/erigontech/erigon/zk/contracts"
	"github.com/erigontech/erigon/zk/hermez_db"
	"github.com/erigontech/erigon/zk/l1infotree"
	"github.com/erigontech/erigon/zk/syncer"
	"github.com/erigontech/erigon/zk/syncer/mocks"
	"github.com/iden3/go-iden3-crypto/keccak256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// Shared test scaffolding

type stageEnv struct {
	ctx         context.Context
	db          kv.RwDB
	tx          kv.RwTx
	hdb         *hermez_db.HermezDb
	em          *mocks.MockIEtherman
	l1Syncer    *syncer.L1Syncer
	updater     *l1infotree.Updater
	cfg         L1InfoTreeCfg
	blockNumber *big.Int
	parentHash  common.Hash
	blockTime   uint64
	contracts   []common.Address
}

func newStageEnv(t *testing.T) *stageEnv {
	t.Helper()

	ctx := context.Background()
	db1 := memdb.NewTestDB(t)
	tx := memdb.BeginRw(t, db1)
	require.NoError(t, hermez_db.CreateHermezBuckets(tx))
	require.NoError(t, db.CreateEriDbBuckets(tx))

	hdb := hermez_db.NewHermezDb(tx)

	// Start progress just before our target block
	latest := uint64(1000)
	require.NoError(t, stages.SaveStageProgress(tx, stages.L1InfoTree, latest))

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	em := mocks.NewMockIEtherman(ctrl)

	cntrcts := []common.Address{
		common.HexToAddress("0x1000"),
	}
	topics := [][]common.Hash{
		// The syncer uses these to construct FilterLogs; we match AnyTimes() in mocks
		{contracts.UpdateL1InfoTreeTopic, contracts.UpdateL1InfoTreeV2Topic},
	}

	parent := common.HexToHash("0xabc")
	ts := uint64(time.Now().Unix())
	bn := big.NewInt(int64(latest + 1))
	header := &types.Header{ParentHash: parent, Number: bn, Time: ts}
	block := types.NewBlockWithHeader(header)

	// Remove default HeaderByNumber here; keep BlockByNumber
	em.EXPECT().BlockByNumber(gomock.Any(), gomock.Any()).Return(block, nil).AnyTimes()

	l1 := syncer.NewL1Syncer(ctx, []syncer.IEtherman{em}, cntrcts, topics, 10000, 0, "latest", 0)
	updater := l1infotree.NewUpdater(ctx, &ethconfig.Zk{L1FirstBlock: latest + 1}, l1, l1infotree.NewInfoTreeL2RpcSyncer(ctx, &ethconfig.Zk{
		L2RpcUrl: "http://127.0.0.1:8545",
	}))
	cfg := StageL1InfoTreeCfg(db1, &ethconfig.Zk{}, updater)

	return &stageEnv{
		ctx:         ctx,
		db:          db1,
		tx:          tx,
		hdb:         hdb,
		em:          em,
		l1Syncer:    l1,
		updater:     updater,
		cfg:         cfg,
		blockNumber: bn,
		parentHash:  parent,
		blockTime:   ts,
		contracts:   cntrcts,
	}
}

func runStageOnce(t *testing.T, env *stageEnv) error {
	t.Helper()
	s := &stagedsync.StageState{ID: stages.L1InfoTree}
	u := &stagedsync.Sync{}
	return SpawnL1InfoTreeStage(s, u, env.tx, env.cfg, env.ctx, log.New())
}

// Helpers for encoding/index/roots

func u256Bytes(n uint64) []byte {
	var out [32]byte
	b := new(big.Int).SetUint64(n).Bytes()
	copy(out[32-len(b):], b)
	return out[:]
}

func computeGERFromV1Log(l types.Log) common.Hash {
	// GER = keccak(mainnetExitRoot || rollupExitRoot)
	combined := append(l.Topics[1].Bytes(), l.Topics[2].Bytes()...)
	return common.BytesToHash(keccak256.Hash(combined))
}

func leafFromV1(l types.Log, parent common.Hash, ts uint64) [32]byte {
	ger := computeGERFromV1Log(l)
	return l1infotree.HashLeafData(ger, parent, ts)
}

func makeV1Log(addr common.Address, block uint64, txIndex, logIndex uint) types.Log {
	// V1: topics[0]=V1 topic; topics[1]=mainnet root; topics[2]=rollup root; data = uint256 index
	mainnetExitRoot := common.HexToHash(fmt.Sprintf("0x%064x", block+uint64(txIndex)+0x111))
	rollupExitRoot := common.HexToHash(fmt.Sprintf("0x%064x", block+uint64(txIndex)+0x222))
	data := u256Bytes(uint64(logIndex / 2)) // logical index for V1 leaf
	return types.Log{
		BlockNumber: block,
		Address:     addr,
		Topics:      []common.Hash{contracts.UpdateL1InfoTreeTopic, mainnetExitRoot, rollupExitRoot},
		Data:        data,
		TxIndex:     txIndex,
		Index:       logIndex,
	}
}

func makeV2Log(addr common.Address, block uint64, txIndex, logIndex uint, leafCount uint64, root common.Hash) types.Log {
	// V2: topics[0]=V2 topic; topics[1]=leafCount (uint256=index+1); data[0:32]=root
	return types.Log{
		BlockNumber: block,
		Address:     addr,
		Topics:      []common.Hash{contracts.UpdateL1InfoTreeV2Topic, common.BigToHash(new(big.Int).SetUint64(leafCount))},
		Data:        root.Bytes(), // exactly 32 bytes
		TxIndex:     txIndex,
		Index:       logIndex,
	}
}

// Build V1/V2 pairs with V2.root equal to the trie root after inserting the previous V1 leaf.
func makeV1V2Pairs(t *testing.T, n int, addr common.Address, block uint64, parent common.Hash, ts uint64) []types.Log {
	t.Helper()
	logs := make([]types.Log, 0, n*2)

	// Local trie to compute expected roots (same logic as production)
	tree, err := l1infotree.NewL1InfoTree(32, nil)
	require.NoError(t, err)

	for i := 0; i < n; i++ {
		txIdx := uint(i)
		v1Idx := uint(2 * i)
		v2Idx := v1Idx + 1

		v1 := makeV1Log(addr, block, txIdx, v1Idx)
		leaf := leafFromV1(v1, parent, ts)

		newRoot, err := tree.AddLeaf(uint32(i), leaf)
		require.NoError(t, err)

		v2 := makeV2Log(addr, block, txIdx, v2Idx, uint64(i+1), common.BytesToHash(newRoot[:]))

		logs = append(logs, v1, v2)
	}

	return logs
}

func expectHeaderOK(env *stageEnv, header *types.Header) {
	env.em.EXPECT().HeaderByNumber(gomock.Any(), env.blockNumber).Return(header, nil).AnyTimes()
}

func expectHeaderErrorOnce(env *stageEnv) {
	env.em.EXPECT().HeaderByNumber(gomock.Any(), env.blockNumber).Return(nil, fmt.Errorf("header error")).Times(1)
}

// Tests

func TestSpawnL1InfoTreeStage_HappyPath(t *testing.T) {
	env := newStageEnv(t)

	const n = 10
	logs := makeV1V2Pairs(t, n, env.contracts[0], env.blockNumber.Uint64(), env.parentHash, env.blockTime)

	// Provide logs
	env.em.EXPECT().FilterLogs(gomock.Any(), gomock.Any()).Return(logs, nil).AnyTimes()
	expectHeaderOK(env, &types.Header{ParentHash: env.parentHash, Number: env.blockNumber, Time: env.blockTime})

	err := runStageOnce(t, env)
	require.NoError(t, err)

	// Only V1 logs create leaves
	leaves, err := env.hdb.GetAllL1InfoTreeLeaves()
	require.NoError(t, err)
	assert.Len(t, leaves, n)

	// Progress advanced to the block containing logs
	progress, err := stages.GetStageProgress(env.tx, stages.L1InfoTree)
	require.NoError(t, err)
	assert.Equal(t, env.blockNumber.Uint64(), progress)
}

func TestSpawnL1InfoTreeStage_UnhappyPath_SkipV1Log(t *testing.T) {
	env := newStageEnv(t)

	const n = 8
	all := makeV1V2Pairs(t, n, env.contracts[0], env.blockNumber.Uint64(), env.parentHash, env.blockTime)

	// Drop one V1 (txIndex==3), keep its V2
	skippedTxIndex := 3
	filtered := make([]types.Log, 0, len(all)-1)
	for _, l := range all {
		if l.Topics[0] == contracts.UpdateL1InfoTreeTopic && l.TxIndex == uint(skippedTxIndex) {
			continue
		}
		filtered = append(filtered, l)
	}

	env.em.EXPECT().FilterLogs(gomock.Any(), gomock.Any()).Return(filtered, nil).AnyTimes()
	expectHeaderOK(env, &types.Header{ParentHash: env.parentHash, Number: env.blockNumber, Time: env.blockTime})

	// Stage will notice mismatch on V2 and attempt rollback; we accept an early return with no error or an error depending on implementation.
	_ = runStageOnce(t, env)

	leaves, err := env.hdb.GetAllL1InfoTreeLeaves()
	require.NoError(t, err)
	// We will rollback the tree up to the confirmed index - skippedTxIndex
	assert.Equal(t, skippedTxIndex, len(leaves))
}

func TestSpawnL1InfoTreeStage_UnhappyPath_GetHeaderFails(t *testing.T) {
	env := newStageEnv(t)

	logs := makeV1V2Pairs(t, 1, env.contracts[0], env.blockNumber.Uint64(), env.parentHash, env.blockTime)
	env.em.EXPECT().FilterLogs(gomock.Any(), gomock.Any()).Return(logs, nil).AnyTimes()

	// Expect header error
	expectHeaderErrorOnce(env)

	err := runStageOnce(t, env)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "header error")
}

func TestSpawnL1InfoTreeStage_UnhappyPath_FilterLogsFails(t *testing.T) {
	env := newStageEnv(t)

	env.em.EXPECT().FilterLogs(gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("filter logs error")).AnyTimes()

	err := runStageOnce(t, env)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "filter logs error")
}
