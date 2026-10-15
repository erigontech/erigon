package stagedsync

import (
	"bytes"
	"context"
	"math/big"
	"slices"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/execctx/execctxapi"
	"github.com/erigontech/erigon/execution/blockreplay"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/protocol/rules/merge"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/state/genesiswrite"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/node/ethconfig"
)

// Genesis occupies txNums 0 and 1; block 1 then spans 2 (block start), 3 (tx0),
// 4 (tx1) and 5 (block end).
const (
	resumeBALGenesisTxNum    = uint64(1)
	resumeBALBlockStartTxNum = uint64(2)
)

// resumeBALFixture is a two-transaction Amsterdam block over a tiny genesis:
// tx0 is a plain transfer and tx1 stores the recipient's balance, so both the
// resumed prefix and the suffix touch state the block access list records.
type resumeBALFixture struct {
	config  *chain.Config
	engine  rules.Engine
	alloc   types.GenesisAlloc
	genesis *types.Block
	block   *types.Block
	senders []common.Address
}

func newResumeBALFixture(t *testing.T) *resumeBALFixture {
	t.Helper()
	config := chain.TestChainOsakaConfig.Copy()
	config.AmsterdamTime = common.NewUint64(10)

	sender := senderIsCoinbaseKey.rawAddress
	target := common.HexToAddress("0x0000000000000000000000000000000000000a11")
	observer := common.HexToAddress("0x0000000000000000000000000000000000000b22")
	observerCode := append([]byte{byte(vm.PUSH20)}, target[:]...)
	observerCode = append(observerCode, byte(vm.BALANCE), byte(vm.PUSH1), 0, byte(vm.SSTORE), byte(vm.STOP))
	alloc := types.GenesisAlloc{
		sender:   {Balance: big.NewInt(1_000_000_000_000_000_000)},
		observer: {Balance: new(big.Int), Nonce: 1, Code: observerCode},
	}
	for _, contract := range []accounts.Address{
		config.GetWithdrawalRequestContract(), config.GetConsolidationRequestContract(),
		config.GetBuilderDepositContract(), config.GetBuilderExitContract(),
	} {
		alloc[contract.Value()] = types.GenesisAccount{Balance: new(big.Int), Nonce: 1, Code: []byte{byte(vm.STOP)}}
	}
	genesis, genesisState, err := genesiswrite.GenesisToBlock(&types.Genesis{Config: config, GasLimit: 30_000_000, Alloc: alloc}, datadir.New(t.TempDir()), log.New())
	require.NoError(t, err)
	genesisState.Close()

	engine := merge.New(ethash.NewFaker())
	t.Cleanup(func() { engine.Close() })
	fx := &resumeBALFixture{config: config, engine: engine, alloc: alloc, genesis: genesis, senders: []common.Address{sender, sender}}

	signer := types.LatestSignerForChainID(config.ChainID)
	gasPrice := uint256.NewInt(1_000_000_000)
	var txs types.Transactions
	for _, txn := range []*types.LegacyTx{
		{CommonTx: types.CommonTx{Nonce: 0, To: &target, Value: *uint256.NewInt(1), GasLimit: 21_000}, GasPrice: *gasPrice},
		{CommonTx: types.CommonTx{Nonce: 1, To: &observer, GasLimit: 100_000}, GasPrice: *gasPrice},
	} {
		signed, err := types.SignTx(txn, *signer, senderIsCoinbaseKey.key)
		require.NoError(t, err)
		txs = append(txs, signed)
	}
	fx.block = fx.assembleBlock(t, txs, common.HexToAddress("0x000000000000000000000000000000000000c01b"))
	return fx
}

func allocAccount(ga types.GenesisAccount) accounts.Account {
	acc := accounts.NewAccount()
	acc.Nonce = ga.Nonce
	acc.Balance = *uint256.MustFromBig(ga.Balance)
	if len(ga.Code) > 0 {
		acc.Incarnation = state.FirstContractIncarnation
		acc.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(ga.Code))
	}
	return acc
}

// seed writes the genesis allocation as block 0 and, for a resumed run, the
// state tx0 left behind, checkpointing the commitment where execution resumes.
func (fx *resumeBALFixture) seed(t *testing.T, db kv.TemporalRwDB, resumeAfterTx0 bool) {
	t.Helper()
	ctx := context.Background()
	err := db.UpdateTemporal(ctx, func(tx kv.TemporalRwTx) error {
		domains, err := execctx.NewSharedDomains(ctx, tx, log.New(), execctx.WithParaTrieDB(db))
		if err != nil {
			return err
		}
		defer domains.Close()
		commitBlock, commitTxNum := uint64(0), resumeBALGenesisTxNum
		if resumeAfterTx0 {
			commitBlock, commitTxNum = 1, resumeBALBlockStartTxNum+1
		}
		domains.SetTxNum(commitTxNum)
		put := func(addr common.Address, acc accounts.Account, code []byte, txNum uint64) error {
			if len(code) > 0 {
				if err := domains.DomainPut(kv.CodeDomain, tx, addr[:], code, txNum, nil); err != nil {
					return err
				}
			}
			return domains.DomainPut(kv.AccountsDomain, tx, addr[:], accounts.SerialiseV3(&acc), txNum, nil)
		}
		for addr, ga := range fx.alloc {
			if err := put(addr, allocAccount(ga), ga.Code, resumeBALGenesisTxNum); err != nil {
				return err
			}
		}
		if err := rawdbv3.TxNums.Append(tx, 0, resumeBALGenesisTxNum); err != nil {
			return err
		}
		if resumeAfterTx0 {
			if err := rawdbv3.TxNums.Append(tx, 1, resumeBALBlockStartTxNum+uint64(len(fx.block.Transactions()))+1); err != nil {
				return err
			}
			bal := fx.block.BlockAccessList()
			for i := range bal {
				if acc, changed := fx.postTx0Account(t, &bal[i]); changed {
					if err := put(bal[i].Address, acc, nil, commitTxNum); err != nil {
						return err
					}
				}
			}
		}
		root, err := domains.ComputeCommitment(ctx, tx, true, commitBlock, commitTxNum, "seed", nil)
		if err != nil {
			return err
		}
		if !resumeAfterTx0 {
			require.Equal(t, fx.genesis.Root(), common.BytesToHash(root))
		}
		return domains.Flush(ctx, tx)
	})
	require.NoError(t, err)
}

// postTx0Account derives an account's state after tx0 from the block access
// list entries at access index 1.
func (fx *resumeBALFixture) postTx0Account(t *testing.T, changes *types.AccountChanges) (accounts.Account, bool) {
	t.Helper()
	const tx0Index = 1
	acc := accounts.NewAccount()
	if ga, ok := fx.alloc[changes.Address]; ok {
		acc = allocAccount(ga)
	}
	changed := false
	for _, bc := range changes.BalanceChanges {
		if bc.Index == tx0Index {
			acc.Balance = bc.Value
			changed = true
		}
	}
	for _, nc := range changes.NonceChanges {
		if nc.Index == tx0Index {
			acc.Nonce = nc.Value
			changed = true
		}
	}
	for _, cc := range changes.CodeChanges {
		require.NotEqual(t, uint32(tx0Index), cc.Index, "tx0 is a plain transfer and must not change code")
	}
	for _, sc := range changes.StorageChanges {
		for _, c := range sc.Changes {
			require.NotEqual(t, uint32(tx0Index), c.Index, "tx0 is a plain transfer and must not change storage")
		}
	}
	return acc, changed
}

// assembleBlock builds block 1 with the production block assembler, so the
// header carries the real state root, receipts and block access list hash.
func (fx *resumeBALFixture) assembleBlock(t *testing.T, txs types.Transactions, coinbase common.Address) *types.Block {
	t.Helper()
	ctx := context.Background()
	logger := log.New()
	db := newResumeTestDB(t)
	fx.seed(t, db, false)
	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		return rawdb.WriteTd(tx, fx.genesis.Hash(), 0, *uint256.NewInt(0))
	}))

	parent := fx.genesis.Header()
	header := types.NewEmptyHeaderForAssembling()
	header.ParentHash = fx.genesis.Hash()
	header.Coinbase = coinbase
	header.Root = parent.Root
	header.Number = *uint256.NewInt(1)
	header.GasLimit = parent.GasLimit
	header.Time = *fx.config.AmsterdamTime
	header.BaseFee = misc.CalcBaseFee(fx.config, parent)
	excessBlobGas := misc.CalcExcessBlobGas(fx.config, parent, header.Time)
	header.ExcessBlobGas = &excessBlobGas
	header.BlobGasUsed = new(uint64)
	header.ParentBeaconBlockRoot = &common.Hash{}
	slot := header.Number.Uint64()
	header.SlotNumber = &slot

	tx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	domains, err := execctx.NewSharedDomains(ctx, tx, logger)
	require.NoError(t, err)
	defer domains.Close()
	txNum, _, err := domains.SeekCommitment(ctx, tx)
	require.NoError(t, err)
	domains.SetTxNum(txNum)
	stateReader := state.NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{}))
	ibs := state.New(stateReader)
	defer ibs.Close()
	ibs.SetVersionMap(state.NewVersionMap(nil))
	ibs.SetTxContext(header.Number.Uint64(), -1)

	ba := exec.NewBlockAssembler(exec.AssemblerCfg{ChainConfig: fx.config, Engine: fx.engine}, &exec.AssembledBlock{
		Header:      header,
		Uncles:      []*types.Header{},
		Txns:        types.Transactions{},
		Receipts:    types.Receipts{},
		Withdrawals: []*types.Withdrawal{},
	})
	require.True(t, ba.HasBAL())
	require.NoError(t, ba.Initialize(ibs, tx, logger))
	_, _, err = ba.AddTransactions(ctx, nil, txs, accounts.InternAddress(coinbase), &vm.Config{}, ibs, nil, "resume-bal", logger)
	require.NoError(t, err)
	require.Len(t, ba.Txns, len(txs))
	assembled, err := ba.AssembleBlock(ibs, tx, logger)
	require.NoError(t, err)

	blockContext := protocol.NewEVMBlockContext(header, protocol.GetHashFn(header, nil), fx.engine, accounts.NilAddress, fx.config)
	blockRules := blockContext.Rules(fx.config)
	domainStorageKeys := state.CommittedStorageKeysFn(domains, tx)
	for i, ws := range ba.BalIO().Outputs() {
		if ws == nil || ws.IsEmpty() {
			continue
		}
		normalized, err := ws.Normalize(ibs.VersionMap(), i-1, 0, stateReader, domainStorageKeys, fx.config.IsEIP161Enabled(1), fx.config.Aura != nil, blockRules.IsAmsterdam)
		require.NoError(t, err)
		require.NoError(t, normalized.Apply(domains, tx, 1, txNum, nil, blockRules, nil, false))
	}
	root, err := domains.ComputeCommitment(ctx, tx, false, 1, txNum, "resume-bal", nil)
	require.NoError(t, err)

	finalHeader := types.CopyHeader(assembled.HeaderNoCopy())
	finalHeader.Root = common.BytesToHash(root)
	block := types.NewBlockForAsembling(finalHeader, ba.Txns, ba.Uncles, ba.Receipts, ba.Withdrawals, types.NewBlockAccessListSidecar(ba.BlockAccessList))
	block.SendersToTxs(fx.senders)
	require.True(t, fx.config.IsAmsterdam(block.Time()))
	require.NotEmpty(t, block.BlockAccessList())
	return block
}

// tamperedBlock commits to an access list with an extra untouched account, so
// the transactions and the state root stay unchanged and only the access list
// check can tell the block apart.
func (fx *resumeBALFixture) tamperedBlock(t *testing.T) *types.Block {
	t.Helper()
	bal := append(fx.block.BlockAccessList().Copy(), types.AccountChanges{Address: common.HexToAddress("0x0000000000000000000000000000000000000c33")})
	slices.SortFunc(bal, func(a, b types.AccountChanges) int { return bytes.Compare(a.Address[:], b.Address[:]) })
	require.NotEqual(t, fx.block.BlockAccessList().Hash(), bal.Hash())
	header := types.CopyHeader(fx.block.HeaderNoCopy())
	balHash := bal.Hash()
	header.BlockAccessListHash = &balHash
	block := types.NewBlockFromNetwork(header, &types.Body{
		Transactions: fx.block.Transactions(),
		Uncles:       fx.block.Uncles(),
		Withdrawals:  fx.block.Withdrawals(),
	}, types.NewBlockAccessListSidecar(bal))
	block.SendersToTxs(fx.senders)
	return block
}

// exec runs block through the parallel executor, from its start or resumed
// after tx0, and returns the invalid-block verdict, if any.
func (fx *resumeBALFixture) exec(t *testing.T, block *types.Block, resumeAfterTx0 bool) *blockVerdict {
	t.Helper()
	ctx := context.Background()
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	db := newResumeTestDB(t)
	fx.seed(t, db, resumeAfterTx0)

	blockRLP, err := rlp.EncodeToBytes(block)
	require.NoError(t, err)
	parentRLP, err := rlp.EncodeToBytes(fx.genesis.Header())
	require.NoError(t, err)
	balBytes, err := types.EncodeBlockAccessListBytes(block.BlockAccessList())
	require.NoError(t, err)
	senders := make([][20]byte, len(fx.senders))
	for i, sender := range fx.senders {
		senders[i] = sender
	}
	blockReader, err := blockreplay.NewMemBlockReader(&blockreplay.Fixture{
		BlockRLP: blockRLP, ParentHeaderRLP: parentRLP, Senders: senders, BALBytes: balBytes,
	})
	require.NoError(t, err)

	syncCfg := ethconfig.Defaults.Sync
	syncCfg.ExecWorkerCount = 2
	cfg := StageExecuteBlocksCfg(db, prune.DefaultMode, 512*datasize.MB, fx.config, fx.engine, &vm.Config{}, nil, false, false, dirs, blockReader, nil, syncCfg, false, nil)

	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	t.Cleanup(rwTx.Rollback)
	domains, err := execctx.NewSharedDomains(ctx, rwTx, logger)
	require.NoError(t, err)
	t.Cleanup(domains.Close)
	domains.SetTxNum(resumeBALBlockStartTxNum)

	rng := execRange{blockNum: 1, initialTxNum: resumeBALGenesisTxNum, inputTxNum: resumeBALBlockStartTxNum, maxBlockNum: 1}
	if resumeAfterTx0 {
		rng.initialTxNum = resumeBALBlockStartTxNum + 1
		rng.offsetFromBlockBeginning = 1
	}
	src := &singleBlockSource{block: block, num: 1, parent: fx.genesis.Header(), bal: block.BlockAccessList()}
	out, err := execV3(ctx, cfg, domains, rwTx, stages.ModeForkValidation, false, "resume-bal", rng, src, logger)
	require.NoError(t, err)
	return out.verdict
}

func requireAccepted(t *testing.T, verdict *blockVerdict, msg string) {
	t.Helper()
	if verdict != nil {
		t.Fatalf("%s: %v", msg, verdict.err)
	}
}

func requireAccessListMismatch(t *testing.T, verdict *blockVerdict, msg string) {
	t.Helper()
	require.NotNil(t, verdict, msg)
	require.ErrorContains(t, verdict.err, "block access list mismatch", msg)
}

// TestResumedBlockValidatesBlockAccessList pins that a block resumed after a
// mid-block commit still has its EIP-7928 block access list checked against
// the header, exactly like a block executed from its start.
func TestResumedBlockValidatesBlockAccessList(t *testing.T) {
	fx := newResumeBALFixture(t)
	tampered := fx.tamperedBlock(t)

	requireAccepted(t, fx.exec(t, fx.block, false), "full execution must accept the block")
	requireAccessListMismatch(t, fx.exec(t, tampered, false), "full execution must reject the tampered access list")

	requireAccepted(t, fx.exec(t, fx.block, true), "the resumed block must still be accepted")
	requireAccessListMismatch(t, fx.exec(t, tampered, true), "the resumed block must reject the tampered access list")
}
