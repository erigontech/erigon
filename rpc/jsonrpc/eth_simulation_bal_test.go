package jsonrpc

import (
	"context"
	"crypto/ecdsa"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
)

func TestSimulateV1BlockAccessListHashAfterAmsterdam(t *testing.T) {
	for _, tc := range []struct {
		name    string
		config  *chain.Config
		wantBAL bool
	}{
		{"amsterdam", chain.AllProtocolChanges, true},
		{"osaka", chain.TestChainOsakaConfig, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, _, bankAddr := fundedBankGenesis(t, tc.config)
			api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)

			to := common.HexToAddress("0x00000000000000000000000000000000c0ffee01")
			gas := hexutil.Uint64(300_000)
			result, err := api.SimulateV1(context.Background(), SimulationRequest{
				BlockStateCalls: []SimulatedBlock{{
					Calls: []ethapi.CallArgs{{From: &bankAddr, To: &to, Value: (*hexutil.U256)(uint256.NewInt(1)), Gas: &gas}},
				}},
			}, rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber))
			require.NoError(t, err)
			require.Len(t, result, 1)
			if tc.wantBAL {
				require.NotNil(t, result[0].BlockAccessListHash)
			} else {
				require.Nil(t, result[0].BlockAccessListHash)
			}
		})
	}
}

type simulateAsGeneratedOptions struct {
	stateOverrides *ethapi.StateOverrides
	omitNonces     bool
}

func simulateAsGenerated(t *testing.T, m *execmoduletester.ExecModuleTester, wants []*types.Block, from common.Address, base rpc.BlockNumberOrHash, opts simulateAsGeneratedOptions) ethapi.RPCBlocks {
	t.Helper()
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)

	blocks := make([]SimulatedBlock, 0, len(wants))
	for _, want := range wants {
		header := want.Header()
		require.True(t, header.Difficulty.IsZero())
		withdrawals := want.Withdrawals()
		gasLimit := hexutil.Uint64(header.GasLimit)
		overrides := &ethapi.BlockOverrides{
			Time:         (*hexutil.Uint64)(&header.Time),
			GasLimit:     &gasLimit,
			FeeRecipient: &header.Coinbase,
			PrevRandao:   &header.MixDigest,
			BeaconRoot:   header.ParentBeaconBlockRoot,
			Withdrawals:  &withdrawals,
		}
		calls := make([]ethapi.CallArgs, 0, len(want.Transactions()))
		for _, txn := range want.Transactions() {
			gas := hexutil.Uint64(txn.GetGasLimit())
			data := hexutil.Bytes(txn.GetData())
			call := ethapi.CallArgs{
				From:                 &from,
				To:                   txn.GetTo(),
				Gas:                  &gas,
				MaxFeePerGas:         (*hexutil.U256)(txn.GetFeeCap()),
				MaxPriorityFeePerGas: (*hexutil.U256)(txn.GetTipCap()),
				Value:                (*hexutil.U256)(txn.GetValue()),
				Input:                &data,
			}
			if !opts.omitNonces {
				nonce := hexutil.Uint64(txn.GetNonce())
				call.Nonce = &nonce
			}
			calls = append(calls, call)
		}
		blocks = append(blocks, SimulatedBlock{BlockOverrides: overrides, Calls: calls})
	}
	blocks[0].StateOverrides = opts.stateOverrides

	result, err := api.SimulateV1(context.Background(), SimulationRequest{BlockStateCalls: blocks, Validation: true}, base)
	require.NoError(t, err)
	require.Len(t, result, len(wants))
	for i, block := range result {
		for j, call := range block.Calls {
			require.Equal(t, uint64(types.ReceiptStatusSuccessful), uint64(call.Status), "block %d call %d: %v", i, j, call.Error)
		}
	}
	return result
}

func generateOracleChain(t *testing.T, m *execmoduletester.ExecModuleTester, key *ecdsa.PrivateKey, from common.Address) *blockgen.ChainPack {
	t.Helper()
	signer := types.LatestSignerForChainID(m.ChainConfig.ChainID)
	feeCap := uint256.NewInt(1_000_000_000_000)
	storingInitCode := []byte{0x60, 0x01, 0x60, 0x00, 0x55, 0x00} // PUSH1 1, PUSH1 0, SSTORE, STOP
	sign := func(nonce uint64, to *common.Address, value uint64, gas uint64, data []byte) types.Transaction {
		txn, err := types.SignTx(&types.DynamicFeeTransaction{
			CommonTx: types.CommonTx{Nonce: nonce, To: to, Value: *uint256.NewInt(value), GasLimit: gas, Data: data},
			ChainID:  *uint256.MustFromBig(m.ChainConfig.ChainID.ToBig()),
			FeeCap:   *feeCap,
		}, *signer, key)
		require.NoError(t, err)
		return txn
	}
	recipient := common.HexToAddress("0x00000000000000000000000000000000c0ffee03")
	chainPack, err := m.GenerateChain(4, func(i int, b *blockgen.BlockGen) {
		nonce := b.TxNonce(from)
		switch i {
		case 0:
			filler := common.HexToAddress("0x00000000000000000000000000000000c0ffee04")
			b.AddTx(sign(nonce, &filler, 5, 300_000, nil))
		case 1:
			b.SetCoinbase(common.HexToAddress("0x00000000000000000000000000000000c0ffee05"))
			b.AddTx(sign(nonce, &recipient, 7, 300_000, nil))
			b.AddTx(sign(nonce+1, nil, 0, 1_000_000, storingInitCode))
			b.AddWithdrawal(&types.Withdrawal{Index: 0, Validator: 1, Address: common.HexToAddress("0x00000000000000000000000000000000c0ffee06"), Amount: 0})
			b.AddWithdrawal(&types.Withdrawal{Index: 1, Validator: 2, Address: recipient, Amount: 3})
		case 2:
			b.AddTx(sign(nonce, &recipient, 11, 300_000, nil))
		case 3:
		}
	})
	require.NoError(t, err)
	return chainPack
}

func TestSimulateV1BlockAccessListMatchesGeneratedBlock(t *testing.T) {
	for _, tc := range []struct {
		name     string
		inserted int
		base     rpc.BlockNumberOrHash
	}{
		{"historical", 4, rpc.BlockNumberOrHashWithNumber(1)},
		{"latest", 1, rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, key, bankAddr := systemContractsGenesis(t, chain.AllProtocolChanges)
			chainPack := generateOracleChain(t, m, key, bankAddr)
			require.NoError(t, m.InsertChain(chainPack.Slice(0, tc.inserted)))
			wants := chainPack.Blocks[1:]

			for _, opts := range []simulateAsGeneratedOptions{{}, {omitNonces: true}} {
				got := simulateAsGenerated(t, m, wants, bankAddr, tc.base, opts)
				require.Equal(t, wants[0].Header().BlockAccessListHash, got[0].BlockAccessListHash)
				require.Equal(t, wants[0].Root(), got[0].StateRoot)
				// A simulated header differs from the generated one (unsigned calls, gas used), so later blocks build on another parent.
				for i := 1; i < len(got); i++ {
					require.NotNil(t, got[i].BlockAccessListHash)
					require.Equal(t, *got[i-1].Hash, got[i].ParentHash)
				}
			}
		})
	}
}

func TestSimulateV1StateOverridesAreNotInBlockAccessList(t *testing.T) {
	m, key, bankAddr := systemContractsGenesis(t, chain.AllProtocolChanges)
	chainPack := generateOracleChain(t, m, key, bankAddr)
	require.NoError(t, m.InsertChain(chainPack.Slice(0, 1)))
	want := chainPack.Blocks[1]

	bankNonce := hexutil.Uint64(1)
	untouchedBalance := (*hexutil.U256)(uint256.NewInt(7))
	untouchedCode := hexutil.Bytes{byte(vm.STOP)}
	got := simulateAsGenerated(t, m, []*types.Block{want}, bankAddr, rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber), simulateAsGeneratedOptions{
		stateOverrides: &ethapi.StateOverrides{
			accounts.InternAddress(bankAddr): {Nonce: &bankNonce},
			accounts.InternAddress(common.HexToAddress("0x00000000000000000000000000000000c0ffee0d")): {
				Balance:   &untouchedBalance,
				Code:      &untouchedCode,
				StateDiff: &map[common.Hash]common.Hash{{1}: {1}},
			},
		},
	})
	require.Equal(t, want.Header().BlockAccessListHash, got[0].BlockAccessListHash)
}

func TestSimulateV1EmptyBlocksHaveSystemCallBlockAccessList(t *testing.T) {
	m, key, bankAddr := systemContractsGenesis(t, chain.AllProtocolChanges)
	chainPack := generateOracleChain(t, m, key, bankAddr)
	require.NoError(t, m.InsertChain(chainPack.Slice(0, 3)))
	want := chainPack.Blocks[3]
	require.Empty(t, want.Transactions())

	got := simulateAsGenerated(t, m, []*types.Block{want}, bankAddr, rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber), simulateAsGeneratedOptions{})
	require.Equal(t, want.Header().BlockAccessListHash, got[0].BlockAccessListHash)
	require.Equal(t, want.Root(), got[0].StateRoot)

	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	latest := rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)
	gapNumber := (*hexutil.U256)(uint256.NewInt(want.NumberU64() + 1))
	withGap, err := api.SimulateV1(context.Background(), SimulationRequest{
		BlockStateCalls: []SimulatedBlock{{BlockOverrides: &ethapi.BlockOverrides{Number: gapNumber}}},
	}, latest)
	require.NoError(t, err)
	require.Len(t, withGap, 2)
	explicit, err := api.SimulateV1(context.Background(), SimulationRequest{
		BlockStateCalls: []SimulatedBlock{{}},
	}, latest)
	require.NoError(t, err)
	require.Len(t, explicit, 1)
	require.NotNil(t, withGap[0].BlockAccessListHash)
	require.Equal(t, explicit[0].BlockAccessListHash, withGap[0].BlockAccessListHash)
	require.Equal(t, explicit[0].StateRoot, withGap[0].StateRoot)
}

func TestSimulateV1BlockAccessListAcrossAmsterdam(t *testing.T) {
	cfg := chain.AllProtocolChanges.Copy()
	cfg.AmsterdamTime = common.NewUint64(20)

	t.Run("pre-fork block matches generated block", func(t *testing.T) {
		m, key, bankAddr := systemContractsGenesis(t, cfg)
		signer := types.LatestSignerForChainID(m.ChainConfig.ChainID)
		to := common.HexToAddress("0x00000000000000000000000000000000c0ffee01")
		chainPack, err := m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) {
			txn, err := types.SignTx(&types.DynamicFeeTransaction{
				CommonTx: types.CommonTx{Nonce: b.TxNonce(bankAddr), To: &to, Value: *uint256.NewInt(1), GasLimit: 300_000},
				ChainID:  *uint256.MustFromBig(m.ChainConfig.ChainID.ToBig()),
				FeeCap:   *uint256.NewInt(1_000_000_000_000),
			}, *signer, key)
			require.NoError(t, err)
			b.AddTx(txn)
		})
		require.NoError(t, err)
		want := chainPack.Blocks[0]
		require.Less(t, want.Time(), *cfg.AmsterdamTime)
		got := simulateAsGenerated(t, m, []*types.Block{want}, bankAddr, rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber), simulateAsGeneratedOptions{})
		require.Nil(t, got[0].BlockAccessListHash)
		require.Equal(t, want.Root(), got[0].StateRoot)
	})

	contract := common.HexToAddress("0x00000000000000000000000000000000c0ffee20")
	for _, base := range simulateBases {
		t.Run("overrides across the fork/"+base.name, func(t *testing.T) {
			crossing := chain.AllProtocolChanges.Copy()
			crossing.AmsterdamTime = common.NewUint64(30)
			m, bankAddr := contractGenesis(t, crossing, contract, types.GenesisAccount{Balance: new(big.Int), Code: slotsReadingCode, Storage: map[common.Hash]common.Hash{{}: {31: 1}, {31: 1}: {31: 7}}})
			api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
			gas := hexutil.Uint64(1_000_000)
			call := []ethapi.CallArgs{{From: &bankAddr, To: &contract, Gas: &gas}}
			stateDiff := func(v byte) *ethapi.StateOverrides {
				return &ethapi.StateOverrides{accounts.InternAddress(contract): {StateDiff: &map[common.Hash]common.Hash{{}: {31: v}, {31: 1}: {31: v + 1}}}}
			}
			blocks := []SimulatedBlock{{StateOverrides: stateDiff(4), Calls: call}, {StateOverrides: stateDiff(9), Calls: call}, {Calls: call}}
			if base.name == "historical" {
				blocks = append([]SimulatedBlock{{}}, blocks...)
			}
			result, err := api.SimulateV1(context.Background(), SimulationRequest{BlockStateCalls: blocks}, base.base)
			require.NoError(t, err)
			result = result[len(result)-3:]
			require.Less(t, uint64(result[0].Timestamp), *crossing.AmsterdamTime)
			require.GreaterOrEqual(t, uint64(result[1].Timestamp), *crossing.AmsterdamTime)
			require.Nil(t, result[0].BlockAccessListHash)
			require.NotNil(t, result[1].BlockAccessListHash)
			require.Equal(t, *result[0].Hash, result[1].ParentHash)
			require.Equal(t, slotWords(4, 5), result[0].Calls[0].ReturnData)
			require.Equal(t, slotWords(9, 10), result[1].Calls[0].ReturnData)
			require.Equal(t, slotWords(1, 10), result[2].Calls[0].ReturnData)
		})
	}
}

// Returns slots 0 and 1, then stores 1 in slot 0.
var slotsReadingCode = []byte{
	byte(vm.PUSH1), 0x00, byte(vm.SLOAD), byte(vm.PUSH1), 0x00, byte(vm.MSTORE),
	byte(vm.PUSH1), 0x01, byte(vm.SLOAD), byte(vm.PUSH1), 0x20, byte(vm.MSTORE),
	byte(vm.PUSH1), 0x01, byte(vm.PUSH1), 0x00, byte(vm.SSTORE),
	byte(vm.PUSH1), 0x40, byte(vm.PUSH1), 0x00, byte(vm.RETURN),
}

func slotWords(slot0, slot1 byte) string {
	return hexutil.Encode(append(common.LeftPadBytes([]byte{slot0}, 32), common.LeftPadBytes([]byte{slot1}, 32)...))
}

func TestSimulateV1StateDiffWrittenBackMatchesNoOverride(t *testing.T) {
	contract := common.HexToAddress("0x00000000000000000000000000000000c0ffee20")
	for _, fork := range []struct {
		name   string
		config *chain.Config
	}{
		{"amsterdam", chain.AllProtocolChanges},
		{"osaka", chain.TestChainOsakaConfig},
	} {
		for _, base := range simulateBases {
			t.Run(fork.name+"/"+base.name, func(t *testing.T) {
				simulate := func(override bool) ethapi.RPCBlocks {
					m, bankAddr := contractGenesis(t, fork.config, contract, types.GenesisAccount{Balance: new(big.Int), Code: slotsReadingCode, Storage: map[common.Hash]common.Hash{{}: {31: 1}, {31: 1}: {31: 7}}})
					api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
					gas := hexutil.Uint64(1_000_000)
					call := []ethapi.CallArgs{{From: &bankAddr, To: &contract, Gas: &gas}}
					first := SimulatedBlock{Calls: call}
					if override {
						first.StateOverrides = &ethapi.StateOverrides{accounts.InternAddress(contract): {StateDiff: &map[common.Hash]common.Hash{{}: {31: 2}}}}
					}
					result, err := api.SimulateV1(context.Background(), SimulationRequest{BlockStateCalls: []SimulatedBlock{first, {Calls: call}}}, base.base)
					require.NoError(t, err)
					require.Len(t, result, 2)
					return result
				}
				got := simulate(true)
				want := simulate(false)
				require.Equal(t, slotWords(2, 7), got[0].Calls[0].ReturnData)
				require.Equal(t, slotWords(1, 7), got[1].Calls[0].ReturnData)
				require.Equal(t, want[0].StateRoot, got[0].StateRoot)
				require.Equal(t, want[1].StateRoot, got[1].StateRoot)
			})
		}
	}
}

func TestSimulateV1TouchedEmptyAccountIsRemovedBeforeNextCall(t *testing.T) {
	authorityKey, err := crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
	require.NoError(t, err)
	authority := crypto.PubkeyToAddress(authorityKey.PublicKey)
	delegate := common.HexToAddress("0x00000000000000000000000000000000c0ffee07")

	authorizationGasUsed := func(t *testing.T, authorityState string) uint64 {
		bankKey, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		require.NoError(t, err)
		bankAddr := crypto.PubkeyToAddress(bankKey.PublicKey)
		alloc := types.GenesisAlloc{bankAddr: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(20), nil)}}
		if authorityState == "genesis" {
			alloc[authority] = types.GenesisAccount{Balance: new(big.Int)}
		}
		m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(&types.Genesis{Config: chain.AllProtocolChanges, Alloc: alloc, GasLimit: 60_000_000}), execmoduletester.WithKey(bankKey))
		if authorityState == "genesis" {
			tx, err := m.DB.BeginTemporalRo(t.Context())
			require.NoError(t, err)
			t.Cleanup(tx.Rollback)
			enc, _, err := tx.GetLatest(kv.AccountsDomain, authority[:], kv.GetLatestOptions{})
			require.NoError(t, err)
			require.NotEmpty(t, enc)
		}
		api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)

		auth, err := types.SignAuthorization(authorityKey, *uint256.MustFromBig(m.ChainConfig.ChainID.ToBig()), delegate, 0)
		require.NoError(t, err)
		var stateOverrides *ethapi.StateOverrides
		if authorityState == "override" {
			zero := hexutil.Uint64(0)
			stateOverrides = &ethapi.StateOverrides{accounts.InternAddress(authority): {Nonce: &zero}}
		}
		gas := hexutil.Uint64(1_000_000)
		result, err := api.SimulateV1(context.Background(), SimulationRequest{
			BlockStateCalls: []SimulatedBlock{{
				StateOverrides: stateOverrides,
				Calls: []ethapi.CallArgs{
					{From: &bankAddr, To: &authority, Gas: &gas},
					{From: &bankAddr, To: &delegate, Gas: &gas, AuthorizationList: []types.JsonAuthorization{types.JsonAuthorization{}.FromAuthorization(auth)}},
				},
			}},
		}, rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber))
		require.NoError(t, err)
		require.Len(t, result, 1)
		calls := result[0].Calls
		require.Len(t, calls, 2)
		for i, call := range calls {
			require.Equal(t, uint64(types.ReceiptStatusSuccessful), uint64(call.Status), "call %d: %v", i, call.Error)
		}
		return uint64(calls[1].GasUsed)
	}

	absent := authorizationGasUsed(t, "absent")
	require.Equal(t, absent, authorizationGasUsed(t, "override"))
	require.Equal(t, absent, authorizationGasUsed(t, "genesis"))
}

func TestSimulateV1SystemCallSelfDestructKeepsBalanceWithoutCalls(t *testing.T) {
	m, _, bankAddr := fundedBankGenesis(t, chain.AllProtocolChanges)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)

	beaconRoots := params.BeaconRootsAddress.Value()
	child := types.CreateAddress(beaconRoots, 1)
	// CREATE a child with 5 wei whose init code is ADDRESS SELFDESTRUCT.
	creatorCode := hexutil.Bytes{
		byte(vm.PUSH2), 0x30, 0xff, byte(vm.PUSH1), 0x00, byte(vm.MSTORE),
		byte(vm.PUSH1), 0x02, byte(vm.PUSH1), 0x1e, byte(vm.PUSH1), 0x05, byte(vm.CREATE), byte(vm.POP), byte(vm.STOP),
	}
	one := hexutil.Uint64(1)
	creatorBalance := (*hexutil.U256)(uint256.NewInt(5))
	balanceReader := common.HexToAddress("0x00000000000000000000000000000000c0ffee0c")
	readerCode := hexutil.Bytes(append(append([]byte{byte(vm.PUSH20)}, child[:]...), byte(vm.BALANCE), byte(vm.PUSH1), 0x00, byte(vm.MSTORE), byte(vm.PUSH1), 0x20, byte(vm.PUSH1), 0x00, byte(vm.RETURN)))
	gas := hexutil.Uint64(100_000)

	result, err := api.SimulateV1(context.Background(), SimulationRequest{
		BlockStateCalls: []SimulatedBlock{
			{
				BlockOverrides: &ethapi.BlockOverrides{
					Withdrawals: &types.Withdrawals{{Index: 0, Validator: 1, Address: child, Amount: 1}},
				},
				StateOverrides: &ethapi.StateOverrides{
					params.BeaconRootsAddress: {Code: &creatorCode, Nonce: &one, Balance: &creatorBalance},
				},
			},
			{
				StateOverrides: &ethapi.StateOverrides{accounts.InternAddress(balanceReader): {Code: &readerCode}},
				Calls:          []ethapi.CallArgs{{From: &bankAddr, To: &balanceReader, Gas: &gas}},
			},
		},
	}, rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber))
	require.NoError(t, err)
	require.Len(t, result, 2)
	require.Len(t, result[1].Calls, 1)
	want := new(big.Int).Add(big.NewInt(5), big.NewInt(1_000_000_000))
	require.Equal(t, hexutil.Encode(common.LeftPadBytes(want.Bytes(), 32)), result[1].Calls[0].ReturnData)
}

func systemContractsGenesis(t *testing.T, cfg *chain.Config) (*execmoduletester.ExecModuleTester, *ecdsa.PrivateKey, common.Address) {
	t.Helper()
	code := func(s string) []byte {
		b, err := hexutil.Decode(s)
		require.NoError(t, err)
		return b
	}
	bankKey, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	bankAddr := crypto.PubkeyToAddress(bankKey.PublicKey)
	alloc := types.GenesisAlloc{
		bankAddr:                                      {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(20), nil)},
		params.BeaconRootsAddress.Value():             {Code: code("0x3373fffffffffffffffffffffffffffffffffffffffe14604d57602036146024575f5ffd5b5f35801560495762001fff810690815414603c575f5ffd5b62001fff01545f5260205ff35b5f5ffd5b62001fff42064281555f359062001fff015500"), Nonce: 1, Balance: new(big.Int)},
		params.HistoryStorageAddress.Value():          {Code: code("0x3373fffffffffffffffffffffffffffffffffffffffe14604657602036036042575f35600143038111604257611fff81430311604257611fff9006545f5260205ff35b5f5ffd5b5f35611fff60014303065500"), Nonce: 1, Balance: new(big.Int)},
		cfg.GetConsolidationRequestContract().Value(): {Code: code("0x3373fffffffffffffffffffffffffffffffffffffffe1460d35760115f54807fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff1461019a57600182026001905f5b5f82111560685781019083028483029004916001019190604d565b9093900492505050366060146088573661019a573461019a575f5260205ff35b341061019a57600154600101600155600354806004026004013381556001015f358155600101602035815560010160403590553360601b5f5260605f60143760745fa0600101600355005b6003546002548082038060021160e7575060025b5f5b8181146101295782810160040260040181607402815460601b815260140181600101548152602001816002015481526020019060030154905260010160e9565b910180921461013b5790600255610146565b90505f6002555f6003555b5f54807fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff141561017357505f5b6001546001828201116101885750505f61018e565b01600190035b5f555f6001556074025ff35b5f5ffd"), Nonce: 1, Balance: new(big.Int)},
		cfg.GetWithdrawalRequestContract().Value():    {Code: code("0x3373fffffffffffffffffffffffffffffffffffffffe1460cb5760115f54807fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff146101f457600182026001905f5b5f82111560685781019083028483029004916001019190604d565b909390049250505036603814608857366101f457346101f4575f5260205ff35b34106101f457600154600101600155600354806003026004013381556001015f35815560010160203590553360601b5f5260385f601437604c5fa0600101600355005b6003546002548082038060101160df575060105b5f5b8181146101835782810160030260040181604c02815460601b8152601401816001015481526020019060020154807fffffffffffffffffffffffffffffffff00000000000000000000000000000000168252906010019060401c908160381c81600701538160301c81600601538160281c81600501538160201c81600401538160181c81600301538160101c81600201538160081c81600101535360010160e1565b910180921461019557906002556101a0565b90505f6002555f6003555b5f54807fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff14156101cd57505f5b6001546002828201116101e25750505f6101e8565b01600290035b5f555f600155604c025ff35b5f5ffd"), Nonce: 1, Balance: new(big.Int)},
	}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(&types.Genesis{Config: cfg, Alloc: alloc, GasLimit: 60_000_000}), execmoduletester.WithKey(bankKey))
	return m, bankKey, bankAddr
}
