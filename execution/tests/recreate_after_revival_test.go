// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package executiontests

import (
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/program"
	"github.com/erigontech/erigon/node/ethconfig"
)

// setExecWorkers sets the executor worker count for the test.
func setExecWorkers(t *testing.T, workers int) {
	t.Helper()
	prev := ethconfig.Defaults.Sync.ExecWorkerCount
	t.Cleanup(func() { ethconfig.Defaults.Sync.ExecWorkerCount = prev })
	ethconfig.Defaults.Sync.ExecWorkerCount = workers
}

// generatedBlockWant pins the root and gas of generated blocks, as the removed serial executor computed them.
// Generation and import share the versioned state code, so the missing-entry hint checks nothing: check a new
// row against an independent source (another client, or the expected state worked out by hand).
var generatedBlockWant = map[string]struct {
	root string
	gas  uint64
}{
	"TestBurntContractDestroyedByItsTx/cancun_contract_created_in_the_tx":               {"0x69b6fc2e46395b49b0fc02c9cd85dbb4603f3421098d89495d62d0d1dffe8d11", 60662},
	"TestBurntContractDestroyedByItsTx/pre-cancun_deployed_contract":                    {"0x28291d881c1948415865119fc3f0f3dcdfa56143df21ca6997482d726ed04902", 28603},
	"TestBurntFeeBurnedWithDestroyedRecipient/created_in_the_same_tx":                   {"0x51c89c029a7bbbc9d4460e7c2427a04cc04adfd1b35ed01080eab0bb14807be6", 101196},
	"TestBurntFeeBurnedWithDestroyedRecipient/existing_contract_before_Cancun":          {"0x00dfe16ebaf960b2a54b086388a1fba61a95300aa569a19c1c8031869f114cb5", 47002},
	"TestBurntFeeIsVisibleToLaterBalanceReads/amsterdam":                                {"0x027d3ca69541add473f80ec2146098693c69042f3245ce6ab4f321152ab410b8", 3133440},
	"TestBurntFeeIsVisibleToLaterBalanceReads/london":                                   {"0x7563357c60eb56bb3d18c32a711309ed4882c44c6dfad3853ff365afbfccbbec", 2134560},
	"TestBurntFeeIsVisibleToLaterBalanceReads/prague":                                   {"0x6a6011fd874825babbadaffc8804837419a0d0711964b5c9cc5dc710b1147830", 2134560},
	"TestBurntFeeKeepsTouchedEmptyRecipient/london":                                     {"0x03e2f50cfa3406887f657499545b930598496ecfd959fcd49b21e763f3ff1873", 23620},
	"TestBurntFeeKeepsTouchedEmptyRecipient/prague":                                     {"0x4b927ff612190493092c88b0c2034de086a1c66f9b720594a3e0ebe281a25380", 23620},
	"TestBurntFeeKeepsTouchedEmptyRecipient/prague_without_storage":                     {"0x855a41e31f5ebd8d237db7c277adf5a7555b2b2029fa8e908541d7a9250d01d1", 23620},
	"TestCodeHashReadAfterRecreate/berlin":                                              {"0xee66f6f5cd449baa3bb21d72a8e7888d9e96357074f204ad1bf022d838648996", 992637},
	"TestCodeHashReadAfterRecreate/shanghai":                                            {"0xbb1c78e677d89f32b0a9713c78ebc962e86b884d82c1f3653651ee44a3566701", 1005646},
	"TestCodeReadAcrossDestructAndRevival/berlin/call":                                  {"0xb635e45745d8eac096199c7ba183e515a70a1aa47e9eecaed121db33bfdfdb2b", 971220},
	"TestCodeReadAcrossDestructAndRevival/berlin/destruct":                              {"0x3e976202378b977d8ea7745e3d099f650a02cc48b910172b5f9db53f670891b6", 945503},
	"TestCodeReadAcrossDestructAndRevival/shanghai/call":                                {"0x2fe36b9904091bea0c930effad4b7ebebd8d642b1cf58443542523e777d2842e", 995222},
	"TestCodeReadAcrossDestructAndRevival/shanghai/destruct":                            {"0x9d4d0b482da2d20df66a2dcf69bbe0d9961cec68a6308c11fc61186bbf81929c", 993505},
	"TestCodeReadAfterRecreateOverCallRevival/berlin":                                   {"0xd93a730442d41a1ef894a3caae35b66fc85fa76cc31334000a47a24b2e4fcb9c", 1041893},
	"TestCodeReadAfterRecreateOverCallRevival/shanghai":                                 {"0xa49b24a6b569357d523c2ddf732cc001d0d562f01b3f3e401fec554e71fb393a", 1054902},
	"TestCodeReadAfterRecreateOverDestructCredit/osaka":                                 {"0x20a6ee682515055eb89f31e7b3354b85f8d971b7b475f6299707e41ed31e191d", 1056926},
	"TestCodeReadAfterRecreateOverDestructCredit/shanghai":                              {"0x413d6cec4b71c663296ec14330c39047eb3cb9169ccdfc7ed594c6b529b53b09", 1056926},
	"TestCodeSizeAfterDestructOfExistingContract/berlin":                                {"0x21623180efb5b695bb14d699aa3307cbc9735718a7cd70bd4c9d8fa0cb9c893d", 1254937},
	"TestCodeSizeAfterDestructOfExistingContract/shanghai":                              {"0xc4152bc2a9c06555e05bff66d298b4b442462207052e5a16b23a1633055d5c5c", 1278937},
	"TestCoinbaseRecreatedWithCodeAfterFeeRevival/amsterdam=false":                      {"0xfab54ebf19462690c0c59b1b5535c8f21c6270b00cb39b7067e1234a50a7ae48", 141446},
	"TestCoinbaseRecreatedWithCodeAfterFeeRevival/amsterdam=true":                       {"0x29d9c0b0bbd07c2a06f0e2c55ea1f04676ab9c710b08389f4c9765b3cc556d06", 232560},
	"TestCreate2AfterCoinbaseDestructAndFeeRevival":                                     {"0x9f5bca819162e640333546263ed762c3d3ed5b65f0130b1c242ed6b73c517e60", 908647},
	"TestCreate2AfterDestructAndTransferRevival":                                        {"0x9ac7dae57dab901312f629da58429619436b41ed1da83348ba3a48587444810a", 185647},
	"TestCreateAfterSameTxRevival/osaka":                                                {"0xa7158442c8b79c9264115e2c6c56a57f62e5f4a150169bd59f80afeb3b674e01", 176348},
	"TestCreateAfterSameTxRevival/shanghai":                                             {"0xa7158442c8b79c9264115e2c6c56a57f62e5f4a150169bd59f80afeb3b674e01", 176348},
	"TestCreditedEmptyAccountIsKept/burnt-fee_recipient_with_a_non-zero_burn/amsterdam": {"0x35f5a7f038bfd5a4d33cb7e28ca02099e9ed963d06ed83201dafcf1b24fcbb3d", 15000},
	"TestCreditedEmptyAccountIsKept/burnt-fee_recipient_with_a_non-zero_burn/prague":    {"0xa01525e207b13e270ed886711327f863269d1f940fd4d0fb58c01ff4ec8b421f", 21000},
	"TestCreditedEmptyAccountIsKept/delegated_authority/amsterdam":                      {"0x7e29a7e2c1ac2409a325af860f632d4bbc547266f462cd3a96a30f0bb3cb0938", 35190},
	"TestCreditedEmptyAccountIsKept/delegated_authority/prague":                         {"0xe75ab61004d2c80a2b821719e68630b01f89561b9cf1de54de896d54b901da23", 36800},
	"TestCreditedEmptyAccountIsKept/tipped_coinbase/amsterdam":                          {"0x1d4bacc661430b8a3b19c2b5769f2733d6837148c01321cacb4b623254b07adf", 15000},
	"TestCreditedEmptyAccountIsKept/tipped_coinbase/prague":                             {"0x6cb6c6687c6d604cdaa2ff314bd94a4b6f778a532987852c3562c51b54125675", 21000},
	"TestCreditedEmptyAccountIsKept/value_call/amsterdam":                               {"0x34f3de35fc1de9b305678716b564f027d71788d10a6d610c6e74f64a98dcf574", 183600},
	"TestCreditedEmptyAccountIsKept/value_call/prague":                                  {"0x87a9d76605f9800e1251ab6ab8ea9e19a0a538c282a187f40722777c63a31491", 55320},
	"TestDroppedNetZeroWrite":                                                           {"0x658c99d04fb32f9cfd318febd4543a8395ff039618f260537879dfe27298a003", 1297980},
	"TestEIP8246CoinbaseSelfdestructPreservesBalance":                                   {"0xf0ab463ec68d3d0ee3ce4b16fa4416289b51e30619b790516ac5f68920eb4d10", 183600},
	"TestEIP8246FundedThenSelfdestructedAccount/workers=1":                              {"0x5b0f55b567bf4f2d38a84319121094cb597c0b70d738e8953c936014e90dde38", 183600},
	"TestEIP8246FundedThenSelfdestructedAccount/workers=4":                              {"0x5b0f55b567bf4f2d38a84319121094cb597c0b70d738e8953c936014e90dde38", 183600},
	"TestEIP8246PreserveToNonSelfBeneficiaryThenCredited/workers=1":                     {"0xc618810598298363b14ed044142939326321fb178632b6f126c0257d6bee9802", 550800},
	"TestEIP8246PreserveToNonSelfBeneficiaryThenCredited/workers=4":                     {"0xc618810598298363b14ed044142939326321fb178632b6f126c0257d6bee9802", 550800},
	"TestEmptiedContractIsKept/amsterdam":                                               {"0x5eebd76796247ca6375a5e606dc21baec1f4633ff2139eec8098f52cb7f50aca", 183600},
	"TestEmptiedContractIsKept/prague":                                                  {"0x9e3fe459f4675dcb45a8e65940cc5196b1d3637d9e23f10b65c7900d8d95c069", 64642},
	"TestFeeRevivingTouchedRecipientIsVisibleToLaterReads/burnt/amsterdam":              {"0xb783fa7ef727c5000fc892b87e2986c9d29dbc17f450401118ccd294756e4dd0", 3035520},
	"TestFeeRevivingTouchedRecipientIsVisibleToLaterReads/burnt/london":                 {"0xd0f79bda6a1fc76765777aca6d20196b31b244edb774d3dbaa17ea6b8270839c", 1440475},
	"TestFeeRevivingTouchedRecipientIsVisibleToLaterReads/burnt/prague":                 {"0xc1c7a7d9d48dc3b9fe223399bff9ab16cf3f6be861086c2fd566f1bf89bb2c0e", 1440475},
	"TestFeeRevivingTouchedRecipientIsVisibleToLaterReads/coinbase/amsterdam":           {"0x5c5ea277e54881a3c162c7809cb6a290a1ccb031169db6addb45c4289a72bd21", 3035520},
	"TestFeeRevivingTouchedRecipientIsVisibleToLaterReads/coinbase/london":              {"0x900e3f8a89edc137f241db7977bb6ea17006acc31bf34da241b35ca4e09e450b", 1440475},
	"TestFeeRevivingTouchedRecipientIsVisibleToLaterReads/coinbase/prague":              {"0x5e40df368f0d786cc4324f0cf3dbecb7ab64e84ab46cba5967825822c58617cf", 1360475},
	"TestParallelBatchStorageAndHistoryMatchSerial":                                     {"0xad3f85347ed48c2e374570af706950dbaefec68ec1112246f9030836e1bbc4df", 87974},
	"TestRevertedCreate2KeepsCredits":                                                   {"0x617c60d82936ee9eb9a83daaae8bd4feb1ab50716417a3f14506bcd5476eba75", 281520},
	"TestRevertedSelfdestructKeepsCreateEndowment/amsterdam=false":                      {"0x1876dda970bbd77e904b2b7fdc5c674e17d38d65d56506fabfd743efbf0d837f", 162166},
	"TestRevertedSelfdestructKeepsCreateEndowment/amsterdam=true":                       {"0x266a5cde6739b5ccf4246279e28ac5fa0134471766ab2fd259c5b20da7524afb", 201960},
	"TestSelfdestructRefundAfterRecreate/berlin":                                        {"0x6c00adbbe82a30f808465bf07c80d70793ff29bededc4ab63aab3c50cfa6e5db", 246372},
	"TestSelfdestructRefundAfterRecreate/petersburg":                                    {"0x552c5fe09dfd21423cae06da59d18cb1c44c44e0cc13bc82381217ac952fc1d5", 233172},
	"TestSelfdestructRefundInTxAfterRecreate/berlin":                                    {"0xebd30387316ce450e736cdb8f8289a7995eb0f83f574ff9a4cf5775f547f025d", 166543},
	"TestSelfdestructRefundInTxAfterRecreate/petersburg":                                {"0xdc15d9db6c68f2f33382ae21d7e77e53415c5b8c638ee32f9e08e37f33e8c1fe", 162043},
	"TestSelfdestructRefundOncePerTx/berlin":                                            {"0x691e15c187ee18959efdd606d269e277075d5759e5fc6442fd8c0aa5381a1512", 512707},
	"TestSelfdestructRefundOncePerTx/petersburg":                                        {"0x059ac83c33d636102399a612ace142a90bacf1cb51e6ec22026aee7a1bc8342b", 1437807},
	"TestStorageReadAfterRecreate/osaka":                                                {"0x4566ac309f9379db840cddaad60371b2631881026bee2b0e39410138373c373b", 981981},
	"TestStorageReadAfterRecreate/shanghai":                                             {"0x110266e0cc2a44f0dc18346331985645aa11e6d811b16108b3157fb53b9a7fec", 1027547},
	"TestTouchedStorageOnlyAccountIsAbsentForLaterTx/amsterdam":                         {"0xac06f2a6d73d1dcecc65cefb9006695e47caee746664fe36c70a6b7be252f187", 183600},
	"TestTouchedStorageOnlyAccountIsAbsentForLaterTx/prague":                            {"0x3e3de2ba4f240e5ef22d52a7cc84d431c57a624ec77837f2358099aebd3a9b78", 42000},
	"TestZeroBaseFeeRemovesEmptyBurntRecipient":                                         {"0xe0fb98b28045966873e9bd251994c0da40e37f4f8b21843431378f2334eb8c07", 21000},
}

// requireGeneratedBlock checks the top block of chainPack against generatedBlockWant.
func requireGeneratedBlock(t *testing.T, chainPack *blockgen.ChainPack) {
	t.Helper()
	want, ok := generatedBlockWant[t.Name()]
	require.Truef(t, ok, "add %s to generatedBlockWant: {%q, %d}", t.Name(), chainPack.TopBlock.Root().Hex(), chainPack.TopBlock.GasUsed())
	require.Equal(t, want.root, chainPack.TopBlock.Root().Hex())
	require.Equal(t, want.gas, chainPack.TopBlock.GasUsed())
}

// buildBlockSerially builds one block, one tx after another. Each call is a tx
// from its own sender.
func buildBlockSerially(t *testing.T, gspec *types.Genesis, keys []*ecdsa.PrivateKey, coinbase common.Address, gasPrice uint64, calls []revivalCall) *blockgen.ChainPack {
	t.Helper()
	signer := types.LatestSignerForChainID(gspec.Config.ChainID)
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(keys[0]))
	chainPack, err := m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) {
		b.SetCoinbase(coinbase)
		for i, call := range calls {
			sender := crypto.PubkeyToAddress(keys[i].PublicKey)
			txn, err := types.SignTx(types.NewTransaction(b.TxNonce(sender), call.to, uint256.NewInt(call.value), 1_000_000, uint256.NewInt(gasPrice), call.data), *signer, keys[i])
			require.NoError(t, err)
			b.AddTx(txn)
		}
	})
	require.NoError(t, err)
	for _, receipt := range chainPack.Receipts[0] {
		require.Equal(t, uint64(types.ReceiptStatusSuccessful), receipt.Status)
	}
	requireGeneratedBlock(t, chainPack)
	return chainPack
}

type revivalCall struct {
	to    common.Address
	value uint64
	data  []byte
}

// importInParallel imports the block with parallel execution at several worker
// counts and checks the resulting state.
func importInParallel(t *testing.T, gspec *types.Genesis, key *ecdsa.PrivateKey, chainPack *blockgen.ChainPack, check func(t *testing.T, st *state.IntraBlockState)) {
	t.Helper()
	prevWorkers := ethconfig.Defaults.Sync.ExecWorkerCount
	t.Cleanup(func() { ethconfig.Defaults.Sync.ExecWorkerCount = prevWorkers })
	for _, workers := range []int{1, 2, 4, 8} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			ethconfig.Defaults.Sync.ExecWorkerCount = workers
			m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(key))
			require.NoError(t, m.InsertChain(chainPack))
			require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
				st := state.New(m.NewStateReader(tx))
				defer st.Close()
				check(t, st)
				return nil
			}))
		})
	}
}

func revivalKeys(t *testing.T, alloc types.GenesisAlloc, n int) []*ecdsa.PrivateKey {
	t.Helper()
	keys := make([]*ecdsa.PrivateKey, n)
	for i := range keys {
		key, err := crypto.ToECDSA(common.LeftPadBytes([]byte{byte(i + 1)}, 32))
		require.NoError(t, err)
		keys[i] = key
		alloc[crypto.PubkeyToAddress(key.PublicKey)] = types.GenesisAccount{Balance: big.NewInt(1_000_000_000_000_000_000)}
	}
	return keys
}

// The coinbase is created by CREATE2 and destroyed in the same tx, a later
// tx's fee revives it with a balance only, and a third tx re-creates it by the
// same CREATE2, this time with code. Parallel import must accept the block, and the
// coinbase must end with the code.
func TestCoinbaseRecreatedWithCodeAfterFeeRevival(t *testing.T) {
	// The constructor self-destructs while the account has no balance and
	// deploys 32 bytes of code once a fee has revived it.
	initCode := program.New().Op(vm.SELFBALANCE).Push(8).Op(vm.JUMPI).Selfdestruct(0xdead)
	_, deploy := initCode.Jumpdest()
	require.Equal(t, uint64(8), deploy)
	runtime := make([]byte, 32)
	runtime[31] = 1
	initCode.ReturnData(runtime)

	factory := common.HexToAddress("0xfac7")
	coinbase := types.CreateAddress2(factory, [32]byte{}, accounts.InternCodeHash(crypto.Keccak256Hash(initCode.Bytes())))
	for _, amsterdam := range []bool{false, true} {
		t.Run(fmt.Sprintf("amsterdam=%v", amsterdam), func(t *testing.T) {
			config := chain.TestChainOsakaConfig.Copy()
			if amsterdam {
				config.AmsterdamTime = common.NewUint64(0)
			}
			alloc := types.GenesisAlloc{factory: {Code: program.New().Create2(initCode.Bytes(), 0).Op(vm.STOP).Bytes()}}
			keys := revivalKeys(t, alloc, 3)
			gspec := &types.Genesis{Config: config, GasLimit: 30_000_000, Alloc: alloc}
			chainPack := buildBlockSerially(t, gspec, keys, coinbase, 10_000_000_000, []revivalCall{
				{to: factory},
				{to: common.HexToAddress("0xbeef")},
				{to: factory},
			})
			importInParallel(t, gspec, keys[0], chainPack, func(t *testing.T, st *state.IntraBlockState) {
				code, err := st.GetCode(accounts.InternAddress(coinbase))
				require.NoError(t, err)
				require.Equal(t, runtime, code)
			})
		})
	}
}

// Before Cancun, one block: a CREATE2 deploys X, the next tx makes X destroy
// itself, a 1-wei transfer revives X as a plain account, and the same CREATE2
// deploys X again. The revived account has no nonce and no code, so the second
// CREATE2 succeeds. The factory records each deployed address in the slot given
// by the calldata.
func TestCreate2AfterDestructAndTransferRevival(t *testing.T) {
	runtime := program.New().Op(vm.CALLER, vm.SELFDESTRUCT).Bytes()
	initCode := program.New().ReturnData(runtime).Bytes()
	factory := common.HexToAddress("0xfac7")
	x := types.CreateAddress2(factory, [32]byte{}, accounts.InternCodeHash(crypto.Keccak256Hash(initCode)))

	alloc := types.GenesisAlloc{factory: {Code: program.New().Create2(initCode, 0).Push(0).Op(vm.CALLDATALOAD, vm.SSTORE, vm.STOP).Bytes()}}
	keys := revivalKeys(t, alloc, 4)
	gspec := &types.Genesis{Config: chain.TestChainBerlinConfig, GasLimit: 30_000_000, Alloc: alloc}
	chainPack := buildBlockSerially(t, gspec, keys, common.Address{1}, 1, []revivalCall{
		{to: factory, data: common.LeftPadBytes([]byte{0}, 32)},
		{to: x},
		{to: x, value: 1},
		{to: factory, data: common.LeftPadBytes([]byte{1}, 32)},
	})
	importInParallel(t, gspec, keys[0], chainPack, func(t *testing.T, st *state.IntraBlockState) {
		for slot := range byte(2) {
			deployed, err := st.GetState(accounts.InternAddress(factory), accounts.InternKey(common.BytesToHash([]byte{slot})))
			require.NoError(t, err)
			require.Equal(t, *new(uint256.Int).SetBytes(x[:]), deployed, "CREATE2 %d", slot)
		}
		code, err := st.GetCode(accounts.InternAddress(x))
		require.NoError(t, err)
		require.Equal(t, runtime, code)
	})
}

// Before Cancun, in one block: a slow tx destroys a pre-existing contract that is
// the coinbase, the next tx's fee revives it as a plain account, and a third tx
// re-creates it by CREATE2. Parallel import must accept the block.
func TestCreate2AfterCoinbaseDestructAndFeeRevival(t *testing.T) {
	initCode := []byte{byte(vm.STOP)}
	factory := common.HexToAddress("0xfac7")
	coinbase := types.CreateAddress2(factory, [32]byte{}, accounts.NewCode(initCode).Hash)
	alloc := types.GenesisAlloc{}
	keys := revivalKeys(t, alloc, 3)
	beneficiary := crypto.PubkeyToAddress(keys[2].PublicKey)
	alloc[factory] = types.GenesisAccount{Nonce: 1, Code: countdownLoop(program.New().Push(1_000)).Create2(initCode, 0).Op(vm.POP, vm.STOP).Bytes()}
	alloc[coinbase] = types.GenesisAccount{Nonce: 1, Code: countdownLoop(program.New().Push(30_000)).Selfdestruct(beneficiary).Bytes()}

	gspec := &types.Genesis{Config: shanghaiConfig(), GasLimit: 60_000_000, Alloc: alloc}
	chainPack := buildBlockSerially(t, gspec, keys, coinbase, 100_000_000_000, []revivalCall{
		{to: coinbase},
		{to: common.HexToAddress("0xbeef")},
		{to: factory},
	})
	importInParallel(t, gspec, keys[0], chainPack, func(t *testing.T, st *state.IntraBlockState) {
		nonce, err := st.GetNonce(accounts.InternAddress(coinbase))
		require.NoError(t, err)
		require.Equal(t, uint64(1), nonce, "the CREATE2 re-creates the revived coinbase")
	})
}
