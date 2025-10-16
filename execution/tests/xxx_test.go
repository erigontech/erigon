package executiontests

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/testlog"
	"github.com/erigontech/erigon/cmd/rpcdaemon/cli"
	"github.com/erigontech/erigon/cmd/rpcdaemon/cli/httpcfg"
	state2 "github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/execution/engineapi"
	enginetypes "github.com/erigontech/erigon/execution/engineapi/engine_types"
	executionp2p "github.com/erigontech/erigon/execution/p2p"
	"github.com/erigontech/erigon/node"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/nodecfg"
	"github.com/erigontech/erigon/p2p"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/sentry"
	"github.com/erigontech/erigon/rpc/jsonrpc"
)

func TestXXX(t *testing.T) {
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)
	//dirs := datadir.New("/Users/taratorio/prysm-erigon-2.fusaka-devnet-3-recent-pure-wrong-trie-root-at-canonical-tip-v2/data/erigon")
	//dirs := datadir.New("/Users/taratorio/erigon-data/lh-erigon-fusaka-devnet-3-experimental/erigon-datadir")
	dirs := datadir.New("/Users/taratorio/erigon-data/lh-erigon-fusaka-devnet-3-experimental/erigon-datadir-retrace2")
	//blockFreezingConfig := ethconfig.BlocksFreezing{
	//	ProduceE2:    true,
	//	ProduceE3:    true,
	//	NoDownloader: true,
	//}
	//sn := freezeblocks.NewRoSnapshots(blockFreezingConfig, dirs.Snap, logger)
	//br := freezeblocks.NewBlockReader(sn, nil)
	nodeConfig := nodecfg.Config{
		Dirs:         dirs,
		MdbxPageSize: ethconfig.DefaultChainDBPageSize,
		MdbxWriteMap: true,
	}
	mdbxDb, err := node.OpenDatabase(ctx, &nodeConfig, dbcfg.ChainDB, "", false, logger)
	require.NoError(t, err)
	agg, err := state.New(dirs).Logger(logger).SanityOldNaming().GenSaltIfNeed(false).Open(ctx, mdbxDb)
	require.NoError(t, err)
	db, err := temporal.New(mdbxDb, agg)
	require.NoError(t, err)
	//blockNum := uint64(272_996)
	//blockHash := common.HexToHash("0x7336c13e5f6da772387d2dff833fe194e6f39c96d53c7403cc389fd72bde7fe5")
	tx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	//body, _, err := br.Body(ctx, roTx, blockHash, blockNum)
	//require.NoError(t, err)
	//require.NotNil(t, body)
	//println("--- debug --- bodyTxns=", len(body.Transactions))
	bodyCursor, err := tx.Cursor(kv.BlockBody)
	require.NoError(t, err)
	defer bodyCursor.Close()

	k, _, err := bodyCursor.Last()
	require.NoError(t, err)
	require.NotNil(t, k)
	println("--- debug --- blockNum=", binary.BigEndian.Uint64(k[:8]))
	println("--- debug --- blockHash=", common.Hash(k[8:40]).String())

	//jsonrpc.NewBaseApi()
	//jsonrpc.DebugAPIImpl{}
}

func TestYYYHeader(t *testing.T) {
	//h := &types.Header{
	//	Number:     big.NewInt(272_996),
	//	ParentHash: common.HexToHash("0x1908acc9ea3bfebfc90beb4e571c141d2a69cde5e36d9674c010524c8de9c3b7"),
	//}
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlTrace)
	//bootrapNodes := []*enode.Node{
	//	enode.MustParse("enode://e0ff6526404345e053080bbd1a7ba4c4011ac47c67a31e9ac9cf9f1d39b6e494544b851e265ac17718ab195842f5b99d616071178fbf08a472e63577827e1337@127.0.0.1:30303"),
	//}
	protocolVersion := uint(68)
	sentryTmpDir := t.TempDir()
	nodeKeyConfig := p2p.NodeKeyConfig{}
	nodeKey, err := nodeKeyConfig.LoadOrGenerateAndSave(nodeKeyConfig.DefaultPath(sentryTmpDir))
	require.NoError(t, err)
	p2pConfig := &p2p.Config{
		PrivateKey:      nodeKey,
		MaxPeers:        32,
		MaxPendingPeers: 32,
		Name:            "xyz",
		BootstrapNodes:  enodes,
		StaticNodes:     enodes,
		ListenAddr:      "localhost:6969",
		AllowedPorts:    []uint{6969},
		ProtocolVersion: []uint{protocolVersion},
		TmpDir:          "",
		NodeDatabase:    sentryTmpDir,
	}
	//dirs := datadir.New("/Users/taratorio/prysm-erigon-2.fusaka-devnet-3-recent-pure-wrong-trie-root-at-canonical-tip-v2/data/erigon")
	dirs := datadir.New("/Users/taratorio/erigon-data/lh-erigon-fusaka-devnet-3-experimental/erigon-datadir-retrace2")
	//blockFreezingConfig := ethconfig.BlocksFreezing{
	//	ProduceE2:    true,
	//	ProduceE3:    true,
	//	NoDownloader: true,
	//}
	//sn := freezeblocks.NewRoSnapshots(blockFreezingConfig, dirs.Snap, logger)
	//br := freezeblocks.NewBlockReader(sn, nil)
	nodeConfig := nodecfg.Config{
		Dirs:         dirs,
		MdbxPageSize: ethconfig.DefaultChainDBPageSize,
		MdbxWriteMap: true,
	}
	mdbxDb, err := node.OpenDatabase(ctx, &nodeConfig, dbcfg.ChainDB, "", false, logger)
	require.NoError(t, err)
	tx, err := mdbxDb.BeginRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	genesisHash, err := rawdb.ReadCanonicalHash(tx, 0)
	require.NoError(t, err)
	require.Equal(t, common.HexToHash("0x3a8c8cef63859865aa1d40ded77b083eef06a1702b8188d5586434b9c3adc4be"), genesisHash)
	genesisBlock := rawdb.ReadBlock(tx, genesisHash, 0)
	require.NotNil(t, genesisBlock)
	chainConfig, err := rawdb.ReadChainConfig(tx, genesisHash)
	require.NoError(t, err)
	require.Equal(t, uint64(7023102237), chainConfig.ChainID.Uint64())
	sentryServer := sentry.NewGrpcServer(ctx, nil, nil, p2pConfig, protocolVersion, logger)
	sentryClient := direct.NewSentryClientDirect(protocolVersion, sentryServer)
	peerPenalizer := executionp2p.NewPeerPenalizer(sentryClient)
	statusDataProvider := sentry.NewStatusDataProvider(
		mdbxDb,
		chainConfig,
		genesisBlock,
		chainConfig.ChainID.Uint64(),
		logger,
	)
	messageListener := executionp2p.NewMessageListener(logger, sentryClient, statusDataProvider.GetStatusData, peerPenalizer)
	messageSender := executionp2p.NewMessageSender(sentryClient)
	peerTracker := executionp2p.NewPeerTracker(logger, messageListener)
	fetcher := executionp2p.NewFetcher(logger, messageListener, messageSender)
	var eg errgroup.Group
	t.Cleanup(func() {
		err := eg.Wait()
		require.ErrorIs(t, err, context.Canceled)
	})
	ctx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	eg.Go(func() error {
		return messageListener.Run(ctx)
	})
	eg.Go(func() error {
		return peerTracker.Run(ctx)
	})
	time.Sleep(10 * time.Second)
	blockNum := uint64(272_996)
	blockHash := common.HexToHash("0x7336c13e5f6da772387d2dff833fe194e6f39c96d53c7403cc389fd72bde7fe5")
	header := rawdb.ReadHeader(tx, blockHash, blockNum)
	terminate := header != nil
	if terminate {
		logger.Info("header already exists", "num", header.Number.Uint64(), "hash", header.Hash(), "parent", header.ParentHash)
		return
	}
	tx.Rollback()
	rwTx, err := mdbxDb.BeginRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()
	for !terminate {
		for _, p := range peerTracker.ListPeers() {
			response, err := fetcher.FetchHeaders(ctx, blockNum, blockNum+1, p)
			if err != nil {
				logger.Error("failed to fetch headers", "err", err, "peer", p)
				continue
			}
			header := response.Data[0]
			if hash := header.Hash(); hash != blockHash {
				logger.Error("fetched header hash does not match", "expected", blockHash, "actual", hash, "peer", p)
				continue
			}
			logger.Info("success! found header", "h", fmt.Sprintf("%v", header))
			parentTd, err := rawdb.ReadTd(rwTx, header.ParentHash, header.Number.Uint64()-1)
			require.NoError(t, err)
			require.NotNil(t, parentTd)
			td := parentTd.Add(parentTd, header.Difficulty)
			err = rawdb.WriteHeader(rwTx, header)
			require.NoError(t, err)
			err = rawdb.WriteTd(rwTx, header.Hash(), header.Number.Uint64(), td)
			require.NoError(t, err)
			err = rwTx.Commit()
			require.NoError(t, err)
			terminate = true
			break
		}
	}
}

func TestZZZFcu(t *testing.T) {
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)
	//jwtConfig := &httpcfg.HttpCfg{JWTSecretPath: "/Users/taratorio/prysm-erigon-2.fusaka-devnet-3-recent-pure-wrong-trie-root-at-canonical-tip-v2/data/jwt.hex"}
	jwtConfig := &httpcfg.HttpCfg{JWTSecretPath: "/Users/taratorio/erigon-data/lh-erigon-fusaka-devnet-3-experimental/jwt.hex"}
	jwtSecret, err := cli.ObtainJWTSecret(jwtConfig, logger)
	require.NoError(t, err)
	engineApiClient, err := engineapi.DialJsonRpcClient("http://localhost:8551", jwtSecret, logger)
	require.NoError(t, err)
	fcu := &enginetypes.ForkChoiceState{
		HeadHash:           common.HexToHash("0x7336c13e5f6da772387d2dff833fe194e6f39c96d53c7403cc389fd72bde7fe5"),
		SafeBlockHash:      common.HexToHash("0x1908acc9ea3bfebfc90beb4e571c141d2a69cde5e36d9674c010524c8de9c3b7"),
		FinalizedBlockHash: common.HexToHash("0x1908acc9ea3bfebfc90beb4e571c141d2a69cde5e36d9674c010524c8de9c3b7"),
	}
	engineApiClient.ForkchoiceUpdatedV3(ctx, fcu, nil)
}

func TestVVVParentFcu(t *testing.T) {
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)
	//jwtConfig := &httpcfg.HttpCfg{JWTSecretPath: "/Users/taratorio/prysm-erigon-2.fusaka-devnet-3-recent-pure-wrong-trie-root-at-canonical-tip-v2/data/jwt.hex"}
	jwtConfig := &httpcfg.HttpCfg{JWTSecretPath: "/Users/taratorio/erigon-data/lh-erigon-fusaka-devnet-3-experimental/jwt.hex"}
	jwtSecret, err := cli.ObtainJWTSecret(jwtConfig, logger)
	require.NoError(t, err)
	engineApiClient, err := engineapi.DialJsonRpcClient("http://localhost:8551", jwtSecret, logger)
	require.NoError(t, err)
	fcu := &enginetypes.ForkChoiceState{
		HeadHash:           common.HexToHash("0x1908acc9ea3bfebfc90beb4e571c141d2a69cde5e36d9674c010524c8de9c3b7"),
		SafeBlockHash:      common.HexToHash("0xfbd35fdb3382f849efea535a08bbdc771a01543964ec78bb8a59be9e0143af68"),
		FinalizedBlockHash: common.HexToHash("0xfbd35fdb3382f849efea535a08bbdc771a01543964ec78bb8a59be9e0143af68"),
	}
	engineApiClient.ForkchoiceUpdatedV3(ctx, fcu, nil)
}

func TestStateDumper(t *testing.T) {
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)
	//dirs := datadir.New("/Users/taratorio/prysm-erigon-2.fusaka-devnet-3-recent-pure-wrong-trie-root-at-canonical-tip-v2/data/erigon")
	dirs := datadir.New("/Users/taratorio/erigon-data/lh-erigon-fusaka-devnet-3-experimental/erigon-datadir")
	nodeConfig := nodecfg.Config{
		Dirs:         dirs,
		MdbxPageSize: ethconfig.DefaultChainDBPageSize,
		MdbxWriteMap: true,
	}
	blockFreezingConfig := ethconfig.BlocksFreezing{
		ProduceE2:    true,
		ProduceE3:    true,
		NoDownloader: true,
	}
	sn := freezeblocks.NewRoSnapshots(blockFreezingConfig, dirs.Snap, logger)
	br := freezeblocks.NewBlockReader(sn, nil)
	mdbxDb, err := node.OpenDatabase(ctx, &nodeConfig, dbcfg.ChainDB, "", false, logger)
	require.NoError(t, err)
	agg, err := state.New(dirs).Logger(logger).SanityOldNaming().GenSaltIfNeed(false).Open(ctx, mdbxDb)
	require.NoError(t, err)
	db, err := temporal.New(mdbxDb, agg)
	require.NoError(t, err)
	tx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	dumper := state2.NewDumper(tx, br.TxnumReader(ctx), 272996, true /* latest */)
	dump := dumper.Dump(true /* excludeCode */, false /* excludeStorage */)
	dumpFileName := "/Users/taratorio/prysm-erigon-2.fusaka-devnet-3-recent-pure-wrong-trie-root-at-canonical-tip-v2/erigon/correct-state-dump.json"
	err = os.WriteFile(dumpFileName, dump, 0644)
	require.NoError(t, err)
}

func TestTxnNums(t *testing.T) {
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlDebug)
	dirs := datadir.New("/Users/taratorio/prysm-erigon-2.fusaka-devnet-3-recent-pure-wrong-trie-root-at-canonical-tip-v2/data/erigon")
	//dirs := datadir.New("/Users/taratorio/erigon-data/lh-erigon-fusaka-devnet-3-experimental/erigon-datadir")
	nodeConfig := nodecfg.Config{
		Dirs:         dirs,
		MdbxPageSize: ethconfig.DefaultChainDBPageSize,
		MdbxWriteMap: true,
	}
	blockFreezingConfig := ethconfig.BlocksFreezing{
		ProduceE2:    true,
		ProduceE3:    true,
		NoDownloader: true,
	}
	sn := freezeblocks.NewRoSnapshots(blockFreezingConfig, dirs.Snap, logger)
	br := freezeblocks.NewBlockReader(sn, nil)
	mdbxDb, err := node.OpenDatabase(ctx, &nodeConfig, dbcfg.ChainDB, "", false, logger)
	require.NoError(t, err)
	agg, err := state.New(dirs).Logger(logger).SanityOldNaming().GenSaltIfNeed(false).Open(ctx, mdbxDb)
	require.NoError(t, err)
	db, err := temporal.New(mdbxDb, agg)
	require.NoError(t, err)
	tx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	txnNumReader := br.TxnumReader(ctx)
	blockNum, txnNum, err := txnNumReader.Last(tx)
	for blockNum >= 272900 {
		require.NoError(t, err)
		fmt.Printf("blockNum: %d, txnNum: %d, step: %d\n", blockNum, txnNum, txnNum/agg.StepSize())
		blockNum--
		txnNum, err = txnNumReader.Max(tx, blockNum)
	}
	require.NoError(t, err)
}

func TestDiffStorageRange(t *testing.T) {
	testDiffStorageRange(t, 272995)
}

func testDiffStorageRange(t *testing.T, blockNum uint64) {
	type payload struct {
		Result *jsonrpc.StorageRangeResult `json:"result"`
	}
	correct272995Bytes, err := os.ReadFile(fmt.Sprintf("/Users/taratorio/prysm-erigon-2.fusaka-devnet-3-recent-pure-wrong-trie-root-at-canonical-tip-v2/erigon/storage_range_%d_correct.json", blockNum))
	require.NoError(t, err)
	wrong272995Bytes, err := os.ReadFile(fmt.Sprintf("/Users/taratorio/prysm-erigon-2.fusaka-devnet-3-recent-pure-wrong-trie-root-at-canonical-tip-v2/erigon/storage_range_%d_wrong.json", blockNum))
	require.NoError(t, err)
	var correct272995 payload
	err = json.Unmarshal(correct272995Bytes, &correct272995)
	require.NoError(t, err)
	var wrong272995 payload
	err = json.Unmarshal(wrong272995Bytes, &wrong272995)
	require.NoError(t, err)
	var keysCorrect []common.Hash
	keyToKeyMapCorrect := map[common.Hash]common.Hash{}
	for k, storageEntry := range correct272995.Result.Storage {
		keyToKeyMapCorrect[*storageEntry.Key] = k
		keysCorrect = append(keysCorrect, *storageEntry.Key)
	}
	var keysWrong []common.Hash
	keyToKeyMapWrong := map[common.Hash]common.Hash{}
	for k, storageEntry := range wrong272995.Result.Storage {
		keyToKeyMapWrong[*storageEntry.Key] = k
		keysWrong = append(keysWrong, *storageEntry.Key)
	}
	sort.SliceStable(keysCorrect, func(i, j int) bool {
		return bytes.Compare(keysCorrect[i][:], keysCorrect[j][:]) < 0
	})
	sort.SliceStable(keysWrong, func(i, j int) bool {
		return bytes.Compare(keysWrong[i][:], keysWrong[j][:]) < 0
	})
	require.Equal(t, keysCorrect, keysWrong)
	for _, k := range keysCorrect {
		correctV := correct272995.Result.Storage[keyToKeyMapCorrect[k]].Value
		wrongV := wrong272995.Result.Storage[keyToKeyMapWrong[k]].Value
		if correctV != wrongV {
			fmt.Printf("key: %s, correct: %s, wrong: %s\n", k, correctV, wrongV)
		}
	}
}

var enodeUrls = []string{
	"enode://f9df4eafbaa13bfb6937d3291241cea8155b0604e261b3b0f55405631690ad0136d2b30f8583fbe32152a40953365bd91d2f756a17629ad570e2d2031aa5b248@178.62.239.100:30303?discport=30303",
	"enode://9e8d9fe5dd6742a61253694707ac608b0cc7f69044d35e90679bf29ce6d54006c88a4a9f8c88d77e244cc817e046ec9629d759a0ae7dd551c1091589a99dd35d@134.199.163.129:30303?discport=30303",
	"enode://13bb1219ad25b6fbb325333d99af6796e98cc31e22ef456145a4ad935234bd416a9db5a19544cdacc4141354c0c08ca4abb0de409f982f2f025ea3a83df5d252@170.64.150.17:30303?discport=30303",
	"enode://147625a3efa1365508b13a30cf0279606af4aa4093a4bb838e80250d30e023c8e6effc00d4923b6b8b8187b9067b0874a6e4a82a09bd1f5d65a399eee5b05ea1@137.184.77.88:30303?discport=30303",
	"enode://e8ea0bc655df3e198d88455c73367ae9a24fc546b09622b657ab455fd9c203562409187f7330186b5f656ac15d83307309a964c694b8ea5fc2e33eecfa731305@128.199.243.149:30303?discport=30303",
	"enode://fdc913675f056d54738b0a2fce520983aebfbb3ddded3ac3b0d048d73bc8739c072ea1448541d96382c37ad17d808641381f81869a7994ef29b783d6ca9f5e05@46.101.214.222:30303?discport=30303",
	"enode://9b430c6f2a1d622c910b54e7d285397e5cfef4d067efe480589c7c239ea2a5a67be4423bc31f007a8481b33d41197eaba61e7eab3ff9ae815c8c5d5296b8357f@209.38.198.0:30303?discport=30303",
	"enode://1d4ce29698b3dd71b09f43bb1f6cf32c8740aecfdd67049b9694a44b62c0288f6b1e7964d11ea2a2243e28545ac6a85e974da3a2fd9b686d67404427f0ce03e5@137.184.107.165:30303?discport=30303",
	"enode://c08e1eafaa195ceb156c916f2917e506524c42c5514d98cb66f8fa219d8b911f3e63c78d9739a631078eb4f98e63716060cb14d1f8d616aed2a74226dfe595c0@24.199.84.132:30303?discport=30303",
	"enode://ee72dad58b61aae357de2f2d42783266e17249e3db95df3773291c2756488cc62d95838f3ac33acabe513567f12e93e55f59fccc8a0a75cf7dea981d0f283f8a@167.99.224.233:30303?discport=30303",
	"enode://b8268f5146d49ce66bfe61e6110d0706c580a402c2557980e153f80d2ebe6605719522761c824c185b73caa82a3483f0f619c57b598d37298c01e54cb1e8822d@134.209.148.242:30303?discport=30303",
	"enode://7cad311c32ee1f6b6881bbf65900743d9b5dea2a9c068a5be1d2d77bc34e160c5775968e7ef03628ef8ea4c7f0689f825710c46538640bf65900a31e4034543b@139.59.76.222:30303?discport=30303",
	"enode://b82c493e8f4bc65847eec6e28333c2ad70eb0aa826cb98cd94052668ace415285439f36308627dcdaa36e1f228cfce06cf35be671ede0e9f90bd8561c27c2f51@142.93.131.83:30303?discport=30303",
	"enode://bc6ffea38cc6e66b09fb454970caf90b8fa67074c9e5d80fda137e8a4073a32e73545fadaa263724397c52124db7a261d956f4b24ab787a8a94d878af8bea6df@146.190.24.90:30303?discport=30303",
	"enode://4b0b1663c78301665b02509af543e99b2273de84c5fe0ebeef5f02d7e42921b0605a76bcf816c1377d962bf6ab7a7ac3c162f38289deca4e17a2d2bf869d5377@68.183.119.118:30303?discport=30303",
	"enode://362c06afebff994cb00eaf32298c986d84f026855171b1d3b6db0534c46d1b475dcfd84608db271526046d11f9220793b2b7667e106094c305cb8e18b33e440b@152.42.195.58:30303?discport=30303",
	"enode://a269c94f91d6379d4238ab00c94a223ec4ad8a7207b9ec39e6a5813d9cf08a92691e5a9f72ac3905381357a77f94b082a1e2b708c42add3d7d5edf7cd4265e88@142.93.244.202:30303?discport=30303",
	"enode://fb6a8d7d5bb02239e6134880882ce218ccb37d20f3e94abf6c0937961cc72892d56391767308441aedadeb98a57d162a6f570f263c6ee8bf2a935a97b12f5860@134.122.22.50:30303?discport=30303",
	"enode://158e494fe916d642d5a0518c1bea15e47ddc64c5a583b32cab514819e6d1e99b8d4e8f809cce69e69d4f010b344291cefadce8aafa0cf863d455c48848ba9b9c@128.199.242.20:30303?discport=30303",
	"enode://78777f79803a414682719c19d07665075a629d99758da19adb606c9b024fb6d377471cd0d9f7e13c205277a11d14c61e108f594aa186674ae5125076755c18a7@139.59.107.234:30303?discport=30303",
	"enode://89d59c8eafda18560b9198cbe72a0d7bdcc9c486b2cc5a5d4a5d7012bf6eb23c320d938471a812cdde3366c69c01647e3831c3044acfcce8c35df0c26463abc1@147.182.211.241:30303?discport=30303",
	"enode://3eb4961a246b35389ddaccb36b0d83ac03e119f68c73b116e49d8d35f7e106e43d8454e8abb1d3f5a967e4f0481409c1a575c38af9b755f2f1b814d75d56ae37@138.68.130.19:30303?discport=30303",
	"enode://f24bb8188d40f2dbc6e84e77399f173567f30c423874c2a4d0a1a9fc639157586c631f6d5fc45c82dd246b075bd9e3f5b1d83d64058f37a33a96fa633c4d6b3f@159.65.60.119:30303?discport=30303",
	"enode://59f7995f0042d192ee719249f1c381e02eb1a22c319530f58f33d8056af08ee841d1c6d8f72821d5998d0857f4bfc6874bb0dd131249bc2b7e2e4d8af6685f40@143.244.141.15:30303?discport=30303",
	"enode://e67312b16f58ad899af96a2cbbe0bd6ba889c800624b61772800469a8bbfb18f27e95675b501c411ddd7be5a4b9424de442dc25bd912aa7238882098fd21bf42@64.227.131.250:30303?discport=30303",
	"enode://16136ece73e85f1c0a475ef3cdb96a269e254d1d8eed269987af6ff0bf914e66f711ca61d137b1c4f59efc4afe836fc4edd45b54b69c35ea7846d548e7570ad5@104.248.122.2:30303?discport=30303",
	"enode://d80d81f15f7e64c44e5aba507fbf218cf8f3b62d2470b899337387634e3382f411b5381f60c988569f3a0b7ff4c7b3210ddd0077d520fb8c0af8ab01b232a151@128.199.121.72:30303?discport=30303",
	"enode://e6e277992697cea4746f41870bfe177486aa516932fe3676c4ccb5e77255c703f0ac48c1bc55470b7aad5f8145ee5540e290021a5cb8f85933bb454682e60ead@137.184.169.82:30303?discport=30303",
	"enode://2348e051756eab33d6a412fbcb698a731f4367aa985ba6af912c5ee381baa9310fd72ad1ab10a88d443b77c2a7b19fe3a69fdacef299862b1da352f5b9f4ecd9@68.183.200.53:30303?discport=30303",
	"enode://0cf19e76d17b477dac8a44bd5b52f821dc146bc1b7f2bc4a6b7d8d93aeea599eb78653eeb57c02910d7009fe1d82b111776f22c11f74ad41fbe88b21b2de78cd@64.227.166.21:30303?discport=30303",
	"enode://b4c2438a9b18c10e17423a2fc79c3e202f1730cf3d9b34570a9d85649966c178630ad420e6539ca784ba3efd187ed322c3b3e4619b5ac255c73e80ec25616370@206.189.132.33:30303?discport=30303",
	"enode://86665040a6834921a092bbf2854d0c22129ec943d7b0010fd13063f5be2ad40b4a7133381ecb610d713322319b0ca72dcd83d341e890049a9f261496433f5239@159.89.51.192:30303?discport=30303",
	"enode://f3083e0f8261e516b1d633397a49f92233a4b718bfd9a3a00e1bff893f69325fef9dc63b5b3f552cdb8c3597db227d772d562d10a94fc53b98007c5493f0a917@64.227.150.112:30303?discport=30303",
	"enode://328de6f3b1674eb9228421d8b2ed3ffac553975cf63247d1f041a29bd32f5dd8c9ed4e27b25fd7ff2526b00c4557670583bd82d08bcc7768f73bb003eb8a5fb6@157.245.107.106:30303?discport=30303",
	"enode://18583be6eb6cd391dd5c117d76e7fc6f45c972ac531333d1e4f0255d1a2653510646ea7d6e8c56acf80d580769f85299539758aa66417c98dfd2642ef7f44c4b@206.81.6.4:30303?discport=30303",
	"enode://511c8f41877d6ac7bdde3589f2680bfb6c5347572a93fbf7f15906852f45747c55944ec21e2b79c5b0548b8416078634e3e4b2bc136a695707c842268aaf2b09@134.199.164.239:30303?discport=30303",
	"enode://831e5d6d8c5a2bded2fc56b3dd67352698236605f0bb3010f3ca62258f01991734072a98d4a67538b977b99d83372a8c9532f202f0b7c4db20c9ff517500c8ff@209.38.90.97:30303?discport=30303",
	"enode://d28729ec50aa61256c21e941c94be987f6a083ce5048475d05f41564436e92a84f547b178fcccc4dd6550e4f7a3afa5983d88fe3fb06ec3524f4cfbf22f0dedc@159.223.115.94:30303?discport=30303",
	"enode://a474479170e7f6b55a33b0a57ed1ee34b75ea567b0dccdd1e6e039be5c0c9009c590dc58ae94f6c44135f7e804be46c18d8037d74ba589f9be309a0563cfcc22@159.223.80.76:30303?discport=30303",
	"enode://11daac3193938c3f75e263e48a40d64c5fc80521fc8b0431ef7391ce2e699f222d3e99ce061dfc3b19897a96ed6ad85d2ea5a5e8af2c5f63f23bdc9ba5d9d892@137.184.188.24:30303?discport=30303",
	"enode://ca95dc422ce542a5ecec0bb38b1a5d9e3b028b10859557f96b7a0486ca37abc4de4da4c013a50e26c919eec1edc9144aaab8f728cc006ab53fb178d46bf09581@137.184.180.24:30303?discport=30303",
	"enode://2020dd4d62300968f3ba1d6d96e28fa3d85136c38ec47e497f94f6527445c51b06963d3a5b84c68df1474a7babb4d4814fbeec3a99a2713e24124f351f0afffb@134.199.145.230:30303?discport=30303",
	"enode://49089812fcc7298f36331b47235a87622e06eef0bf9618e93f4665a68ad295dd5d2fa37a3a484917a8826e3631fe0f24d25e93b3b18f2510dca8da3270aaeb1a@134.199.152.249:30303?discport=30303",
	"enode://628f42913f56280caace804f1053a1ca8bf6d2ac9a135e2bbbcdbac7b3e0080e3dd55e38cb8c2719653ee8c369605d56297abbc3266e0dcc4d9c5bfd1d429aa6@137.184.211.135:30303?discport=30303",
	"enode://f5945e421d66100d171547cd4209cb04594b987a780beb9d759ebb7caacc382209818491a84d97c4ae5ca86bdb16ab46b08cddabe12546b33a5ed2d4c29a1e14@167.71.230.19:30303?discport=30303",
	"enode://4e871c11be70459b09db0a40906fe0dd75171465b50de2ec060582d3e3c216182a3ebf3575398f9f5313f2c6ca6158b0a6d9144c2f04832db06d9d57d9aae717@143.110.250.33:30303?discport=30303",
	"enode://6120974003a5e13569c17a92e61ad62265001085432bc1b507d110d7920deb42b0f80eda0c107da97188653219ce53607aeab66b6f4055af3b42d984b5cdef00@206.189.10.188:30303?discport=30303",
	"enode://51d7936568c9d8a882b9cea12fe12d976a79c255f38ffc5c9a6ebf867de102ff63897de4bb8d0ae258c50a28c0edfe089522d3c75de9330b40678a400a1926bb@209.38.109.31:30303?discport=30303",
	"enode://df8b9c988b4cb47e4b041cf12989fc40995115d2dd5c7443aeb763e52c0d49696eeafab2cd689793c23216fbd522bdcdafc109cfd3555bb6374054732812af06@159.89.54.130:30303?discport=30303",
	"enode://d7ed62a111df378462b12e000fb2bc38d09f603e6d3c448a944dd5ebcac955fe67bd6ff59650942a0683189cc2f9f401b38d0c8e6700da2126332f52ea6039f9@188.166.231.238:30303?discport=30303",
	"enode://6c0ea79067ba9e1cd537a8830a5dc53b60017fe6e10a838591ac38654d448e846f5eb20ee9603039423cd8e31570596aefa72674e07d0a40c1da77620d00928e@174.138.80.76:30303?discport=30303",
	"enode://9550c67952b18e243b0d94376117d22c87f247062f00ed64dbb2713c3940fe812c2ccec33473dc63c33b915366294b530458ff8d70dfa537d90c8fc2733fe764@165.227.110.16:30303?discport=30303",
	"enode://6231b5a99275d7eec580f900602b433e75c6d5757af2633a1efeb12fccc5a6ee99b16e071154bc70e6bc82de10403d4c085bd11abc345cce878171f97c74af6d@152.42.133.145:30303?discport=30303",
	"enode://ebc29173c5191397c5440141fcbe10e0611d44179b5a1b7299f9e1db00849e3a119dde5744ff207201a65d6ed8b4abe83e78ea14af27fecdb60e782501bdb7e4@128.199.46.153:30303?discport=30303",
	"enode://dc53cb982c2a94421c779ab1fc67d4524df951b52a4078c6b681110c4438d9564d5d6049800859355ef6d01480bd68103caa378c73fc18a35135e6438f4c4a59@147.182.169.114:30303?discport=30303",
	"enode://2cfa672891371b101c368acb8ffabcd358b858a2efb076d0715b9f9f6ebb8488f8dc403023243f3425f20f7dce3e3f74be5bae45341b3a7f7f192e39574bf2f1@138.68.102.19:30303?discport=30303",
	"enode://35738a1b9a715804b32381ec50df37e21ba8185e86c8e50a0253f45031bbdd470b09db106307f7f6d94cb8e93be0d626547d8b2aec695c858b7fc30a6b3aaf5a@46.101.251.214:30303?discport=30303",
	"enode://6819ee9bcd72f4159989ff53e559b4c3af520399c5b1f291518722992cc40c284b78def5684534278bef92710507cd3273b6040b1cf7b805363411830caf9de0@209.38.88.251:30303?discport=30303",
	"enode://51b7b0dd3f7e82cb377ef608daebf4a191b7ab0b415306d6a3594be03aac1879619c80209c4ddb314ee104073d2c32cc2532f6cc2a1013171bbc8db4590b8f19@134.199.151.178:30303?discport=30303",
	"enode://d0d5201c903a35deada42cb885b8fe15a659ecd838635261b29f432e270a242208195607d5af0f1ece67df8d108146d763a893525d7739c932bba77f7e50ba59@139.59.78.193:30303?discport=30303",
	"enode://a43f3e59a9f179636044918f3212e3b519552d5251cbe34b1184a677f1d5589d5d00e6e8cb4ae81d634f98dbeb2b49ffbf22b6d6aa8fd4b1afc5f574a324ad60@68.183.94.49:30303?discport=30303",
	"enode://c4a35ec0045a7cf382f64b81f968a7fe9b8a482ee6b939e532cd73e25a4e6e675475d38594f6aec55236b308e35b959eed8f1f0ad41f2e1cf48e49c2dffbdb59@46.101.198.197:30303?discport=30303",
	"enode://2ad98bae7b82fe7e19e452e31c1d49d95ee5f88d6dd19992f84d09c4f6ccdccce2193fd324c334c9f15985e6a11f629e66c690f8432f05d4dce069e4ac255d62@209.38.249.244:30303?discport=30303",
	"enode://1497f73f6ba7e901bb619a3ffff2b17aa48b03e1a46ff89a235326656d3f9664d41e0a854fe710c0b1524e7f8f6799b377e505f484ad18de6101b56be1bf7ba5@128.199.52.206:30303?discport=30303",
	"enode://51dfa831b0e0b2038db24b598544ddaa2ee59e7f811a0a034c01922da71830b5169b778076dda3d436f6086c9c2fb604e5f09ae5a940184118dac90d16be6b26@167.99.223.80:30303?discport=30303",
	"enode://f61c11924a448d2881467f982bb826173ae97d9619a4f254e840a2d9835c4db3680fd0f3860caac716b355320009dc0463bc9248741517ca0a6ebc7ced2d260e@167.71.18.232:30303?discport=30303",
	"enode://fa8a28d738e7b5f020b0b1485f0f73a312917e95c067d7832fcdb9abdfd04dc420242d71868a0b759d18332138bffb358907c19d52da0bb40bd61d2edbcd880c@134.199.166.212:30303?discport=30303",
	"enode://857756de2f59b37290bc79b3e0d0b776d0ab8497dfd5564c056a067bde809f054319f864f9949afc2ef5a90985863471abbb46717f7f306d0fb00e930f55121c@134.199.153.170:30303?discport=30303",
}

var enodes []*enode.Node

func init() {
	for _, u := range enodeUrls {
		enodes = append(enodes, enode.MustParseV4(u))
	}
}
