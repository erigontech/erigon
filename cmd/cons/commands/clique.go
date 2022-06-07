package commands

import (
	"context"
	"embed"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	"github.com/holiman/uint256"
	common2 "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	proto_cons "github.com/ledgerwatch/erigon-lib/gointerfaces/consensus"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus/clique"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/log/v3"
	"github.com/pelletier/go-toml"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/types/known/emptypb"
)

//go:embed configs
var configs embed.FS

func init() {
	withApiAddr(cliqueCmd)
	withDataDir(cliqueCmd)
	withConfig(cliqueCmd)
	rootCmd.AddCommand(cliqueCmd)
}

var cliqueCmd = &cobra.Command{
	Use:   "clique",
	Short: "Run clique consensus engine",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, _ := common2.RootContext()
		logger := log.New()
		return cliqueEngine(ctx, logger)
	},
}

func cliqueEngine(ctx context.Context, logger log.Logger) error {
	var server *CliqueServerImpl
	var err error
	if config == "test" {
		// Configuration will be received from the test driver
		server, err = grpcCliqueServer(ctx, true /* testServer */)
		if err != nil {
			return err
		}
	} else {
		var configuration []byte
		if strings.HasPrefix(config, "embed:") {
			filename := config[len("embed:"):]
			if configuration, err = configs.ReadFile(filepath.Join("configs", filename)); err != nil {
				return fmt.Errorf("reading embedded configuration for %s: %w", filename, err)
			}
		} else if strings.HasPrefix(config, "file:") {
			filename := config[len("file:"):]
			if configuration, err = os.ReadFile(filename); err != nil {
				return fmt.Errorf("reading configuration from file %s: %w", filename, err)
			}
		} else {
			return fmt.Errorf("unrecognized config option: [%s], `file:<path>` to specify config file in file system, `embed:<path>` to use embedded file, `test` to register test interface and receive config from test driver", config)
		}
		if server, err = grpcCliqueServer(ctx, false /* testServer */); err != nil {
			return err
		}
		if err = server.initAndConfig(configuration); err != nil {
			return err
		}
	}
	server.db = openDB(filepath.Join(datadirCli, "clique", "db"), logger)
	server.c = clique.New(server.chainConfig, params.CliqueSnapshot, server.db)
	<-ctx.Done()
	return nil
}

func grpcCliqueServer(ctx context.Context, testServer bool) (*CliqueServerImpl, error) {
	// STARTING GRPC SERVER
	log.Info("Starting Clique server", "on", consensusAddr)
	listenConfig := net.ListenConfig{
		Control: func(network, address string, _ syscall.RawConn) error {
			log.Info("Clique received connection", "via", network, "from", address)
			return nil
		},
	}
	lis, err := listenConfig.Listen(ctx, "tcp", consensusAddr)
	if err != nil {
		return nil, fmt.Errorf("could not create Clique listener: %w, addr=%s", err, consensusAddr)
	}
	var (
		streamInterceptors []grpc.StreamServerInterceptor
		unaryInterceptors  []grpc.UnaryServerInterceptor
	)
	//if metrics.Enabled {
	//streamInterceptors = append(streamInterceptors, grpc_prometheus.StreamServerInterceptor)
	//unaryInterceptors = append(unaryInterceptors, grpc_prometheus.UnaryServerInterceptor)
	//}
	streamInterceptors = append(streamInterceptors, grpc_recovery.StreamServerInterceptor())
	unaryInterceptors = append(unaryInterceptors, grpc_recovery.UnaryServerInterceptor())
	var grpcServer *grpc.Server
	cpus := uint32(runtime.GOMAXPROCS(-1))
	opts := []grpc.ServerOption{
		grpc.NumStreamWorkers(cpus), // reduce amount of goroutines
		grpc.WriteBufferSize(1024),  // reduce buffers to save mem
		grpc.ReadBufferSize(1024),
		grpc.MaxConcurrentStreams(100), // to force clients reduce concurrency level
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time: 10 * time.Minute,
		}),
		// Don't drop the connection, settings accordign to this comment on GitHub
		// https://github.com/grpc/grpc-go/issues/3171#issuecomment-552796779
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(streamInterceptors...)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(unaryInterceptors...)),
	}
	grpcServer = grpc.NewServer(opts...)

	cliqueServer := NewCliqueServer(ctx)
	proto_cons.RegisterConsensusEngineServer(grpcServer, cliqueServer)
	if testServer {
		proto_cons.RegisterTestServer(grpcServer, cliqueServer)
	}
	//if metrics.Enabled {
	//	grpc_prometheus.Register(grpcServer)
	//}
	go func() {
		if err1 := grpcServer.Serve(lis); err1 != nil {
			log.Error("Clique server fail", "err", err1)
		}
	}()
	return cliqueServer, nil
}

type CliqueServerImpl struct {
	proto_cons.UnimplementedConsensusEngineServer
	proto_cons.UnimplementedTestServer
	genesis     *core.Genesis
	chainConfig *params.ChainConfig
	c           *clique.Clique
	db          kv.RwDB
}

func NewCliqueServer(_ context.Context) *CliqueServerImpl {
	return &CliqueServerImpl{}
}

// initAndConfig resets the Clique Engine and configures it according to given configuration
func (cs *CliqueServerImpl) initAndConfig(configuration []byte) error {
	tree, err := toml.LoadBytes(configuration)
	if err != nil {
		return err
	}
	var (
		epoch         int64
		period        int64
		vanityStr     string
		signersStr    []string
		gaslimit      int64
		timestamp     int64
		balances      *toml.Tree
		chainIdStr    string
		forksTree     *toml.Tree
		difficultyStr string
		ok            bool
	)
	if epoch, ok = tree.Get("engine.params.epoch").(int64); !ok {
		return fmt.Errorf("engine.params.epoch absent or of wrong type")
	}
	if period, ok = tree.Get("engine.params.period").(int64); !ok {
		return fmt.Errorf("engine.params.period absent or of wrong type")
	}
	if vanityStr, ok = tree.Get("genesis.vanity").(string); !ok {
		return fmt.Errorf("genesis.vanity absent or of wrong type")
	}
	if signersStr, ok = tree.GetArray("genesis.signers").([]string); !ok {
		return fmt.Errorf("signers absent or of wrong type")
	}
	if gaslimit, ok = tree.Get("genesis.gas_limit").(int64); !ok {
		return fmt.Errorf("genesis.gaslimit absent or of wrong type")
	}
	if timestamp, ok = tree.Get("genesis.timestamp").(int64); !ok {
		return fmt.Errorf("genesis.timestamp absent or of wrong type")
	}
	if difficultyStr, ok = tree.Get("genesis.difficulty").(string); !ok {
		return fmt.Errorf("genesis.difficulty absent or of wrong type")
	}
	if balances, ok = tree.Get("genesis.balances").(*toml.Tree); !ok {
		return fmt.Errorf("genesis.balances absent or of wrong type")
	}
	// construct chain config
	var chainConfig params.ChainConfig
	if chainIdStr, ok = tree.Get("genesis.chain_id").(string); !ok {
		return fmt.Errorf("genesis.chain_id absent or of wrong type")
	}
	var chainId big.Int
	chainId.SetBytes(common.Hex2Bytes(chainIdStr))
	chainConfig.ChainID = &chainId
	if forksTree, ok = tree.Get("forks").(*toml.Tree); !ok {
		return fmt.Errorf("forks absent or of wrong type")
	}
	for forkName, forkNumber := range forksTree.ToMap() {
		var number int64
		if number, ok = forkNumber.(int64); !ok {
			return fmt.Errorf("forks.%s is of a wrong type: %T", forkName, forkNumber)
		}
		bigNumber := big.NewInt(number)
		switch forkName {
		case "homestead":
			chainConfig.HomesteadBlock = bigNumber
		case "tangerine":
			chainConfig.TangerineWhistleBlock = bigNumber
		case "spurious":
			chainConfig.SpuriousDragonBlock = bigNumber
		case "byzantium":
			chainConfig.ByzantiumBlock = bigNumber
		case "constantinople":
			chainConfig.ConstantinopleBlock = bigNumber
		case "petersburg":
			chainConfig.PetersburgBlock = bigNumber
		case "istanbul":
			chainConfig.IstanbulBlock = bigNumber
		case "berlin":
			chainConfig.BerlinBlock = bigNumber
		case "london":
			chainConfig.LondonBlock = bigNumber
		default:
			return fmt.Errorf("unknown fork name [%s]", forkName)
		}
	}
	chainConfig.Clique = &params.CliqueConfig{
		Epoch:  uint64(epoch),
		Period: uint64(period),
	}
	// construct genesis
	var genesis core.Genesis
	genesis.Config = &chainConfig
	genesis.Timestamp = uint64(timestamp)
	genesis.ExtraData = common.FromHex(vanityStr)
	for _, signer := range signersStr {
		genesis.ExtraData = append(genesis.ExtraData, common.HexToAddress(signer).Bytes()...)
	}
	genesis.ExtraData = append(genesis.ExtraData, make([]byte, clique.ExtraSeal)...)
	genesis.GasLimit = uint64(gaslimit)
	genesis.Difficulty = new(big.Int).SetBytes(common.FromHex(difficultyStr))
	genesis.Alloc = make(core.GenesisAlloc)
	for account, balance := range balances.ToMap() {
		genesis.Alloc[common.HexToAddress(account)] = core.GenesisAccount{
			Balance: new(big.Int).SetBytes(common.FromHex(balance.(string))),
		}
	}
	var genesisBlock *types.Block
	if genesisBlock, _, err = genesis.ToBlock(); err != nil {
		return fmt.Errorf("creating genesis block: %w", err)
	}
	log.Info("Created genesis block", "hash", genesisBlock.Hash())
	return nil
}

// StartTestCase implements Test interface from consensus.proto
// When called, it signals to the consensus engine to reset its state and re-initialise using configuration
// received from the test driver
func (cs *CliqueServerImpl) StartTestCase(_ context.Context, testCase *proto_cons.StartTestCaseMessage) (*emptypb.Empty, error) {
	if testCase.Mechanism != "clique" {
		return &emptypb.Empty{}, fmt.Errorf("expected mechanism [clique], got [%s]", testCase.Mechanism)
	}
	if err := cs.initAndConfig(testCase.Config); err != nil {
		return &emptypb.Empty{}, err
	}
	return &emptypb.Empty{}, nil
}

func (cs *CliqueServerImpl) ChainSpec(context.Context, *emptypb.Empty) (*proto_cons.ChainSpecMessage, error) {
	var cliqueConfig []byte
	var forks []*proto_cons.Fork
	var chainId uint256.Int
	if cs.chainConfig.ChainID != nil {
		chainId.SetFromBig(cs.chainConfig.ChainID)
	}
	var genesisDiff uint256.Int
	if cs.genesis.Difficulty != nil {
		genesisDiff.SetFromBig(cs.genesis.Difficulty)
	}
	var extraWithoutSigners []byte
	extraWithoutSigners = append(extraWithoutSigners, cs.genesis.ExtraData[:clique.ExtraVanity]...)
	extraWithoutSigners = append(extraWithoutSigners, cs.genesis.ExtraData[len(cs.genesis.ExtraData)-clique.ExtraSeal:]...)
	tomlTree, err := toml.TreeFromMap(make(map[string]interface{}))
	if err != nil {
		return nil, err
	}
	genesisSigners := make([]string, (len(cs.genesis.ExtraData)-clique.ExtraVanity-clique.ExtraSeal)/common.AddressLength)
	for i := 0; i < len(genesisSigners); i++ {
		genesisSigners[i] = fmt.Sprintf("%x", cs.genesis.ExtraData[clique.ExtraVanity+i*common.AddressLength:])
	}
	tomlTree.Set("genesis.signers", genesisSigners)
	cliqueConfig, err = tomlTree.Marshal()
	if err != nil {
		return nil, err
	}
	return &proto_cons.ChainSpecMessage{
		Mechanism:       "clique",
		MechanismConfig: cliqueConfig,
		Genesis: &proto_cons.Genesis{
			ChainId: gointerfaces.ConvertUint256IntToH256(&chainId),
			Template: &proto_cons.Template{
				ParentHash: gointerfaces.ConvertHashToH256(cs.genesis.ParentHash),
				Coinbase:   gointerfaces.ConvertAddressToH160(cs.genesis.Coinbase),
				Difficulty: gointerfaces.ConvertUint256IntToH256(&genesisDiff),
				Number:     cs.genesis.Number,
				GasLimit:   cs.genesis.GasLimit,
				Time:       cs.genesis.Timestamp,
				Extra:      extraWithoutSigners, // Initial signers are passed in the clique-specific configuration
				Nonce:      cs.genesis.Nonce,
			},
		},
		Forks: forks,
	}, nil
}
