package commands

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/cmd/utils"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus/clique"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/gointerfaces"
	proto_cons "github.com/ledgerwatch/turbo-geth/gointerfaces/consensus"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/metrics"
	"github.com/ledgerwatch/turbo-geth/params"
	toml "github.com/pelletier/go-toml"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/types/known/emptypb"
)

func init() {
	withApiAddr(cliqueCmd)
	withDatadir(cliqueCmd)
	withChain(cliqueCmd)
	rootCmd.AddCommand(cliqueCmd)
}

var cliqueCmd = &cobra.Command{
	Use:   "clique",
	Short: "Run clique consensus engine",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, _ := utils.RootContext()
		return cliqueEngine(ctx)
	},
}

func cliqueEngine(ctx context.Context) error {
	var genesis *core.Genesis
	var chainConfig *params.ChainConfig
	cliqueConfig := params.NewSnapshotConfig(10, 1024, 16384, false /* inMemory */, "clique", false /* mdbx */)
	switch chain {
	case params.RinkebyChainName:
		genesis = core.DefaultRinkebyGenesisBlock()
		chainConfig = params.RinkebyChainConfig
	case params.GoerliChainName:
		genesis = core.DefaultGoerliGenesisBlock()
		chainConfig = params.GoerliChainConfig
	case params.AleutChainName:
		genesis = core.DefaultAleutGenesisBlock()
		chainConfig = params.AleutChainConfig
	default:
		return fmt.Errorf("chain with name [%s] is not supported for clique", chain)
	}
	dbPath := filepath.Join(datadir, "clique", "db")
	db := openDatabase(dbPath)
	clique.New(chainConfig, cliqueConfig, db)
	server, err := grpcCliqueServer(ctx)
	if err != nil {
		return err
	}
	server.genesis = genesis
	server.chainConfig = chainConfig
	return nil
}

func grpcCliqueServer(ctx context.Context) (*CliqueServerImpl, error) {
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
	if metrics.Enabled {
		streamInterceptors = append(streamInterceptors, grpc_prometheus.StreamServerInterceptor)
		unaryInterceptors = append(unaryInterceptors, grpc_prometheus.UnaryServerInterceptor)
	}
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
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(streamInterceptors...)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(unaryInterceptors...)),
	}
	grpcServer = grpc.NewServer(opts...)

	cliqueServer := NewCliqueServer(ctx)
	proto_cons.RegisterConsensusEngineServer(grpcServer, cliqueServer)
	if metrics.Enabled {
		grpc_prometheus.Register(grpcServer)
	}
	go func() {
		if err1 := grpcServer.Serve(lis); err1 != nil {
			log.Error("Clique server fail", "err", err1)
		}
	}()
	return cliqueServer, nil
}

type CliqueServerImpl struct {
	proto_cons.UnimplementedConsensusEngineServer
	genesis     *core.Genesis
	chainConfig *params.ChainConfig
}

func NewCliqueServer(_ context.Context) *CliqueServerImpl {
	return &CliqueServerImpl{}
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
