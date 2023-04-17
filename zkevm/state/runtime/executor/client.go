package executor

import (
	"context"
	"os/exec"
	"time"

	"github.com/ledgerwatch/erigon/zkevm/log"
	"github.com/ledgerwatch/erigon/zkevm/state/runtime/executor/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const maxMsgSize = 100000000

// NewExecutorClient is the executor client constructor.
func NewExecutorClient(ctx context.Context, c Config) (pb.ExecutorServiceClient, *grpc.ClientConn, context.CancelFunc) {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize)),
		grpc.WithBlock(),
	}
	const maxWaitSeconds = 120
	const maxRetries = 5
	ctx, cancel := context.WithTimeout(ctx, maxWaitSeconds*time.Second)

	connectionRetries := 0

	var executorConn *grpc.ClientConn
	var err error
	delay := 2
	for connectionRetries < maxRetries {
		log.Infof("trying to connect to executor: %v", c.URI)
		executorConn, err = grpc.DialContext(ctx, c.URI, opts...)
		if err != nil {
			log.Infof("Retrying connection to executor #%d", connectionRetries)
			time.Sleep(time.Duration(delay) * time.Second)
			connectionRetries = connectionRetries + 1
			out, err := exec.Command("docker", []string{"logs", "zkevm-prover"}...).Output()
			if err == nil {
				log.Infof("Prover logs:\n%s\n", out)
			}
		} else {
			log.Infof("connected to executor")
			break
		}
	}

	if connectionRetries == maxRetries {
		log.Fatalf("fail to dial: %v", err)
	}
	executorClient := pb.NewExecutorServiceClient(executorConn)
	return executorClient, executorConn, cancel
}
