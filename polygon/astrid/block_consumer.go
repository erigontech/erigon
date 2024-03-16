package astrid

import (
	"context"
	"errors"
	"runtime"
	"syscall"
	"time"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/direct"
	executionclient "github.com/ledgerwatch/erigon/cl/phase1/execution_client"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/polygon/sync"
)

type BlockConsumerDependencies struct {
	Logger          log.Logger
	ChainConfig     *chain.Config
	Sentries        []direct.SentryClient
	MaxPeers        int
	HeimdallUrl     string
	ExecutionEngine executionclient.ExecutionEngine
}

func RunBlockConsumer(ctx context.Context, dependencies *BlockConsumerDependencies) {
	//
	// TODO - pending sentry multi client refactor to be able to work with multiple sentries
	//
	var sentry67 direct.SentryClient
	for _, sentryClient := range dependencies.Sentries {
		if sentryClient.Protocol() == direct.ETH67 {
			sentry67 = sentryClient
			break
		}
	}
	if sentry67 == nil {
		terminateProcess(dependencies.Logger, errors.New("sentry 67 not found"))
		return
	}

	sync := sync.NewService(
		dependencies.Logger,
		dependencies.ChainConfig,
		sentry67,
		dependencies.MaxPeers,
		dependencies.HeimdallUrl,
		dependencies.ExecutionEngine,
	)

	err := sync.Run(ctx)
	if err == nil || errors.Is(err, context.Canceled) {
		return
	}

	terminateProcess(dependencies.Logger, err)
}

func terminateProcess(logger log.Logger, err error) {
	logger.Error("astrid block consumer crashed - terminating", "err", err)
	//goland:noinspection GoBoolExpressions
	if runtime.GOOS == "windows" {
		utils.Fatalf("astrid block consumer crashed - err=%v", err)
		return
	}

	timer := time.NewTimer(15 * time.Second)
	for {
		select {
		case <-timer.C:
			if err = syscall.Kill(syscall.Getpid(), syscall.SIGINT); err != nil {
				logger.Error("could not send term signal", "err", err)
			}
		}
	}
}
