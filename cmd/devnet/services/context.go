package services

import (
	"context"

	"github.com/ledgerwatch/erigon/cmd/devnet/devnet"
	"github.com/ledgerwatch/erigon/cmd/devnet/services/accounts"
	"github.com/ledgerwatch/erigon/cmd/devnet/services/polygon"
)

type ctxKey int

const (
	ckFaucet ctxKey = iota
)

func Faucet(ctx context.Context) *accounts.Faucet {
	if network := devnet.CurrentNetwork(ctx); network != nil {
		for _, service := range network.Services {
			if faucet, ok := service.(*accounts.Faucet); ok {
				return faucet
			}
		}
	}

	return nil
}

func Heimdall(ctx context.Context) *polygon.Heimdall {
	if network := devnet.CurrentNetwork(ctx); network != nil {
		for _, service := range network.Services {
			if heimdall, ok := service.(*polygon.Heimdall); ok {
				return heimdall
			}
		}
	}

	return nil
}

func ProofGenerator(ctx context.Context) *polygon.ProofGenerator {
	if network := devnet.CurrentNetwork(ctx); network != nil {
		for _, service := range network.Services {
			if proofGenerator, ok := service.(*polygon.ProofGenerator); ok {
				return proofGenerator
			}
		}
	}

	return nil
}
