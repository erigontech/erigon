package clstages

import (
	"context"
	"fmt"
	"time"

	"github.com/ledgerwatch/log/v3"
)

type StageGraph[CONFIG any, ARGUMENTS any] struct {
	ArgsFunc func(ctx context.Context, cfg CONFIG) (args ARGUMENTS)
	Stages   map[string]Stage[CONFIG, ARGUMENTS]
}

type Stage[CONFIG any, ARGUMENTS any] struct {
	Description    string
	ActionFunc     func(ctx context.Context, logger log.Logger, cfg CONFIG, args ARGUMENTS) error
	TransitionFunc func(cfg CONFIG, args ARGUMENTS, err error) string
}

func (s *StageGraph[CONFIG, ARGUMENTS]) StartWithStage(ctx context.Context, startStage string, logger log.Logger, cfg CONFIG) error {
	stageName := startStage
	args := s.ArgsFunc(ctx, cfg)
	for {
		currentStage, ok := s.Stages[stageName]
		if !ok {
			return fmt.Errorf("attempted to transition to unknown stage: %s", stageName)
		}
		lg := logger.New("stage", stageName)
		start := time.Now()
		sctx, cn := context.WithCancel(ctx)
		err := currentStage.ActionFunc(sctx, lg, cfg, args)
		if err != nil {
			lg.Error("error executing clstage", "err", err)
		}
		cn()
		dur := time.Since(start)
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			args = s.ArgsFunc(ctx, cfg)
			nextStage := currentStage.TransitionFunc(cfg, args, err)
			logger.Info("clstage finish", "stage", stageName, "in", dur, "next", nextStage)
			stageName = nextStage
		}
	}
}
