package terminate

import (
	"context"
	"runtime"
	"syscall"
	"time"

	"github.com/ledgerwatch/log/v3"
	"github.com/shirou/gopsutil/v3/process"
)

func TryGracefully(ctx context.Context, logger log.Logger) {
	pid := syscall.Getpid()
	p, err := process.NewProcess(int32(pid))
	if err != nil {
		logger.Error("could not create process instance for current pid", "pid", pid, "err", err)
		return
	}

	//goland:noinspection GoBoolExpressions
	if runtime.GOOS == "windows" {
		logger.Info("can't terminate process gracefully on windows - killing")
		if err = p.Kill(); err != nil {
			logger.Error("could not kill current process", "err", err)
		}

		return
	}

	timer := time.NewTimer(15 * time.Second)
	defer timer.Stop()

	for attempt := 1; attempt <= 10; attempt++ {
		select {
		case <-ctx.Done():
			//
			// TODO - this actually does not work - we never get to 10 attempts
			//      - remove TryGracefully (it actually needs to spawn an independent process to supervise the gracefull shutdown...)
			//      - instead simply log the error and cancel context when polygon sync service crashes
			//      - or create a mechanism where there is a close channel that comes from the eri node container and notify it to close all services
			//
			return
		case <-timer.C:
			logger.Info("sending interrupt signal to current process", "attempt", attempt)
			if err = p.SendSignal(syscall.SIGINT); err != nil {
				logger.Error("could not send interrupt signal to current process", "err", err)
			}
		}
	}

	logger.Info("could not gracefully terminate process - killing")
	if err = p.Kill(); err != nil {
		logger.Error("could not kill current process", "err", err)
	}
}
