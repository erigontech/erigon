package execmodule_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/generics"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/types"
)

func retryBusy[T any](ctx context.Context, f func() (T, bool, error)) (T, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	var b backoff.BackOff
	b = backoff.NewConstantBackOff(time.Millisecond)
	b = backoff.WithContext(b, ctx)
	return backoff.RetryWithData(
		func() (T, error) {
			r, busy, err := f()
			if err != nil {
				return generics.Zero[T](), backoff.Permanent(err)
			}
			if busy {
				return generics.Zero[T](), errors.New("retrying busy")
			}
			return r, nil
		},
		b,
	)
}

func insertBlocks(ctx context.Context, exec *execmodule.ExecModule, blocks []*types.Block) (execmodule.ExecutionStatus, error) {
	return retryBusy(ctx, func() (execmodule.ExecutionStatus, bool, error) {
		status, err := exec.InsertBlocks(ctx, blocks)
		if err != nil {
			return execmodule.ExecutionStatusBusy, false, err
		}
		return status, status == execmodule.ExecutionStatusBusy, nil
	})
}

func validateChain(ctx context.Context, exec *execmodule.ExecModule, b *types.Block) (execmodule.ValidationResult, error) {
	return retryBusy(ctx, func() (execmodule.ValidationResult, bool, error) {
		r, err := exec.ValidateChain(ctx, b)
		if err != nil {
			return execmodule.ValidationResult{}, false, err
		}
		return r, r.ValidationStatus == execmodule.ExecutionStatusBusy, nil
	})
}

func forkChoiceTo(ctx context.Context, exec *execmodule.ExecModule, h *types.Header) (execmodule.ForkChoiceResult, error) {
	return retryBusy(ctx, func() (execmodule.ForkChoiceResult, bool, error) {
		r, err := exec.UpdateForkChoice(ctx, h.Hash(), common.Hash{}, common.Hash{})
		if err != nil {
			return execmodule.ForkChoiceResult{}, false, err
		}
		return r, r.Status == execmodule.ExecutionStatusBusy, nil
	})
}

func insertValidateAndUfc1By1(ctx context.Context, exec *execmodule.ExecModule, blocks []*types.Block) error {
	ir, err := insertBlocks(ctx, exec, blocks)
	if err != nil {
		return err
	}
	if ir != execmodule.ExecutionStatusSuccess {
		return fmt.Errorf("unexpected insertBlocks status: %s", ir)
	}
	for _, b := range blocks {
		vr, err := validateChain(ctx, exec, b)
		if err != nil {
			return err
		}
		if vr.ValidationStatus != execmodule.ExecutionStatusSuccess {
			return fmt.Errorf("unexpected validateChain status: %s: %s", vr.ValidationStatus, vr.ValidationError)
		}
		ur, err := forkChoiceTo(ctx, exec, b.Header())
		if err != nil {
			return err
		}
		if ur.Status != execmodule.ExecutionStatusSuccess {
			return fmt.Errorf("unexpected updateForkChoice status: %s", ur.Status)
		}
	}
	// UpdateForkChoice answers before its commit settles; one more FCU waits for the previous commit.
	if len(blocks) > 0 {
		if _, err := forkChoiceTo(ctx, exec, blocks[len(blocks)-1].Header()); err != nil {
			return err
		}
	}
	return nil
}
