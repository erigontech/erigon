package stages

import (
	"context"
)

type Mode int8 // in which staged sync can run

const (
	ModeBlockProduction Mode = iota
	ModeForkValidation
	ModeApplyingBlocks
)

type modeContextKey struct{}

func PutMode(ctx context.Context, v Mode) context.Context {
	return context.WithValue(ctx, modeContextKey{}, v)
}
func GetMode(ctx context.Context) (Mode, bool) {
	v := ctx.Value(modeContextKey{})
	if v == nil {
		return 0, false
	}
	return v.(Mode), true
}
