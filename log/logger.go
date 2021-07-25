package log

import (
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
)

func New() *zap.SugaredLogger {
	cfg := zap.NewProductionConfig()
	cfg.Encoding = "console"
	cfg.EncoderConfig.EncodeTime = zapcore.RFC3339TimeEncoder
	cfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	ll, err := cfg.Build()
	if err != nil {
		panic(err)
	}
	return ll.Sugar()
}

func NewTest(tb testing.TB) *zap.SugaredLogger {
	return zaptest.NewLogger(tb,
		zaptest.Level(zap.InfoLevel),
		zaptest.WrapOptions(zap.AddCaller()),
		zaptest.WrapOptions(zap.AddStacktrace(zap.ErrorLevel)),
	).Sugar()
}

//zap.NewProductionConfig()
//logger := zap.New(zapcore.NewCore(
//	zapcore.NewConsoleEncoder(encoderCfg),
//	zapcore.Lock(os.Stdout),
//	zap.DebugLevel,
//), zap.AddCaller(), zap.AddStacktrace(zap.ErrorLevel)).Sugar()
