package log

import (
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func New() *zap.SugaredLogger {
	ll, err := cfg().Build()
	if err != nil {
		panic(err)
	}
	return ll.Sugar()
}

func cfg() zap.Config {
	cfg := zap.NewProductionConfig()
	cfg.Encoding = "console"
	cfg.EncoderConfig.EncodeTime = GethCompatibleTime //zapcore.RFC3339TimeEncoder
	cfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	return cfg
}
func NewTest(tb testing.TB) *zap.SugaredLogger {
	cfg := cfg()
	cfg.Level.SetLevel(zapcore.DebugLevel)
	ll, err := cfg.Build(zap.AddCaller())
	if err != nil {
		panic(err)
	}
	logger := ll.Sugar()
	tb.Cleanup(func() {
		_ = logger.Sync()
	})
	return logger
}

func GethCompatibleTime(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	encodeTimeLayout(t, "01-02|15:04:05", enc)
}

func encodeTimeLayout(t time.Time, layout string, enc zapcore.PrimitiveArrayEncoder) {
	type appendTimeEncoder interface {
		AppendTimeLayout(time.Time, string)
	}

	if enc, ok := enc.(appendTimeEncoder); ok {
		enc.AppendTimeLayout(t, layout)
		return
	}

	enc.AppendString(t.Format(layout))
}
