package log

import (
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Logger - limited *zap.SugaredLogger interface
type Logger interface {
	Debugf(template string, args ...interface{})
	Infof(template string, args ...interface{})
	Warnf(template string, args ...interface{})
	Errorf(template string, args ...interface{})
	Named(name string) Logger
}

type LoggerImpl struct {
	*zap.SugaredLogger
}

func (l *LoggerImpl) Named(name string) Logger {
	return &LoggerImpl{l.SugaredLogger.Named(name)}
}

func New() *LoggerImpl {
	ll, err := cfg().Build()
	if err != nil {
		panic(err)
	}
	return &LoggerImpl{ll.Sugar()}
}

func cfg() zap.Config {
	cfg := zap.NewProductionConfig()
	cfg.Encoding = "console"
	cfg.EncoderConfig.EncodeTime = gethCompatibleTime //zapcore.RFC3339TimeEncoder
	cfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	return cfg
}
func NewTest(tb testing.TB) *LoggerImpl {
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
	return &LoggerImpl{logger}
}

func gethCompatibleTime(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
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
