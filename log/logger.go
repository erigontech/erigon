/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

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
