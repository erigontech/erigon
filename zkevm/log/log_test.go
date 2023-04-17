package log

import (
	"testing"
)

func TestLogNotInitialized(t *testing.T) {
	Info("Test log.Info", " value is ", 10)
	Infof("Test log.Infof %d", 10)
	Infow("Test log.Infow", "value", 10)
	Debugf("Test log.Debugf %d", 10)
	Error("Test log.Error", " value is ", 10)
	Errorf("Test log.Errorf %d", 10)
	Errorw("Test log.Errorw", "value", 10)
	Warnf("Test log.Warnf %d", 10)
	Warnw("Test log.Warnw", "value", 10)
}

func TestLog(t *testing.T) {
	cfg := Config{
		Environment: EnvironmentDevelopment,
		Level:       "debug",
		Outputs:     []string{"stderr"}, //[]string{"stdout", "test.log"}
	}

	Init(cfg)

	Info("Test log.Info", " value is ", 10)
	Infof("Test log.Infof %d", 10)
	Infow("Test log.Infow", "value", 10)
	Debugf("Test log.Debugf %d", 10)
	Error("Test log.Error", " value is ", 10)
	Errorf("Test log.Errorf %d", 10)
	Errorw("Test log.Errorw", "value", 10)
	Warnf("Test log.Warnf %d", 10)
	Warnw("Test log.Warnw", "value", 10)
}
