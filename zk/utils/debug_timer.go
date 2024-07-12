package utils

import (
	"fmt"
	"time"

	"github.com/ledgerwatch/log/v3"
)

var timerEnabled bool

type Timer struct {
	start     time.Time
	taskNames []string
}

func EnableTimer(enable bool) {
	timerEnabled = enable
}

func StartTimer(taskNames ...string) *Timer {
	if !timerEnabled {
		return nil
	}
	return &Timer{
		start:     time.Now(),
		taskNames: taskNames,
	}
}

func (t *Timer) LogTimer() {
	if !timerEnabled || t == nil {
		return
	}

	elapsed := time.Since(t.start)
	logArgs := []interface{}{"duration", elapsed, "task", t.taskNames[0]}

	for i, task := range t.taskNames[1:] {
		logArgs = append(logArgs, fmt.Sprintf("subtask%d", i+1), task)
	}

	log.Info("[cdk-metric]", logArgs...)
}
