package utils

import (
	"fmt"
	"time"

	"github.com/ledgerwatch/log/v3"
)

var timerEnabled bool

type Timer struct {
	start     time.Time
	elapsed   time.Duration
	taskNames []string
}

func EnableTimer(enable bool) {
	timerEnabled = enable
}

func StartTimer(taskNames ...string) *Timer {
	return &Timer{
		start:     time.Now(),
		taskNames: taskNames,
		elapsed:   0,
	}
}

func (t *Timer) LogTimer() {
	if !timerEnabled || t == nil {
		return
	}

	t.elapsed = time.Since(t.start)
	logMessage := fmt.Sprintf("duration: %s, task: %s", t.elapsed, t.taskNames[0])

	for i, task := range t.taskNames[1:] {
		logMessage += fmt.Sprintf(", subtask%d: %s", i+1, task)
	}

	log.Info("[cdk-metric] " + logMessage)
}

func (t *Timer) Elapsed() time.Duration {
	return t.elapsed
}
