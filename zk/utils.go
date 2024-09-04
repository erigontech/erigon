package zk

import (
	"errors"
	"fmt"
	"time"

	"github.com/ledgerwatch/log/v3"
	"sync"
)

var ErrLimboState = errors.New("Calculating limbo state")

// prints progress every 10 seconds
// returns a channel to send progress to, and a function to stop the printer routine
func ProgressPrinter(message string, total uint64, quiet bool) (chan uint64, func()) {
	progress := make(chan uint64)
	ctDone := make(chan bool)
	var once sync.Once

	cleanup := func() {
		once.Do(func() {
			close(ctDone)
		})
	}

	go func() {
		defer close(progress)
		defer cleanup()

		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		var pc uint64
		var pct uint64

		for {
			select {
			case newPc := <-progress:
				pc += newPc
				if total > 0 {
					pct = (pc * 100) / total
				}
				if pct == 100 {
					log.Info(fmt.Sprintf("%s: %d/%d (%d%%)", message, pc, total, pct))
				}
			case <-ticker.C:
				if pc > 0 && !quiet {
					log.Info(fmt.Sprintf("%s: %d/%d (%d%%)", message, pc, total, pct))
				}
			case <-ctDone:
				return
			}
		}
	}()

	return progress, cleanup
}

// prints progress every 10 seconds
// returns a channel to send progress to, and a function to stop the printer routine
func ProgressPrinterWithoutTotal(message string) (chan uint64, func()) {
	progress := make(chan uint64)
	ctDone := make(chan bool)
	var once sync.Once

	cleanup := func() {
		once.Do(func() {
			close(ctDone)
		})
	}

	go func() {
		defer close(progress)
		defer cleanup()

		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		var pc uint64

		for {
			select {
			case newPc := <-progress:
				pc = newPc
			case <-ticker.C:
				if pc > 0 {
					log.Info(fmt.Sprintf("%s: %d", message, pc))
				}
			case <-ctDone:
				return
			}
		}
	}()

	return progress, cleanup
}

// prints progress every 10 seconds
// returns a channel to send progress to, and a function to stop the printer routine
func ProgressPrinterWithoutValues(message string, total uint64) (chan uint64, func()) {
	progress := make(chan uint64)
	ctDone := make(chan bool)
	var once sync.Once

	cleanup := func() {
		once.Do(func() {
			close(ctDone)
		})
	}

	go func() {
		defer close(progress)
		defer cleanup()

		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		var pc uint64
		var pct uint64

		for {
			select {
			case newPc := <-progress:
				pc = newPc
				if total > 0 {
					pct = (pc * 100) / total
				}
			case <-ticker.C:
				if pc > 0 {
					log.Info(fmt.Sprintf("%s: (%d%%)", message, pct))
				}
			case <-ctDone:
				return
			}
		}
	}()

	return progress, cleanup
}
