// Instrumentation: log SELFDESTRUCT-related burn events.
// Detects three scenarios:
//   - SD-SELF:   real burn at SELFDESTRUCT time (self == beneficiary, balance > 0,
//                and the account is actually destroyed — pre-Cancun always, post-Cancun
//                only if just_created).
//   - FAKE-BURN: post-Cancun "would-have-been" burn that EIP-6780 turned into a no-op
//                (self == beneficiary on an existing contract, balance > 0).
//   - SD-FINAL:  emitted by the state package when an account is destroyed at tx end
//                with a non-zero balance (e.g. ETH arrived after SELFDESTRUCT).
package vm

import (
	"io"
	"log"
	"os"
	"sync"
)

// SDBurnKind classifies a SELFDESTRUCT-time event. It is independent of the
// balance > 0 check so tests can exercise every condition separately.
type SDBurnKind int

const (
	SDBurnNone SDBurnKind = iota
	SDBurnSelf            // real burn (pre-6780 or post-6780 newContract) with self == beneficiary
	SDBurnFake            // post-6780 would-be burn blocked by EIP-6780 (!newContract)
)

// ClassifySDBurn returns the kind of burn event for an opcode-time SELFDESTRUCT.
// Pure function — all inputs are already extracted from the EVM state.
//
//	post6780:    chain is at or past Cancun.
//	newContract: account was created in the current tx (EIP-6780 trigger).
//	selfEqBen:   beneficiary address equals the contract's own address.
//	balanceZero: contract's balance is zero.
func ClassifySDBurn(post6780, newContract, selfEqBen, balanceZero bool) SDBurnKind {
	if balanceZero || !selfEqBen {
		return SDBurnNone
	}
	if !post6780 || newContract {
		return SDBurnSelf
	}
	return SDBurnFake
}

var (
	sdBurnOnce sync.Once
	sdBurnLog  *log.Logger
)

// sdBurnPath is the default log file; overridable via env SD_BURN_LOG.
const sdBurnDefaultPath = "/tmp/sd_burns.log"

func sdBurnInit() {
	path := os.Getenv("SD_BURN_LOG")
	if path == "" {
		path = sdBurnDefaultPath
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		sdBurnLog = log.New(io.Discard, "", 0)
		return
	}
	sdBurnLog = log.New(f, "", 0)
}

// SDBurnLog writes one line to the burn log. Safe for concurrent use
// (log.Logger uses an internal mutex and O_APPEND makes small writes atomic).
func SDBurnLog(format string, args ...any) {
	sdBurnOnce.Do(sdBurnInit)
	sdBurnLog.Printf(format, args...)
}

// SetSDBurnLogWriter redirects output for testing. Not concurrency-safe with
// ongoing SDBurnLog calls — intended for test setup only.
func SetSDBurnLogWriter(w io.Writer) {
	sdBurnOnce.Do(func() {}) // mark init done so sdBurnInit doesn't overwrite
	sdBurnLog = log.New(w, "", 0)
}
