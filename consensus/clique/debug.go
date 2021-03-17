package clique

import (
	"fmt"

	"github.com/ledgerwatch/turbo-geth/log"
)

func debugLog(args ...interface{}) {
	log.Debug(fmt.Sprint(args...))
}

func debugLogf(format string, args ...interface{}) {
	log.Debug(fmt.Sprintf(format, args[1:]...))
}
