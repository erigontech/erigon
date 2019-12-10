package debug

import (
	"os"
	"sync"
)

var gerEnv sync.Once
var ThinHistory bool
func IsThinHistory() bool  {
	gerEnv.Do(func() {
		_, ThinHistory = os.LookupEnv("THIN_HISTORY")
	})
	return ThinHistory
}