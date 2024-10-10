package heimdall

import "fmt"

func heimdallLogPrefix(message string) string {
	return fmt.Sprintf("[bor.heimdall] %s", message)
}
