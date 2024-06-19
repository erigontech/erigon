package bridge

import "fmt"

func bridgeLogPrefix(message string) string {
	return fmt.Sprintf("[bridge] %s", message)
}
