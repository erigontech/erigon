package sync

import "fmt"

func syncLogPrefix(message string) string {
	return fmt.Sprintf("[sync] %s", message)
}
