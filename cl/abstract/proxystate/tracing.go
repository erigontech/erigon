package proxystate

import "strings"

func IsWriteOp(op string) bool {
	prefixes := []string{"Set", "Add", "Increment", "Append", "Reset"}
	for _, prefix := range prefixes {
		if strings.HasPrefix(op, prefix) {
			return true
		}
	}
	return false
}
