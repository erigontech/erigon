//go:build integration

package p2p

import "time"

func init() {
	dialTestDialerUnexpectedDialTimeout = 150 * time.Millisecond
}
