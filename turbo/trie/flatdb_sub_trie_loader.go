package trie

import (
	"bytes"
	"fmt"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types/accounts"
)

type StreamReceiver interface {
	Receive(
		itemType StreamItem,
		accountKey []byte,
		storageKey []byte,
		accountValue *accounts.Account,
		storageValue []byte,
		hash []byte,
		hasTree bool,
		cutoff int,
	) error

	Result() SubTries
	Root() common.Hash
}

func makeCurrentKeyStr(k []byte) string {
	var currentKeyStr string
	if k == nil {
		currentKeyStr = "final"
	} else if len(k) < 4 {
		currentKeyStr = fmt.Sprintf("%x", k)
	} else {
		currentKeyStr = fmt.Sprintf("%x...", k[:4])
	}
	return currentKeyStr
}

func isSequenceOld(prev []byte, next []byte) bool {
	isSequence := false
	if bytes.HasPrefix(next, prev) {
		tail := next[len(prev):] // if tail has only zeroes, then no state records can be between fstl.nextHex and fstl.ihK
		isSequence = true
		for _, n := range tail {
			if n != 0 {
				isSequence = false
				break
			}
		}
	}

	return isSequence
}
