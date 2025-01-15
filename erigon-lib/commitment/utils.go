package commitment

import (
	"github.com/erigontech/erigon-lib/common/length"
	ecrypto "github.com/erigontech/erigon-lib/crypto"
)

// Hash the account or storage key and return the resulting hashed key in the nibblized form
func HashAndNibblizeKey(key []byte) []byte {
	keyLen := length.Addr
	if len(key) < length.Addr {
		keyLen = len(key)
	}
	compactedHashKey := ecrypto.Keccak256(key[:keyLen])
	if len(key) > length.Addr { // storage
		storageKeyHash := ecrypto.Keccak256(key[length.Addr:])
		compactedHashKey = append(compactedHashKey, storageKeyHash...)
	}
	return nibblize(compactedHashKey)
}
