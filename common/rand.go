package common

import (
	"crypto/rand"
	"math"
	"math/big"
)

func CryptoInt64() *big.Int {
	n, _ := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
	return n
}
