package util

import (
	"math/rand"
	"time"
)

var (
	provider = rand.New(rand.NewSource(time.Now().UnixNano()))
)

// RandomProvider returns a centralized random provider, seeded with time.Now().UnixNano().
func RandomProvider() *rand.Rand {
	return provider
}
