package accounts

import (
	"math/big"
)

func EtherAmount(amount float64) *big.Int {
	ether, _ := (&big.Float{}).Mul(big.NewFloat(1e18), big.NewFloat(amount)).Int(nil)
	return ether
}
