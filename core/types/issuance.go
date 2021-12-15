package types

import (
	"encoding/json"
	"math/big"
)

type BlockIssuance struct {
	BlockReward *big.Int `json:"blockReward"`
	UncleReward *big.Int `json:"uncleReward"`
	Issuance    *big.Int `json:"issuance"`
	TotalIssued *big.Int `json:"totalIssued"`
	TotalBurnt  *big.Int `json:"totalBurnt"`
}

func Encode(data []byte) (BlockIssuance, error) {
	var issuance BlockIssuance
	err := json.Unmarshal(data, &issuance)
	return issuance, err
}
