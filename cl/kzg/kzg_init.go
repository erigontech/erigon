package kzg

import (
	_ "embed"
	"encoding/json"
	"sync"

	"github.com/erigontech/erigon-lib/common"
	ckzg "github.com/ethereum/c-kzg-4844/v2/bindings/go"
)

var initOnce sync.Once

//go:embed trusted_setup_4096.json
var trustedSetup []byte

type setup struct {
	G1Monomial []string `json:"g1_monomial"`
	G1Lagrange []string `json:"g1_lagrange"`
	G2Monomial []string `json:"g2_monomial"`
}

func InitKZG() {
	initOnce.Do(func() {
		var setup setup
		if err := json.Unmarshal(trustedSetup, &setup); err != nil {
			panic(err)
		}

		var g1MonomialBytes, g1LagrangeBytes, g2MonomialBytes []byte
		for _, s := range setup.G1Monomial {
			g1MonomialBytes = append(g1MonomialBytes, common.FromHex(s)...)
		}
		for _, s := range setup.G1Lagrange {
			g1LagrangeBytes = append(g1LagrangeBytes, common.FromHex(s)...)
		}
		for _, s := range setup.G2Monomial {
			g2MonomialBytes = append(g2MonomialBytes, common.FromHex(s)...)
		}

		if err := ckzg.LoadTrustedSetup(g1MonomialBytes, g1LagrangeBytes, g2MonomialBytes, 8); err != nil {
			panic(err)
		}
	})
}
