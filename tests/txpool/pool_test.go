package txpool

import (
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cmd/devnet/requests"
	"github.com/erigontech/erigon/core/types"
	"github.com/holiman/uint256"
)

var (
	// addr1 = 0x71562b71999873DB5b286dF957af199Ec94617F7
	pkey1, _ = crypto.HexToECDSA("26e86e45f6fc45ec6e2ecd128cec80fa1d1505e5507dcd2ae58c3130a7a97b48")
	addr1    = crypto.PubkeyToAddress(pkey1.PublicKey)

	// addr2 = 0x9fB29AAc15b9A4B7F17c3385939b007540f4d791
	pkey2, _ = crypto.HexToECDSA("45a915e4d060149eb4365960e6a7a45f334393093061116b197e3240065ff2d8")
	addr2    = crypto.PubkeyToAddress(pkey2.PublicKey)

	staticPeers = []string{
		"enode://ae1b5442a5e9ad831608a07c5c04c180c6aebea23dfb7958ff011c1a2ab6f7fef224d0fdb689cf12183284bb53aeffbada9a5a3e0a1078e85eba92d2d6a1fdab@127.0.0.1:30304",
	}
)

func TestA(t *testing.T) {
	nonce := 49723

	rpcClient := requests.NewRequestGenerator(
		"localhost:8545",
		log.New(),
	)

	start := time.Now()
	fmt.Println("time start", start)

	for i := nonce; i < nonce+20000; i++ {
		fmt.Println(i, time.Since(start))

		signedTx, err := types.SignTx(
			&types.LegacyTx{
				CommonTx: types.CommonTx{
					Nonce: uint64(i),
					Gas:   21000,
					To:    &addr2,
					Value: uint256.NewInt(100),
					Data:  nil,
				},
				GasPrice: uint256.NewInt(1),
			},
			*types.LatestSignerForChainID(big.NewInt(1337)),
			pkey1,
		)
		if err != nil {
			panic(err)
		}

		hash, err := rpcClient.SendTransaction(signedTx)
		if err != nil {
			panic(err)
		}

		fmt.Println(hash.String())
	}

	panic(1)
}
