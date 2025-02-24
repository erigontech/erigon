//go:build integration

package txpool

import (
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cmd/devnet/requests"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/tests/txpool/helper"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

var (
	// addr1 = 0x71562b71999873DB5b286dF957af199Ec94617F7
	pkey1, _ = crypto.HexToECDSA("26e86e45f6fc45ec6e2ecd128cec80fa1d1505e5507dcd2ae58c3130a7a97b48")
	addr1    = crypto.PubkeyToAddress(pkey1.PublicKey)

	// addr2 = 0x9fB29AAc15b9A4B7F17c3385939b007540f4d791
	pkey2, _ = crypto.HexToECDSA("45a915e4d060149eb4365960e6a7a45f334393093061116b197e3240065ff2d8")
	addr2    = crypto.PubkeyToAddress(pkey2.PublicKey)

	rpcAddressNode1 = "localhost:8545"
	rpcAddressNode2 = "localhost:8546"
)

// Topology of the network:
// p2p_helper ---> node1 <--- RPC txns
//
// This test sends transaction to node1 RPC which means they are local for node1
// P2P helper is binded to node1 port, that's why we measure performance of local txs processing
func TestSimpleLocalTxThroughputBenchmark(t *testing.T) {
	t.Skip()

	txToSendCount := 15000
	measureAtEvery := 1000

	p2p := helper.NewP2P(fmt.Sprintf("http://%s/", rpcAddressNode1))

	gotTxCh, errCh, err := p2p.Connect()
	require.NoError(t, err)

	start := time.Now()

	// sender part
	go func() {
		rpcClient := requests.NewRequestGenerator(
			rpcAddressNode1,
			log.New(),
		)

		for i := 0; i < txToSendCount; i++ {
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
			require.NoError(t, err)

			_, err = rpcClient.SendTransaction(signedTx)
			require.NoError(t, err)
		}
	}()

	lastMeasureTime := time.Now()
	gotTx := 0

	for gotTx < txToSendCount {
		select {
		case msg := <-gotTxCh:
			if msg.MessageID != sentryproto.MessageId_TRANSACTIONS_66 {
				continue
			}

			gotTx += 1

			if gotTx%measureAtEvery != 0 {
				continue
			}

			fmt.Printf("Tx/s: (%d txs processed): %.2f / s \n", measureAtEvery, float64(measureAtEvery)*float64(time.Second)/float64(time.Since(lastMeasureTime)))
			lastMeasureTime = time.Now()

		case err := <-errCh:
			require.NoError(t, err)
		}
	}

	fmt.Printf("\nTx/s: (total %d txs processed): %.2f / s \n", txToSendCount, float64(txToSendCount)*float64(time.Second)/float64(time.Since(start)))
	fmt.Println("Processed time:", time.Since(start))
}

// Topology of the network:
// p2p_helper ---> node1 <--- RPC txns
//
// This test sends transaction to node1 RPC which means they are local for node1
// P2P helper is binded to node1 port, that's why we measure performance of local txs processing
func TestSimpleLocalTxLatencyBenchmark(t *testing.T) {
	t.Skip()

	txToSendCount := 1000

	p2p := helper.NewP2P(fmt.Sprintf("http://%s/", rpcAddressNode1))

	gotTxCh, errCh, err := p2p.Connect()
	require.NoError(t, err)

	rpcClient := requests.NewRequestGenerator(
		rpcAddressNode1,
		log.New(),
	)

	averageLatency := time.Duration(0)

	for i := 0; i < txToSendCount; i++ {
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
		require.NoError(t, err)

		start := time.Now()

		_, err = rpcClient.SendTransaction(signedTx)
		require.NoError(t, err)

		for stop := false; !stop; {
			select {
			case msg := <-gotTxCh:
				if msg.MessageID == sentryproto.MessageId_TRANSACTIONS_66 {
					stop = true
				}
			case err := <-errCh:
				require.NoError(t, err)
			}
		}

		averageLatency += time.Since(start)
	}

	averageLatency = averageLatency / time.Duration(txToSendCount)
	fmt.Println("Avg latency:", averageLatency)
}
