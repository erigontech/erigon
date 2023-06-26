package freezeblocks_test

import (
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	types2 "github.com/ledgerwatch/erigon-lib/types"

	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
	"github.com/ledgerwatch/erigon/turbo/stages"
)

func TestDump(t *testing.T) {
	m := createDumpTestKV(t, 5)
	chainID, _ := uint256.FromBig(m.ChainConfig.ChainID)
	t.Run("txs", func(t *testing.T) {
		require := require.New(t)
		slot := types2.TxSlot{}
		parseCtx := types2.NewTxParseContext(*chainID)
		parseCtx.WithSender(false)
		var sender [20]byte

		var systemTxs int
		var nonceList []uint64
		cnt, err := freezeblocks.DumpTxs(m.Ctx, m.DB, 0, 10, m.ChainConfig, 1, log.LvlInfo, log.New(), func(v []byte) error {
			if v == nil {
				systemTxs++
			} else {
				if _, err := parseCtx.ParseTransaction(v[1+20:], 0, &slot, sender[:], false /* hasEnvelope */, false /* wrappedWithBlobs */, nil); err != nil {
					return err
				}
				nonceList = append(nonceList, slot.Nonce)
			}
			return nil
		})
		require.NoError(err)
		require.Equal(2*(5+1), systemTxs)
		require.Equal([]uint64{0, 1, 2, 3, 4}, nonceList)
		require.Equal(2*(5+1)+5, cnt)
	})
	t.Run("txs_not_from_zero", func(t *testing.T) {
		require := require.New(t)
		slot := types2.TxSlot{}
		parseCtx := types2.NewTxParseContext(*chainID)
		parseCtx.WithSender(false)
		var sender [20]byte

		var systemTxs int
		var nonceList []uint64
		cnt, err := freezeblocks.DumpTxs(m.Ctx, m.DB, 2, 5, m.ChainConfig, 1, log.LvlInfo, log.New(), func(v []byte) error {
			if v == nil {
				systemTxs++
			} else {
				if _, err := parseCtx.ParseTransaction(v[1+20:], 0, &slot, sender[:], false /* hasEnvelope */, false /* wrappedWithBlobs */, nil); err != nil {
					return err
				}
				nonceList = append(nonceList, slot.Nonce)
			}
			return nil
		})
		require.NoError(err)
		require.Equal(2*3, systemTxs)
		require.Equal([]uint64{1, 2, 3}, nonceList)
		require.Equal(2*3+3, cnt)
	})
	t.Run("headers", func(t *testing.T) {
		require := require.New(t)
		var nonceList []uint64
		err := freezeblocks.DumpHeaders(m.Ctx, m.DB, 0, 10, 1, log.LvlInfo, log.New(), func(v []byte) error {
			h := types.Header{}
			if err := rlp.DecodeBytes(v[1:], &h); err != nil {
				return err
			}
			nonceList = append(nonceList, h.Number.Uint64())
			return nil
		})
		require.NoError(err)
		require.Equal([]uint64{0, 1, 2, 3, 4, 5}, nonceList)
	})
	t.Run("headers_not_from_zero", func(t *testing.T) {
		require := require.New(t)
		var nonceList []uint64
		err := freezeblocks.DumpHeaders(m.Ctx, m.DB, 2, 5, 1, log.LvlInfo, log.New(), func(v []byte) error {
			h := types.Header{}
			if err := rlp.DecodeBytes(v[1:], &h); err != nil {
				return err
			}
			nonceList = append(nonceList, h.Number.Uint64())
			return nil
		})
		require.NoError(err)
		require.Equal([]uint64{2, 3, 4}, nonceList)
	})
	t.Run("body", func(t *testing.T) {
		require := require.New(t)
		i := 0
		txsAmount := uint64(0)
		var baseIdList []uint64
		firstTxNum := uint64(0)
		err := freezeblocks.DumpBodies(m.Ctx, m.DB, 0, 2, firstTxNum, 1, log.LvlInfo, log.New(), func(v []byte) error {
			i++
			body := &types.BodyForStorage{}
			require.NoError(rlp.DecodeBytes(v, body))
			txsAmount += uint64(body.TxAmount)
			baseIdList = append(baseIdList, body.BaseTxId)
			return nil
		})
		require.NoError(err)
		require.Equal(2, i)
		require.Equal(5, int(txsAmount))
		require.Equal([]uint64{0, 2}, baseIdList)

		firstTxNum += txsAmount
		i = 0
		baseIdList = baseIdList[:0]
		err = freezeblocks.DumpBodies(m.Ctx, m.DB, 2, 10, firstTxNum, 1, log.LvlInfo, log.New(), func(v []byte) error {
			i++
			body := &types.BodyForStorage{}
			require.NoError(rlp.DecodeBytes(v, body))
			txsAmount += uint64(body.TxAmount)
			baseIdList = append(baseIdList, body.BaseTxId)
			return nil
		})
		require.NoError(err)
		require.Equal(4, i)
		require.Equal(17, int(txsAmount))
		require.Equal([]uint64{5, 8, 11, 14}, baseIdList)
	})
	t.Run("body_not_from_zero", func(t *testing.T) {
		require := require.New(t)
		i := 0
		var baseIdList []uint64
		firstTxNum := uint64(1000)
		err := freezeblocks.DumpBodies(m.Ctx, m.DB, 2, 5, firstTxNum, 1, log.LvlInfo, log.New(), func(v []byte) error {
			i++
			body := &types.BodyForStorage{}
			require.NoError(rlp.DecodeBytes(v, body))
			baseIdList = append(baseIdList, body.BaseTxId)
			return nil
		})
		require.NoError(err)
		require.Equal(3, i)
		require.Equal([]uint64{1000, 1000 + 3, 1000 + 6}, baseIdList)
	})
}

func createDumpTestKV(t *testing.T, chainSize int) *stages.MockSentry {
	var (
		key, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		addr   = crypto.PubkeyToAddress(key.PublicKey)
		gspec  = &types.Genesis{
			Config: params.TestChainConfig,
			Alloc:  types.GenesisAlloc{addr: {Balance: big.NewInt(math.MaxInt64)}},
		}
		signer = types.LatestSigner(gspec.Config)
	)
	m := stages.MockWithGenesis(t, gspec, key, false)

	// Generate testing blocks
	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, chainSize, func(i int, b *core.BlockGen) {
		b.SetCoinbase(libcommon.Address{1})
		tx, txErr := types.SignTx(types.NewTransaction(b.TxNonce(addr), libcommon.HexToAddress("deadbeef"), uint256.NewInt(100), 21000, uint256.NewInt(uint64(int64(i+1)*params.GWei)), nil), *signer, key)
		if txErr != nil {
			t.Fatalf("failed to create tx: %v", txErr)
		}
		b.AddTx(tx)
	})
	if err != nil {
		t.Error(err)
	}
	// Construct testing chain
	if err = m.InsertChain(chain, nil); err != nil {
		t.Error(err)
	}

	return m
}
