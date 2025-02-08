package testhelpers

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/rand"
	"math/big"
	mathrand "math/rand"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon/txnprovider/shutter"
	shuttercrypto "github.com/erigontech/erigon/txnprovider/shutter/internal/crypto"
	"github.com/erigontech/erigon/txnprovider/shutter/internal/proto"
)

type EonKeyGeneration struct {
	EonIndex        shutter.EonIndex
	ActivationBlock uint64
	Threshold       uint64
	Keypers         []Keyper
	EonPublicKey    *shuttercrypto.EonPublicKey
}

func (ekg EonKeyGeneration) Eon() shutter.Eon {
	members := make([]libcommon.Address, len(ekg.Keypers))
	for i, keyper := range ekg.Keypers {
		members[i] = keyper.Address()
	}

	return shutter.Eon{
		Index:           ekg.EonIndex,
		ActivationBlock: ekg.ActivationBlock,
		Threshold:       ekg.Threshold,
		Members:         members,
		Key:             ekg.EonPublicKey.Marshal(),
	}
}

func (ekg EonKeyGeneration) DecryptionKeys(slot uint64, ips ...shutter.IdentityPreimage) []*proto.Key {
	slotIp := makeSlotIdentityPreimage(slot)
	allIps := append([]shutter.IdentityPreimage{slotIp}, ips...)
	keys := make([]*proto.Key, len(allIps))
	for i, ip := range allIps {
		epochSecretKey := ekg.EpochSecretKey(ip)
		keys[i] = &proto.Key{
			IdentityPreimage: ip,
			Key:              epochSecretKey.Marshal(),
		}
	}

	return keys
}

func (ekg EonKeyGeneration) EpochSecretKey(ip shutter.IdentityPreimage) *shuttercrypto.EpochSecretKey {
	keypers := make([]Keyper, len(ekg.Keypers))
	copy(keypers, ekg.Keypers)
	mathrand.Shuffle(int(ekg.Threshold), func(i, j int) { keypers[i], keypers[j] = keypers[j], keypers[i] })
	keypers = keypers[:ekg.Threshold]
	epochSecretKeyShares := make([]*shuttercrypto.EpochSecretKeyShare, len(keypers))
	keyperIndices := make([]int, len(keypers))
	for i, keyper := range keypers {
		keyperIndices[i] = keyper.Index
		epochSecretKeyShares[i] = keyper.EpochSecretKeyShare(ip)
	}

	epochSecretKey, err := shuttercrypto.ComputeEpochSecretKey(keyperIndices, epochSecretKeyShares, ekg.Threshold)
	if err != nil {
		panic(err)
	}

	return epochSecretKey
}

type Keyper struct {
	Index             int
	PrivateKey        *ecdsa.PrivateKey
	EonSecretKeyShare *shuttercrypto.EonSecretKeyShare
	EonPublicKeyShare *shuttercrypto.EonPublicKeyShare
}

func (k Keyper) PublicKey() ecdsa.PublicKey {
	return k.PrivateKey.PublicKey
}

func (k Keyper) Address() libcommon.Address {
	return crypto.PubkeyToAddress(k.PublicKey())
}

func (k Keyper) EpochSecretKeyShare(ip shutter.IdentityPreimage) *shuttercrypto.EpochSecretKeyShare {
	id := shuttercrypto.ComputeEpochID(ip)
	return shuttercrypto.ComputeEpochSecretKeyShare(k.EonSecretKeyShare, id)
}

func MockEonKeyGeneration() EonKeyGeneration {
	threshold := uint64(2)
	numKeypers := uint64(3)
	keypers := make([]Keyper, numKeypers)
	polynomials := make([]*shuttercrypto.Polynomial, numKeypers)
	gammas := make([]*shuttercrypto.Gammas, numKeypers)
	for i := 0; i < int(numKeypers); i++ {
		polynomial, err := shuttercrypto.RandomPolynomial(rand.Reader, threshold-1)
		if err != nil {
			panic(err)
		}

		polynomials[i] = polynomial
		gammas[i] = polynomial.Gammas()
	}

	for i := 0; i < int(numKeypers); i++ {
		privKey, err := crypto.GenerateKey()
		if err != nil {
			panic(err)
		}

		keyperX := shuttercrypto.KeyperX(i)
		polynomialEvals := make([]*big.Int, numKeypers)
		for j := 0; j < int(numKeypers); j++ {
			polynomialEvals[j] = polynomials[j].Eval(keyperX)
		}

		keypers[i] = Keyper{
			Index:             i,
			PrivateKey:        privKey,
			EonSecretKeyShare: shuttercrypto.ComputeEonSecretKeyShare(polynomialEvals),
			EonPublicKeyShare: shuttercrypto.ComputeEonPublicKeyShare(i, gammas),
		}
	}

	return EonKeyGeneration{
		EonIndex:        shutter.EonIndex(78),
		ActivationBlock: 32123,
		Threshold:       threshold,
		Keypers:         keypers,
		EonPublicKey:    shuttercrypto.ComputeEonPublicKey(gammas),
	}
}

func makeSlotIdentityPreimage(slot uint64) shutter.IdentityPreimage {
	// 32 bytes of zeros plus the block number as 20 byte big endian (ie starting with lots of
	// zeros as well). This ensures the block identity preimage is always alphanumerically before
	// any transaction identity preimages, because sender addresses cannot be that small.
	var buf bytes.Buffer
	buf.Write(libcommon.BigToHash(libcommon.Big0).Bytes())
	buf.Write(libcommon.BigToHash(new(big.Int).SetUint64(slot)).Bytes()[12:])
	return buf.Bytes()
}
