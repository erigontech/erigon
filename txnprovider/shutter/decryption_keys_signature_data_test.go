package shutter_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/txnprovider/shutter"
	"github.com/erigontech/erigon/txnprovider/shutter/internal/testhelpers"
)

func TestIdentityPreimageEncodeDecodeSSZ(t *testing.T) {
	ip := testhelpers.Uint64ToIdentityPreimage(t, 123)
	buf, err := ip.EncodeSSZ(nil)
	require.NoError(t, err)
	ip2, err := shutter.IdentityPreimageFromBytes(buf)
	require.NoError(t, err)
	require.Equal(t, ip, ip2)
}

func TestIdentityPreimageDecodeSSZWithInvalidLength(t *testing.T) {
	buf := make([]byte, 39)
	_, err := shutter.IdentityPreimageFromBytes(buf)
	require.ErrorIs(t, err, shutter.ErrIncorrectIdentityPreimageSize)
	buf = make([]byte, 64)
	_, err = shutter.IdentityPreimageFromBytes(buf)
	require.ErrorIs(t, err, shutter.ErrIncorrectIdentityPreimageSize)
}

func TestDecryptionKeysSignatureDataWithInvalidPreimagesLength(t *testing.T) {
	ips := testhelpers.MockIdentityPreimages(t, 1025)
	sigData := shutter.DecryptionKeysSignatureData{
		InstanceId:        1,
		Eon:               2,
		Slot:              3,
		TxnPointer:        4,
		IdentityPreimages: ips.ToListSSZ(),
	}

	err := sigData.Validate()
	require.ErrorIs(t, err, shutter.ErrTooManyIdentityPreimages)
	_, err = sigData.HashSSZ()
	require.ErrorIs(t, err, shutter.ErrTooManyIdentityPreimages)
	_, err = sigData.Verify(nil, libcommon.Address{})
	require.ErrorIs(t, err, shutter.ErrTooManyIdentityPreimages)
	_, err = sigData.Sign(nil)
	require.ErrorIs(t, err, shutter.ErrTooManyIdentityPreimages)
}

func TestDecryptionKeysSignatureDataHashSsz(t *testing.T) {
	// cross-referencing the hash that is produced by github.com/shutter-network/rolling-shutter
	// for the same signature data input
	want := "259bf7718b7430abc238ec0ac3260574dd73d23005adec26eed1a655ccdcc1ec"
	slot := uint64(6336)
	sigData := shutter.DecryptionKeysSignatureData{
		InstanceId:        123,
		Eon:               76,
		Slot:              slot,
		TxnPointer:        556,
		IdentityPreimages: testhelpers.MockIdentityPreimagesWithSlotIp(t, slot, 2).ToListSSZ(),
	}

	have, err := sigData.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, want, fmt.Sprintf("%x", have))
}
