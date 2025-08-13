// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package services

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/beacon/synced_data/mock_services"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	das_mock "github.com/erigontech/erigon/cl/das/mock_services"
	blob_storage_mock "github.com/erigontech/erigon/cl/persistence/blob_storage/mock_services"
	forkchoice_mock "github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
)

var (
	testSlot          = uint64(321)
	testEpoch         = uint64(10)
	testSlotsPerEpoch = uint64(32)
	testParentRoot    = common.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
)

type dataColumnSidecarTestSuite struct {
	suite.Suite
	gomockCtrl               *gomock.Controller
	mockForkChoice           *forkchoice_mock.ForkChoiceStorageMock
	mockSyncedData           *mock_services.MockSyncedData
	mockEthClock             *eth_clock.MockEthereumClock
	mockColumnSidecarStorage *blob_storage_mock.MockDataColumnStorage
	mockPeerDas              *das_mock.MockPeerDas
	dataColumnSidecarService DataColumnSidecarService
	beaconConfig             *clparams.BeaconChainConfig
	mockFuncs                *mockFuncs
}

func (t *dataColumnSidecarTestSuite) SetupTest() {
	t.gomockCtrl = gomock.NewController(t.T())
	t.mockForkChoice = forkchoice_mock.NewForkChoiceStorageMock(t.T())
	t.mockEthClock = eth_clock.NewMockEthereumClock(t.gomockCtrl)
	t.mockColumnSidecarStorage = blob_storage_mock.NewMockDataColumnStorage(t.gomockCtrl)
	t.mockPeerDas = das_mock.NewMockPeerDas(t.gomockCtrl)
	t.mockSyncedData = mock_services.NewMockSyncedData(t.gomockCtrl)
	t.mockForkChoice.MockPeerDas = t.mockPeerDas

	t.beaconConfig = &clparams.BeaconChainConfig{
		SlotsPerEpoch:    testSlotsPerEpoch,
		NumberOfColumns:  4,
		ElectraForkEpoch: 100000,
	}

	t.dataColumnSidecarService = NewDataColumnSidecarService(
		t.beaconConfig,
		t.mockEthClock,
		t.mockForkChoice,
		t.mockSyncedData,
		t.mockColumnSidecarStorage,
	)

	t.mockFuncs = &mockFuncs{
		ctrl: t.gomockCtrl,
	}
	verifyDataColumnSidecarInclusionProof = t.mockFuncs.VerifyDataColumnSidecarInclusionProof
	verifyDataColumnSidecarKZGProofs = t.mockFuncs.VerifyDataColumnSidecarKZGProofs
	verifyDataColumnSidecar = t.mockFuncs.VerifyDataColumnSidecar
	blsVerify = t.mockFuncs.BlsVerify
	blsVerifyMultipleSignatures = t.mockFuncs.BlsVerifyMultipleSignatures
	computeSigningRoot = t.mockFuncs.ComputeSigningRoot
}

func (t *dataColumnSidecarTestSuite) TearDownTest() {
	t.gomockCtrl.Finish()
}

func createMockDataColumnSidecar(slot uint64, index uint64) *cltypes.DataColumnSidecar {
	// Create a minimal but valid data column sidecar
	sidecar := &cltypes.DataColumnSidecar{
		Index: index,
		SignedBlockHeader: &cltypes.SignedBeaconBlockHeader{
			Header: &cltypes.BeaconBlockHeader{
				Slot:          slot,
				ParentRoot:    testParentRoot,
				ProposerIndex: 1,
				BodyRoot:      common.Hash{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			},
			Signature: [96]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95},
		},
	}

	// Initialize the sidecar with proper data structures manually
	// Use reasonable defaults instead of calling GetBeaconConfig()
	sidecar.Column = solid.NewStaticListSSZ[*cltypes.Cell](4, 32)
	sidecar.KzgCommitments = solid.NewStaticListSSZ[*cltypes.KZGCommitment](4, 48)
	sidecar.KzgProofs = solid.NewStaticListSSZ[*cltypes.KZGProof](4, 48)
	sidecar.KzgCommitmentsInclusionProof = solid.NewHashVector(32)

	// Add some mock data to make it pass validation
	// Add a mock KZG commitment
	commitment := &cltypes.KZGCommitment{}
	copy(commitment[:], []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48})
	sidecar.KzgCommitments.Append(commitment)

	// Add a mock KZG proof
	proof := &cltypes.KZGProof{}
	copy(proof[:], []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48})
	sidecar.KzgProofs.Append(proof)

	// Add a mock cell
	cell := &cltypes.Cell{}
	copy(cell[:], []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32})
	sidecar.Column.Append(cell)

	return sidecar
}

func TestDataColumnSidecarService(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	suite.Run(t, &dataColumnSidecarTestSuite{})
}
