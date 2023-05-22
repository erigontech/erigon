package transition

import (
	"testing"

	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	state2 "github.com/ledgerwatch/erigon/cl/phase1/core/state"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/common"
)

const propInd = 49

var (
	testPublicKeySlashing         = [48]byte{152, 250, 212, 202, 139, 152, 8, 45, 224, 234, 128, 186, 142, 67, 172, 78, 124, 69, 173, 39, 48, 98, 75, 146, 244, 131, 55, 15, 138, 170, 246, 81, 77, 88, 244, 114, 1, 47, 123, 123, 55, 174, 58, 139, 90, 249, 237, 19}
	testPublicKey2Slashing        = [48]byte{176, 125, 127, 86, 74, 174, 45, 222, 192, 245, 150, 120, 40, 200, 37, 95, 67, 142, 18, 171, 27, 12, 49, 200, 170, 177, 234, 64, 9, 202, 152, 25, 98, 166, 136, 31, 59, 122, 32, 148, 218, 207, 12, 4, 117, 9, 119, 157}
	testSignature1Slashing        = [96]byte{144, 214, 122, 20, 108, 94, 14, 45, 68, 221, 175, 43, 187, 243, 162, 50, 135, 18, 184, 138, 227, 43, 191, 180, 180, 46, 94, 90, 62, 88, 63, 30, 110, 208, 113, 44, 137, 130, 163, 158, 227, 108, 23, 167, 243, 71, 32, 176, 21, 138, 152, 66, 143, 20, 119, 93, 5, 80, 49, 117, 115, 27, 58, 42, 68, 147, 9, 186, 67, 159, 216, 106, 192, 28, 156, 43, 4, 244, 122, 191, 191, 233, 35, 124, 69, 70, 25, 228, 196, 75, 196, 196, 234, 192, 73, 176}
	testSignature2Slashing        = [96]byte{153, 33, 67, 30, 188, 164, 75, 18, 158, 66, 161, 71, 177, 15, 242, 177, 167, 211, 15, 180, 226, 104, 138, 240, 190, 186, 167, 220, 91, 180, 39, 128, 221, 197, 235, 193, 48, 52, 28, 163, 173, 70, 249, 181, 53, 114, 12, 253, 11, 212, 80, 212, 57, 135, 102, 129, 174, 247, 126, 171, 69, 234, 48, 232, 128, 195, 156, 181, 13, 109, 99, 59, 7, 1, 3, 191, 186, 64, 131, 253, 27, 232, 3, 235, 82, 136, 216, 129, 68, 202, 1, 1, 192, 21, 1, 245}
	testInvalidSignatureSlashing  = [96]byte{175, 121, 194, 204, 23, 153, 149, 62, 85, 130, 13, 116, 164, 165, 251, 183, 234, 72, 227, 255, 153, 244, 144, 223, 56, 126, 249, 105, 167, 225, 181, 244, 124, 0, 105, 244, 119, 44, 220, 81, 61, 86, 39, 0, 160, 141, 129, 189, 20, 63, 56, 209, 84, 8, 109, 198, 218, 205, 81, 27, 158, 233, 122, 30, 63, 146, 110, 9, 161, 170, 166, 77, 1, 5, 21, 44, 0, 24, 31, 98, 70, 255, 142, 176, 163, 73, 171, 238, 152, 73, 6, 231, 43, 21, 43, 208}
	testAggregateSignature        = [96]byte{143, 172, 243, 17, 193, 221, 80, 173, 8, 79, 172, 91, 120, 155, 157, 231, 93, 167, 231, 76, 254, 78, 150, 37, 11, 218, 190, 223, 254, 10, 170, 173, 19, 187, 239, 124, 239, 166, 165, 5, 231, 118, 73, 255, 35, 0, 199, 117, 25, 16, 139, 170, 245, 184, 121, 123, 62, 158, 104, 17, 73, 148, 19, 142, 104, 187, 190, 170, 81, 40, 125, 236, 170, 229, 30, 73, 29, 246, 164, 71, 187, 187, 243, 158, 117, 113, 19, 115, 183, 17, 160, 122, 107, 178, 148, 234}
	testAggregateSignature2       = [96]byte{143, 217, 108, 250, 104, 199, 61, 129, 225, 98, 235, 184, 162, 63, 91, 143, 246, 188, 198, 202, 238, 61, 22, 85, 86, 204, 20, 249, 234, 225, 93, 145, 158, 78, 166, 15, 104, 200, 107, 89, 105, 6, 146, 119, 217, 165, 191, 130, 5, 97, 225, 161, 37, 248, 168, 113, 210, 126, 240, 76, 213, 10, 103, 23, 44, 200, 50, 203, 125, 5, 146, 228, 79, 27, 129, 81, 145, 177, 209, 136, 47, 69, 204, 74, 182, 41, 214, 40, 115, 119, 42, 79, 253, 193, 183, 12}
	testInvalidAggregateSignature = [96]byte{181, 40, 22, 125, 119, 1, 41, 208, 202, 183, 95, 118, 144, 56, 50, 25, 47, 147, 220, 48, 176, 153, 162, 95, 152, 210, 235, 128, 201, 54, 132, 76, 69, 135, 110, 120, 18, 118, 135, 102, 84, 150, 96, 42, 244, 96, 88, 101, 11, 226, 127, 10, 4, 85, 253, 90, 171, 230, 21, 228, 6, 65, 34, 91, 80, 145, 84, 224, 205, 97, 174, 92, 151, 220, 132, 28, 60, 129, 189, 173, 94, 237, 12, 65, 43, 142, 43, 67, 118, 121, 197, 134, 49, 65, 231, 155}
)

func getSuccessfulProposerSlashing() *cltypes.ProposerSlashing {
	return &cltypes.ProposerSlashing{
		Header1: &cltypes.SignedBeaconBlockHeader{
			Header: &cltypes.BeaconBlockHeader{
				Slot:          42,
				ProposerIndex: propInd,
				Root:          libcommon.BytesToHash([]byte("root1")),
			},
			Signature: testSignature1Slashing,
		},
		Header2: &cltypes.SignedBeaconBlockHeader{
			Header: &cltypes.BeaconBlockHeader{
				Slot:          42,
				ProposerIndex: propInd,
				Root:          libcommon.BytesToHash([]byte("root2")),
			},
			Signature: testSignature2Slashing,
		},
	}
}

// Surround vote.
func getSuccessfulAttesterSlashing() *cltypes.AttesterSlashing {
	return &cltypes.AttesterSlashing{
		Attestation_1: &cltypes.IndexedAttestation{
			AttestingIndices: []uint64{0, 1},
			Signature:        testAggregateSignature,
			Data: solid.NewAttestionDataFromParameters(0, 0, libcommon.Hash{},
				solid.NewCheckpointFromParameters(libcommon.Hash{}, 1),
				solid.NewCheckpointFromParameters(libcommon.Hash{}, 5)),
		},
		Attestation_2: &cltypes.IndexedAttestation{
			AttestingIndices: []uint64{0, 1},
			Signature:        testAggregateSignature2,
			Data: solid.NewAttestionDataFromParameters(0, 0, libcommon.Hash{},
				solid.NewCheckpointFromParameters(libcommon.Hash{}, 2),
				solid.NewCheckpointFromParameters(libcommon.Hash{}, 4)),
		},
	}
}

func TestProcessProposerSlashing(t *testing.T) {
	unchangingState := getTestState(t)
	v := &cltypes.Validator{}
	v.SetSlashed(false)
	v.SetActivationEpoch(0)
	v.SetWithdrawableEpoch(10000)
	v.SetPublicKey(testPublicKeySlashing)

	vc := &cltypes.Validator{}
	v.CopyTo(vc)
	unchangingState.SetValidatorAtIndex(propInd, vc)
	successState := getTestState(t)

	vc = &cltypes.Validator{}
	v.CopyTo(vc)
	successState.SetValidatorAtIndex(propInd, vc)
	successBalances := []uint64{}
	successState.ForEachValidator(func(v *cltypes.Validator, i, total int) bool {
		successBalances = append(successBalances, uint64(i+1))
		return true
	})
	successState.SetBalances(successBalances)

	successSlashing := getSuccessfulProposerSlashing()

	failureSlashingSlots := getSuccessfulProposerSlashing()
	failureSlashingSlots.Header1.Header.Slot += 1

	failureSlashingPIs := getSuccessfulProposerSlashing()
	failureSlashingPIs.Header1.Header.ProposerIndex += 1

	failureSlashingRoots := getSuccessfulProposerSlashing()
	failureSlashingRoots.Header1.Header.Root = failureSlashingRoots.Header2.Header.Root

	failureSlashingSlashable := getSuccessfulProposerSlashing()
	failureSlashingSlashable.Header1.Header.ProposerIndex = 0
	failureSlashingSlashable.Header2.Header.ProposerIndex = 0

	failureSlashingErrorSig := getSuccessfulProposerSlashing()
	failureSlashingErrorSig.Header1.Signature = [96]byte{}

	failureSlashingInvalidSig := getSuccessfulProposerSlashing()
	failureSlashingInvalidSig.Header1.Signature = testInvalidSignatureSlashing

	testCases := []struct {
		description string
		state       *state2.BeaconState
		slashing    *cltypes.ProposerSlashing
		wantErr     bool
	}{
		{
			description: "success",
			state:       successState,
			slashing:    successSlashing,
			wantErr:     false,
		},
		{
			description: "failure_slots_dont_match",
			state:       successState,
			slashing:    failureSlashingSlots,
			wantErr:     true,
		},
		{
			description: "failure_slots_prop_inds_dont_match",
			state:       successState,
			slashing:    failureSlashingPIs,
			wantErr:     true,
		},
		{
			description: "failure_slots_differ_root",
			state:       successState,
			slashing:    failureSlashingRoots,
			wantErr:     true,
		},
		{
			description: "failure_not_slashable",
			state:       successState,
			slashing:    failureSlashingSlashable,
			wantErr:     true,
		},
		{
			description: "failure_error_sig",
			state:       unchangingState,
			slashing:    failureSlashingErrorSig,
			wantErr:     true,
		},
		{
			description: "failure_invalid_sig",
			state:       unchangingState,
			slashing:    failureSlashingInvalidSig,
			wantErr:     true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			err := ProcessProposerSlashing(tc.state, tc.slashing)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("unexpected success, want error")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error, wanted success: %v", err)
			}
		})
	}
}

func TestProcessAttesterSlashing(t *testing.T) {
	unchangingState := getTestState(t)
	v1 := &cltypes.Validator{}
	v1.SetSlashed(false)
	v1.SetActivationEpoch(0)
	v1.SetWithdrawableEpoch(10000)
	v1.SetPublicKey(testPublicKeySlashing)
	v1c := &cltypes.Validator{}
	v1.CopyTo(v1c)

	v2 := &cltypes.Validator{}
	v2.SetSlashed(false)
	v2.SetActivationEpoch(0)
	v2.SetWithdrawableEpoch(10000)
	v2.SetPublicKey(testPublicKey2Slashing)
	v2c := &cltypes.Validator{}
	v2.CopyTo(v2c)

	unchangingState.SetValidatorAtIndex(0, v1)
	unchangingState.SetValidatorAtIndex(1, v2)

	successState := getTestState(t)
	successState.SetValidatorAtIndex(0, v1c)
	successState.SetValidatorAtIndex(1, v2c)
	successBalances := []uint64{}
	successState.ForEachValidator(func(v *cltypes.Validator, i, total int) bool {
		successBalances = append(successBalances, uint64(i+1))
		return true
	})
	successState.SetBalances(successBalances)

	successSlashing := getSuccessfulAttesterSlashing()

	failureSlashingNotSlashable := getSuccessfulAttesterSlashing()
	failureSlashingNotSlashable.Attestation_1 = failureSlashingNotSlashable.Attestation_2

	failureSlashingBadSigAtt1 := getSuccessfulAttesterSlashing()
	failureSlashingBadSigAtt1.Attestation_1.Signature = testInvalidAggregateSignature

	failureSlashingErrorSigAtt1 := getSuccessfulAttesterSlashing()
	failureSlashingErrorSigAtt1.Attestation_1.Signature = [96]byte{}

	failureSlashingBadSigAtt2 := getSuccessfulAttesterSlashing()
	failureSlashingBadSigAtt2.Attestation_2.Signature = testInvalidAggregateSignature

	failureSlashingErrorSigAtt2 := getSuccessfulAttesterSlashing()
	failureSlashingErrorSigAtt2.Attestation_2.Signature = [96]byte{}

	testCases := []struct {
		description string
		state       *state2.BeaconState
		slashing    *cltypes.AttesterSlashing
		wantErr     bool
	}{
		{
			description: "success",
			state:       successState,
			slashing:    successSlashing,
			wantErr:     false,
		},
		{
			description: "failure_not_slashable_attestation",
			state:       unchangingState,
			slashing:    failureSlashingNotSlashable,
			wantErr:     true,
		},
		{
			description: "failure_invalid_attestation1_signature",
			state:       unchangingState,
			slashing:    failureSlashingBadSigAtt1,
			wantErr:     true,
		},
		{
			description: "failure_error_attestation1_signature",
			state:       unchangingState,
			slashing:    failureSlashingErrorSigAtt1,
			wantErr:     true,
		},
		{
			description: "failure_invalid_attestation2_signature",
			state:       unchangingState,
			slashing:    failureSlashingBadSigAtt2,
			wantErr:     true,
		},
		{
			description: "failure_error_attestation2_signature",
			state:       unchangingState,
			slashing:    failureSlashingErrorSigAtt2,
			wantErr:     true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			err := ProcessAttesterSlashing(tc.state, tc.slashing)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("unexpected success, want error")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error, wanted success: %v", err)
			}
		})
	}
}

func makeBytes48FromHex(s string) (ret [48]byte) {
	bytesString := common.Hex2Bytes(s)
	copy(ret[:], bytesString)
	return
}

func makeBytes96FromHex(s string) (ret [96]byte) {
	bytesString := common.Hex2Bytes(s)
	copy(ret[:], bytesString)
	return
}

func TestProcessDeposit(t *testing.T) {
	eth1Data := &cltypes.Eth1Data{
		Root:         libcommon.HexToHash("efe26a8b16ed08bbb7989418a34e6f2073bc68ce4cc88176909a850f3b47099e"),
		BlockHash:    libcommon.HexToHash("efe26a8b16ed08bbb7989418a34e6f2073bc68ce4cc88176909a850f3b47099e"),
		DepositCount: 1,
	}
	deposit := &cltypes.Deposit{
		Data: &cltypes.DepositData{
			PubKey:                makeBytes48FromHex("a99a76ed7796f7be22d5b7e85deeb7c5677e88e511e0b337618f8c4eb61349b4bf2d153f649f7b53359fe8b94a38e44c"),
			Signature:             makeBytes96FromHex("953b44ee497f9fc9abbc1340212597c264b77f3dea441921d65b2542d64195171ba0598fad34905f03c0c1b6d5540faa10bb2c26084fc5eacbafba119d9a81721f56821cae7044a2ff374e9a128f68dee68d3b48406ea60306148498ffe007c7"),
			Amount:                32000000000,
			WithdrawalCredentials: libcommon.HexToHash("00ec7ef7780c9d151597924036262dd28dc60e1228f4da6fecf9d402cb3f3594"),
		},
	}
	testState := state2.GetEmptyBeaconState()
	v := &cltypes.Validator{}
	v.SetPublicKey([48]byte{1})
	v.SetWithdrawalCredentials([32]byte{1, 2, 3})
	testState.AddValidator(v, 0)
	testState.SetEth1Data(eth1Data)
	require.NoError(t, ProcessDeposit(testState, deposit, false))
	bal, err := testState.ValidatorBalance(1)
	require.NoError(t, err)
	require.Equal(t, deposit.Data.Amount, bal)
}

func TestProcessVoluntaryExits(t *testing.T) {
	state := state2.GetEmptyBeaconState()
	exit := &cltypes.SignedVoluntaryExit{
		VolunaryExit: &cltypes.VoluntaryExit{
			ValidatorIndex: 0,
			Epoch:          0,
		},
	}
	v := &cltypes.Validator{}
	v.SetExitEpoch(clparams.MainnetBeaconConfig.FarFutureEpoch)
	v.SetActivationEpoch(0)
	state.AddValidator(v, 0)
	state.SetSlot((clparams.MainnetBeaconConfig.SlotsPerEpoch * 5) + (clparams.MainnetBeaconConfig.SlotsPerEpoch * clparams.MainnetBeaconConfig.ShardCommitteePeriod))

	require.NoError(t, ProcessVoluntaryExit(state, exit, false), "Could not process exits")
	newRegistry, err := state.ValidatorForValidatorIndex(0)
	require.NoError(t, err)
	require.Equal(t, newRegistry.ExitEpoch(), uint64(266))
}
