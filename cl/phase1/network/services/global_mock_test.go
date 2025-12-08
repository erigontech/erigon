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
	"github.com/erigontech/erigon-lib/types/ssz"
	"github.com/erigontech/erigon/cl/cltypes"
	"go.uber.org/mock/gomock"
)

type mockFuncs struct {
	ctrl *gomock.Controller
}

func (m *mockFuncs) ComputeSigningRoot(obj ssz.HashableSSZ, domain []byte) ([32]byte, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ComputeSigningRoot", obj, domain)
	ret0, _ := ret[0].([32]byte)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (m *mockFuncs) BlsVerify(pubkey, message, signature []byte) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "BlsVerify", pubkey, message, signature)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (m *mockFuncs) BlsVerifyMultipleSignatures(pubkey, message, signature [][]byte) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "BlsVerifyMultipleSignatures", pubkey, message, signature)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (m *mockFuncs) VerifyDataColumnSidecarInclusionProof(sidecar *cltypes.DataColumnSidecar) bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "VerifyDataColumnSidecarInclusionProof", sidecar)
	ret0, _ := ret[0].(bool)
	return ret0
}

func (m *mockFuncs) VerifyDataColumnSidecarKZGProofs(sidecar *cltypes.DataColumnSidecar) bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "VerifyDataColumnSidecarKZGProofs", sidecar)
	ret0, _ := ret[0].(bool)
	return ret0
}

func (m *mockFuncs) VerifyDataColumnSidecar(sidecar *cltypes.DataColumnSidecar) bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "VerifyDataColumnSidecar", sidecar)
	ret0, _ := ret[0].(bool)
	return ret0
}
