// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package solid

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestListSSZUnmarshalJSONRejectsDecodeLimit(t *testing.T) {
	list := NewStaticProgressiveListSSZWithDecodeLimit[*DepositRequest](2, SizeDepositRequest)

	err := list.UnmarshalJSON([]byte(`[{}, {}, {}]`))

	require.Error(t, err)
	require.Zero(t, list.Len())
}

func TestListSSZUnmarshalJSONAcceptsDecodeLimit(t *testing.T) {
	list := NewStaticProgressiveListSSZWithDecodeLimit[*DepositRequest](2, SizeDepositRequest)

	err := list.UnmarshalJSON([]byte(`[{}, {}]`))

	require.NoError(t, err)
	require.Equal(t, 2, list.Len())
}

func TestListSSZUnmarshalJSONRejectsElementAtZeroLimit(t *testing.T) {
	list := NewStaticListSSZ[*DepositRequest](0, SizeDepositRequest)

	err := list.UnmarshalJSON([]byte(`[{}]`))

	require.Error(t, err)
	require.Zero(t, list.Len())
}

func TestListSSZUnmarshalJSONPreservesNullAsEmpty(t *testing.T) {
	list := NewStaticListSSZ[*DepositRequest](0, SizeDepositRequest)

	err := list.UnmarshalJSON([]byte(`null`))

	require.NoError(t, err)
	require.Zero(t, list.Len())
}
