// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package caplin1

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/builder"
	"github.com/erigontech/erigon/cl/clparams"
)

func TestWithBuilderSupportsDynamicValidatorAPIWithoutRelay(t *testing.T) {
	var got option
	WithBuilder("", &clparams.MainnetBeaconConfig, builder.BuilderTargetPolicy{})(&got)
	require.NotNil(t, got.builderClient)
}

func TestBuilderTargetPolicyForConfig(t *testing.T) {
	require.False(t, builderTargetPolicyForConfig(nil).AllowPrivate)
	require.False(t, builderTargetPolicyForConfig(&clparams.CaplinConfig{}).AllowPrivate)
	require.False(t, builderTargetPolicyForConfig(&clparams.CaplinConfig{CustomConfigPath: "config.yaml"}).AllowPrivate)
	require.True(t, builderTargetPolicyForConfig(&clparams.CaplinConfig{AllowPrivateBuilderURLs: true}).AllowPrivate)
}

func TestBuilderOptionKeepsDynamicAndLegacyGatingIndependent(t *testing.T) {
	t.Run("validator API without relay", func(t *testing.T) {
		config := &clparams.CaplinConfig{}
		config.BeaconAPIRouter.Validator = true
		builderOption, skippedLegacy := builderOptionForConfig(config, &clparams.MainnetBeaconConfig)
		require.NotNil(t, builderOption)
		require.False(t, skippedLegacy)
		var got option
		builderOption(&got)
		require.NotNil(t, got.builderClient)
	})

	t.Run("legacy builder API without relay", func(t *testing.T) {
		config := &clparams.CaplinConfig{}
		config.BeaconAPIRouter.Builder = true
		builderOption, skippedLegacy := builderOptionForConfig(config, &clparams.MainnetBeaconConfig)
		require.Nil(t, builderOption)
		require.True(t, skippedLegacy)
		require.False(t, config.BeaconAPIRouter.Builder)
	})
}
