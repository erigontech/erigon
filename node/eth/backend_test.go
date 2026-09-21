package eth

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/erigontech/erigon/node/ethconfig"
)

func TestValidateEmbeddedBuilderMode(t *testing.T) {
	for _, test := range []struct {
		name       string
		enabled    bool
		internalCL bool
		networkID  uint64
		wantError  bool
	}{
		{name: "disabled without embedded Caplin"},
		{name: "enabled with embedded Caplin", enabled: true, internalCL: true, networkID: 1},
		{name: "enabled without embedded Caplin", enabled: true, wantError: true},
		{name: "enabled on unsupported network", enabled: true, internalCL: true, networkID: 999, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := ethconfig.Config{InternalCL: test.internalCL, NetworkID: test.networkID}
			cfg.CaplinConfig.EpbsBuilder.Enabled = test.enabled
			err := validateEmbeddedBuilderMode(&cfg)
			if test.wantError {
				require.ErrorContains(t, err, "embedded Caplin")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateEmbeddedBuilderRPCExposure(t *testing.T) {
	for _, test := range []struct {
		name      string
		enabled   bool
		api       []string
		configure func(*httpcfg.HttpCfg)
		wantError bool
	}{
		{name: "RPC globally disabled", enabled: true, api: []string{"builder"}, configure: func(cfg *httpcfg.HttpCfg) {
			cfg.Enabled = false
			cfg.HttpServerEnabled = true
			cfg.HttpListenAddress = "0.0.0.0"
		}},
		{name: "builder disabled", api: []string{"builder"}, configure: func(cfg *httpcfg.HttpCfg) { cfg.HttpServerEnabled = true; cfg.HttpListenAddress = "0.0.0.0" }},
		{name: "builder namespace disabled", enabled: true, api: []string{"eth"}, configure: func(cfg *httpcfg.HttpCfg) { cfg.HttpServerEnabled = true; cfg.HttpListenAddress = "0.0.0.0" }},
		{name: "loopback IPv4", enabled: true, api: []string{"builder"}, configure: func(cfg *httpcfg.HttpCfg) { cfg.HttpServerEnabled = true; cfg.HttpListenAddress = "127.0.0.1" }},
		{name: "loopback IPv6", enabled: true, api: []string{"builder"}, configure: func(cfg *httpcfg.HttpCfg) { cfg.HttpServerEnabled = true; cfg.HttpListenAddress = "::1" }},
		{name: "localhost", enabled: true, api: []string{"builder"}, configure: func(cfg *httpcfg.HttpCfg) { cfg.HttpServerEnabled = true; cfg.HttpListenAddress = "localhost" }},
		{name: "localhost case insensitive", enabled: true, api: []string{"builder"}, configure: func(cfg *httpcfg.HttpCfg) { cfg.HttpServerEnabled = true; cfg.HttpListenAddress = "LOCALHOST" }},
		{name: "unix override", enabled: true, api: []string{"builder"}, configure: func(cfg *httpcfg.HttpCfg) {
			cfg.HttpServerEnabled = true
			cfg.HttpURL = "unix:///tmp/erigon-builder.sock"
		}},
		{name: "wildcard HTTP", enabled: true, api: []string{"builder"}, configure: func(cfg *httpcfg.HttpCfg) { cfg.HttpServerEnabled = true; cfg.HttpListenAddress = "0.0.0.0" }, wantError: true},
		{name: "public HTTPS", enabled: true, api: []string{"builder"}, configure: func(cfg *httpcfg.HttpCfg) { cfg.HttpsServerEnabled = true; cfg.HttpsListenAddress = "192.0.2.1" }, wantError: true},
		{name: "HTTPS URL enables listener", enabled: true, api: []string{"builder"}, configure: func(cfg *httpcfg.HttpCfg) { cfg.HttpsURL = "tcp://192.0.2.1:8546" }, wantError: true},
		{name: "wildcard websocket", enabled: true, api: []string{"builder"}, configure: func(cfg *httpcfg.HttpCfg) { cfg.WebsocketEnabled = true; cfg.HttpListenAddress = "0.0.0.0" }, wantError: true},
		{name: "separate websocket ignores HTTP Unix override", enabled: true, api: []string{"builder"}, configure: func(cfg *httpcfg.HttpCfg) {
			cfg.HttpServerEnabled = true
			cfg.HttpURL = "unix:///tmp/erigon-builder.sock"
			cfg.HttpPort = 8545
			cfg.WebsocketEnabled = true
			cfg.WebsocketPort = 8546
			cfg.HttpListenAddress = "0.0.0.0"
		}, wantError: true},
		{name: "remote TCP override", enabled: true, api: []string{"builder"}, configure: func(cfg *httpcfg.HttpCfg) { cfg.HttpServerEnabled = true; cfg.HttpURL = "tcp://0.0.0.0:8545" }, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			builderCfg := ethconfig.Config{}
			builderCfg.CaplinConfig.EpbsBuilder.Enabled = test.enabled
			httpCfg := httpcfg.HttpCfg{Enabled: true, API: test.api}
			test.configure(&httpCfg)
			err := validateEmbeddedBuilderRPCExposure(&builderCfg, &httpCfg)
			if test.wantError {
				require.ErrorContains(t, err, "loopback")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRemoveContents(t *testing.T) {
	tmpDirName := t.TempDir()
	//t.Logf("creating %s/root...", rootName)
	rootName := filepath.Join(tmpDirName, "root")
	err := os.Mkdir(rootName, 0750)
	require.NoError(t, err)
	//fmt.Println("OK")
	for i := range 3 {
		outerName := filepath.Join(rootName, fmt.Sprintf("outer_%d", i+1))
		//t.Logf("creating %s... ", outerName)
		err = os.Mkdir(outerName, 0750)
		require.NoError(t, err)
		//t.Logf("OK")
		for j := range 2 {
			innerName := filepath.Join(outerName, fmt.Sprintf("inner_%d", j+1))
			//t.Logf("creating %s... ", innerName)
			err = os.Mkdir(innerName, 0750)
			require.NoError(t, err)
			//t.Log("OK")
			for k := range 2 {
				innestName := filepath.Join(innerName, fmt.Sprintf("innest_%d", k+1))
				//t.Logf("creating %s... ", innestName)
				err = os.Mkdir(innestName, 0750)
				require.NoError(t, err)
				//t.Log("OK")
			}
		}
	}
	list, err := os.ReadDir(rootName)
	require.NoError(t, err)

	require.Len(t, list, 3)

	err = RemoveContents(rootName)
	require.NoError(t, err)

	list, err = os.ReadDir(rootName)
	require.NoError(t, err)

	require.Empty(t, list)
}
