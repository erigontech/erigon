package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/params"
	erigonapp "github.com/ledgerwatch/erigon/turbo/app"
	erigoncli "github.com/ledgerwatch/erigon/turbo/cli"
	"github.com/ledgerwatch/erigon/turbo/node"
	"github.com/ledgerwatch/log/v3"
	"github.com/pelletier/go-toml"
	"github.com/urfave/cli"
	"gopkg.in/yaml.v2"
)

func main() {
	defer func() {
		panicResult := recover()
		if panicResult == nil {
			return
		}

		log.Error("catch panic", "err", panicResult, "stack", dbg.Stack())
		os.Exit(1)
	}()

	app := erigonapp.MakeApp(runErigon, erigoncli.DefaultFlags)
	if err := app.Run(os.Args); err != nil {
		_, printErr := fmt.Fprintln(os.Stderr, err)
		if printErr != nil {
			log.Warn("Fprintln error", "err", printErr)
		}
		os.Exit(1)
	}
}

func runErigon(cliCtx *cli.Context) {
	logger := log.New()
	// initializing the node and providing the current git commit there
	logger.Info("Build info", "git_branch", params.GitBranch, "git_tag", params.GitTag, "git_commit", params.GitCommit)

	configFilePath := cliCtx.GlobalString(utils.ConfigFlag.Name)
	if configFilePath != "" {
		if err := setFlagsFromConfigFile(cliCtx, configFilePath); err != nil {
			log.Warn("failed setting config flags from yaml/toml file", "err", err)
		}
	}

	nodeCfg := node.NewNodConfigUrfave(cliCtx)
	ethCfg := node.NewEthConfigUrfave(cliCtx, nodeCfg)

	ethNode, err := node.New(nodeCfg, ethCfg, logger)
	if err != nil {
		log.Error("Erigon startup", "err", err)
		return
	}
	err = ethNode.Serve()
	if err != nil {
		log.Error("error while serving an Erigon node", "err", err)
	}
}

func setFlagsFromConfigFile(ctx *cli.Context, filePath string) error {
	fileExtension := filepath.Ext(filePath)

	fileConfig := make(map[string]string)

	if fileExtension == ".yaml" {
		yamlFile, err := ioutil.ReadFile(filePath)
		if err != nil {
			return err
		}
		err = yaml.Unmarshal(yamlFile, fileConfig)
		if err != nil {
			return err
		}
	} else if fileExtension == ".toml" {
		tomlFile, err := ioutil.ReadFile(filePath)
		if err != nil {
			return err
		}
		err = toml.Unmarshal(tomlFile, &fileConfig)
		if err != nil {
			return err
		}
	} else {
		return errors.New("config files only accepted are .yaml and .toml")
	}

	// sets global flags to value in yaml/toml file
	for key, value := range fileConfig {
		if !ctx.GlobalIsSet(key) {
			err := ctx.GlobalSet(key, value)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
