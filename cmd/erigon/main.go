package main

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/params"
	erigonapp "github.com/ledgerwatch/erigon/turbo/app"
	erigoncli "github.com/ledgerwatch/erigon/turbo/cli"
	"github.com/ledgerwatch/erigon/turbo/node"
	"github.com/ledgerwatch/log/v3"
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
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runErigon(cliCtx *cli.Context) {
	logger := log.New()
	// initializing the node and providing the current git commit there
	logger.Info("Build info", "git_branch", params.GitBranch, "git_tag", params.GitTag, "git_commit", params.GitCommit)

	yamlFilePath := cliCtx.GlobalString(utils.ConfigFileFlag.Name)
	if yamlFilePath != "" {
		if err := setFlagsFromYamlFile(cliCtx, yamlFilePath); err != nil {
			log.Warn("failed setting config flags from yaml file", "err", err)
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

func setFlagsFromYamlFile(ctx *cli.Context, filePath string) error {
	yamlFile, err := ioutil.ReadFile(filePath)
	if err != nil {
		return err
	}
	yamlFileConfig := make(map[string]string)
	err = yaml.Unmarshal(yamlFile, yamlFileConfig)
	if err != nil {
		return err
	}

	// sets global flags to value in yaml file
	for key, value := range yamlFileConfig {
		if !ctx.GlobalIsSet(key) {
			err := ctx.GlobalSet(key, value)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
