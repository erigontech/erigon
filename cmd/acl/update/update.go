package update

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cmd/snapshots/sync"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/zk/txpool"
	"github.com/urfave/cli/v2"
)

var (
	csvFile string
	aclType string

	address string
	policy  string
)

var UpdateCommand = cli.Command{
	Action: updateRun,
	Name:   "update",
	Usage:  "Update the ACL",
	Flags: []cli.Flag{
		&utils.DataDirFlag,
		&cli.StringFlag{
			Name:        "csv",
			Usage:       "CSV file with the ACL",
			DefaultText: "",
			Destination: &csvFile,
			Required:    true,
		},
		&cli.StringFlag{
			Name:        "type",
			Usage:       "Type of the ACL (allowlist or blocklist)",
			DefaultText: "allowlist",
			Destination: &aclType,
			Required:    true,
		},
	},
}

var RemoveCommand = cli.Command{
	Action: removeRun,
	Name:   "remove",
	Usage:  "Remove the ACL policy",
	Flags: []cli.Flag{
		&utils.DataDirFlag,
		&cli.StringFlag{
			Name:        "address",
			Usage:       "Address of the account to remove the policy",
			Required:    true,
			Destination: &address,
		},
		&cli.StringFlag{
			Name:        "policy",
			Usage:       "Policy to remove",
			Required:    true,
			Destination: &policy,
		},
		&cli.StringFlag{
			Name:        "type",
			Usage:       "Type of the ACL (allowlist or blocklist)",
			DefaultText: "allowlist",
			Destination: &aclType,
			Required:    true,
		},
	},
}

var AddCommand = cli.Command{
	Action: addRun,
	Name:   "add",
	Usage:  "Add the ACL policy",
	Flags: []cli.Flag{
		&utils.DataDirFlag,
		&cli.StringFlag{
			Name:        "address",
			Usage:       "Address of the account to add the policy",
			Required:    true,
			Destination: &address,
		},
		&cli.StringFlag{
			Name:        "policy",
			Usage:       "Policy to add",
			Required:    true,
			Destination: &policy,
		},
		&cli.StringFlag{
			Name:        "type",
			Usage:       "Type of the ACL (allowlist or blocklist)",
			DefaultText: "allowlist",
			Destination: &aclType,
			Required:    true,
		},
	},
}

// addRun is the entry point for the add command that adds the ACL policy for the given address
func addRun(cliCtx *cli.Context) error {
	logger := sync.Logger(cliCtx.Context)

	if !cliCtx.IsSet(utils.DataDirFlag.Name) {
		return errors.New("data directory is not set")
	}

	dataDir := cliCtx.String(utils.DataDirFlag.Name)

	logger.Info("Adding ACL policy", "dataDir", dataDir, "address", address, "policy", policy)

	aclDB, err := txpool.OpenACLDB(cliCtx.Context, dataDir)
	if err != nil {
		logger.Error("Failed to open ACL database", "err", err)
		return err
	}

	addr := common.HexToAddress(address)
	policy, err := txpool.ResolvePolicy(policy)
	if err != nil {
		logger.Error("Failed to resolve policy", "err", err)
		return err
	}

	if err := txpool.AddPolicy(cliCtx.Context, aclDB, aclType, addr, policy); err != nil {
		logger.Error("Failed to add policy", "err", err)
		return err
	}

	logger.Info("Policy added", "address", address, "policy", policy)

	return nil
}

// removeRun is the entry point for the remove command that removes the ACL policy for the given address
func removeRun(cliCtx *cli.Context) error {
	logger := sync.Logger(cliCtx.Context)

	if !cliCtx.IsSet(utils.DataDirFlag.Name) {
		return errors.New("data directory is not set")
	}

	dataDir := cliCtx.String(utils.DataDirFlag.Name)

	logger.Info("Removing ACL policy", "dataDir", dataDir, "address", address, "policy", policy)

	aclDB, err := txpool.OpenACLDB(cliCtx.Context, dataDir)
	if err != nil {
		logger.Error("Failed to open ACL database", "err", err)
		return err
	}

	addr := common.HexToAddress(address)
	policy, err := txpool.ResolvePolicy(policy)
	if err != nil {
		logger.Error("Failed to resolve policy", "err", err)
		return err
	}

	if err := txpool.RemovePolicy(cliCtx.Context, aclDB, aclType, addr, policy); err != nil {
		logger.Error("Failed to remove policy", "err", err)
		return err
	}

	logger.Info("Policy removed", "address", address, "policy", policy)

	return nil
}

// updateRun is the entry point for the update command that updates the ACL based on the given CSV file
func updateRun(cliCtx *cli.Context) error {
	logger := sync.Logger(cliCtx.Context)

	if !cliCtx.IsSet(utils.DataDirFlag.Name) {
		return errors.New("data directory is not set")
	}

	dataDir := cliCtx.String(utils.DataDirFlag.Name)

	logger.Info("Updating ACL", "dataDir", dataDir)

	aclDB, err := txpool.OpenACLDB(cliCtx.Context, dataDir)
	if err != nil {
		logger.Error("Failed to open ACL database", "err", err)
		return err
	}

	addresses, policies, err := readCSV(csvFile)
	if err != nil {
		logger.Error("Failed to read CSV file", "err", err)
		return err
	}

	if err := txpool.UpdatePolicies(cliCtx.Context, aclDB, aclType, addresses, policies); err != nil {
		logger.Error("Failed to update policies", "err", err)
		return err
	}

	return nil
}

// readCSV reads the CSV file and returns the addresses and policies
func readCSV(filePath string) ([]common.Address, [][]txpool.Policy, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	var (
		addresses []common.Address
		policies  [][]txpool.Policy
		row       int
	)

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read record: %w", err)
		}

		if len(record) != 2 {
			return nil, nil, errors.New("invalid record")
		}

		addresses = append(addresses, common.HexToAddress(record[0]))

		addressPolicies := make([]txpool.Policy, 0, len(record[1]))

		stringPolicies := strings.Split(strings.TrimSpace(record[1]), ",")
		for _, pc := range stringPolicies {
			policy, err := txpool.ResolvePolicy(pc)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to resolve policy: %w in row: %d", err, row)
			}

			addressPolicies = append(addressPolicies, policy)
		}

		policies = append(policies, addressPolicies)

		row++
	}

	return addresses, policies, nil
}
