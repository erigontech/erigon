package cmd

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"github.com/ledgerwatch/erigon/cmd/starknet/services"
	"github.com/spf13/cobra"
	"os"
	"strings"
)

type Flags struct {
	Contract   string
	Salt       string
	PrivateKey string
	Datadir    string
	Output     string
}

var generateRawTxCmd = &cobra.Command{
	Use:   "generateRawTx",
	Short: "Generate data for starknet_sendRawTransaction RPC method",
}

func init() {
	flags := Flags{}

	generateRawTxCmd.Flags().StringVarP(&flags.Contract, "contract", "c", "", "Path to compiled cairo contract in JSON format")
	generateRawTxCmd.MarkFlagRequired("contract")

	generateRawTxCmd.Flags().StringVarP(&flags.Salt, "salt", "s", "", "Cairo contract address salt")
	generateRawTxCmd.MarkFlagRequired("salt")

	generateRawTxCmd.Flags().StringVarP(&flags.PrivateKey, "private_key", "k", "", "Private key")
	generateRawTxCmd.MarkFlagRequired("private_key")

	rootCmd.PersistentFlags().StringVar(&flags.Datadir, "datadir", "", "path to Erigon working directory")

	generateRawTxCmd.Flags().StringVarP(&flags.Output, "output", "o", "", "Path to file where sign transaction will be saved. Print to stdout if empty.")

	generateRawTxCmd.RunE = func(cmd *cobra.Command, args []string) error {
		rawTxGenerator := services.NewRawTxGenerator(flags.PrivateKey)

		fs := os.DirFS("/")
		buf := bytes.NewBuffer(nil)
		err := rawTxGenerator.CreateFromFS(fs, strings.Trim(flags.Contract, "/"), []byte(flags.Salt), buf)
		if err != nil {
			return err
		}

		if flags.Output != "" {
			outputFile, err := os.Create(flags.Output)
			if err != nil {
				return fmt.Errorf("could not create output file: %v", flags.Output)
			}
			defer outputFile.Close()

			_, err = outputFile.WriteString(hex.EncodeToString(buf.Bytes()))
			if err != nil {
				return fmt.Errorf("could not write to output file: %v", flags.Output)
			}
		} else {
			fmt.Println(hex.EncodeToString(buf.Bytes()))
		}

		return err
	}

	rootCmd.AddCommand(generateRawTxCmd)
}
