package db

import (
	"encoding/json"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cmd/diag/flags"
	"github.com/ledgerwatch/erigon/cmd/diag/util"
	"github.com/urfave/cli/v2"
)

type DBInfo struct {
	name   string
	tables []BDTableInfo
	count  int
	size   string
}

type BDTableInfo struct {
	Name  string
	Count int
	Size  uint64
}

var Command = cli.Command{
	Name:      "databases",
	Aliases:   []string{"dbs"},
	ArgsUsage: "",
	Subcommands: []*cli.Command{
		{
			Name:      "all",
			Aliases:   []string{"a"},
			Action:    printAllDBsInfo,
			Usage:     "Print database tables info.",
			ArgsUsage: "",
			Flags: []cli.Flag{
				&flags.DebugURLFlag,
				&flags.OutputFlag,
			},
		},
		{
			Name:      "populated",
			Aliases:   []string{"pop"},
			Action:    printPopuplatedDBsInfo,
			Usage:     "Print database tables info which is not empty.",
			ArgsUsage: "",
			Flags: []cli.Flag{
				&flags.DebugURLFlag,
				&flags.OutputFlag,
			},
		},
	},
	Description: ``,
}

func printAllDBsInfo(cliCtx *cli.Context) error {
	data, err := AllDBsInfo(cliCtx)
	if err != nil {
		return err
	}

	switch cliCtx.String(flags.OutputFlag.Name) {
	case "json":
		bytes, err := json.Marshal(data)
		if err != nil {
			return err
		}

		fmt.Println(string(bytes))

	case "text":
		printDBsInfo(data)
	}

	return nil
}

func printPopuplatedDBsInfo(cliCtx *cli.Context) error {
	data, err := AllDBsInfo(cliCtx)
	if err != nil {
		return err
	}

	// filter out empty tables
	for i := 0; i < len(data); i++ {
		tables := data[i].tables
		for j := 0; j < len(tables); j++ {
			if tables[j].Count == 0 {
				tables = append(tables[:j], tables[j+1:]...)
				j--
			}
		}
		data[i].tables = tables
	}

	//filter out empty dbs
	for i := 0; i < len(data); i++ {
		if len(data[i].tables) == 0 {
			data = append(data[:i], data[i+1:]...)
			i--
		}
	}

	switch cliCtx.String(flags.OutputFlag.Name) {
	case "json":
		bytes, err := json.Marshal(data)
		if err != nil {
			return err
		}

		fmt.Println(string(bytes))

	case "text":
		printDBsInfo(data)
	}

	return nil
}

func printDBsInfo(data []DBInfo) {
	fmt.Print("\n")
	fmt.Println("------------------------DBs-------------------")
	fmt.Println("DB Name             Keys Count            Size")
	fmt.Println("----------------------------------------------")
	for _, db := range data {
		nLen := len(db.name)
		fmt.Print(db.name)
		for i := 0; i < 20-nLen; i++ {
			fmt.Print(" ")
		}

		fmt.Printf("%d", db.count)
		for i := 0; i < 22-len(fmt.Sprint(db.count)); i++ {
			fmt.Print(" ")
		}

		fmt.Printf("%s\n", db.size)
	}

	fmt.Print("\n")
	fmt.Print("\n")

	//db := data[0]
	for _, db := range data {

		nl := len(db.name)

		dashCount := (60 - nl) / 2

		for i := 0; i < dashCount; i++ {
			fmt.Print("-")
		}

		//fmt.Printf(" %s ", db.name)
		fmt.Print("\033[1m " + db.name + " \033[0m")

		for i := 0; i < dashCount; i++ {
			fmt.Print("-")
		}
		fmt.Print("\n")

		//fmt.Println("------------------------------------------------------------")
		fmt.Println("Table Name                        Keys Count            Size")
		for i := 0; i < 60; i++ {
			fmt.Print("-")
		}
		fmt.Print("\n")

		for _, table := range db.tables {
			nLen := len(table.Name)
			fmt.Printf("%s", table.Name)
			for i := 0; i < 34-nLen; i++ {
				fmt.Print(" ")
			}

			fmt.Printf("%d", table.Count)
			for i := 0; i < 22-len(fmt.Sprint(table.Count)); i++ {
				fmt.Print(" ")
			}

			fmt.Printf("%s\n", common.ByteCount(table.Size))
		}
		fmt.Print("\n")
	}
}

func AllDBsInfo(cliCtx *cli.Context) ([]DBInfo, error) {
	data := make([]DBInfo, 0)

	dbsNames, err := getAllDbsNames(cliCtx)
	if err != nil {
		return data, err
	}

	for _, dbName := range dbsNames {
		tables, err := getDb(cliCtx, dbName)
		if err != nil {
			continue
		}

		tCount := 0
		tSize := uint64(0)
		for _, table := range tables {
			tCount += table.Count
			tSize += table.Size
		}

		dbInfo := DBInfo{
			name:   dbName,
			tables: tables,
			count:  tCount,
			size:   common.ByteCount(tSize),
		}
		data = append(data, dbInfo)
	}

	return data, nil
}

func getAllDbsNames(cliCtx *cli.Context) ([]string, error) {
	var data []string
	url := "http://" + cliCtx.String(flags.DebugURLFlag.Name) + "/debug/dbs"

	err := util.MakeHttpGetCall(cliCtx.Context, url, &data)
	if err != nil {
		return data, err
	}

	return data, nil
}

func getDb(cliCtx *cli.Context, dbName string) ([]BDTableInfo, error) {
	var data []BDTableInfo
	url := "http://" + cliCtx.String(flags.DebugURLFlag.Name) + "/debug/dbs/" + dbName + "/tables"

	err := util.MakeHttpGetCall(cliCtx.Context, url, &data)
	if err != nil {
		return data, err
	}

	return data, nil
}
