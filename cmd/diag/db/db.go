package db

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/kataras/tablewriter"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cmd/diag/flags"
	"github.com/ledgerwatch/erigon/cmd/diag/util"
	"github.com/lensesio/tableprinter"
	"github.com/urfave/cli/v2"
)

type DBInfo struct {
	name   string        `header:"DB Name"`
	tables []BDTableInfo `header:"Tables"`
	count  int           `header:"Keys Count"`
	size   string        `header:"Size"`
}

type BDTableInfo struct {
	Name  string `header:"Table Name"`
	Count int    `header:"Keys Count"`
	Size  uint64 `header:"Size"`
}

var (
	DBPopulatedFlag = cli.BoolFlag{
		Name:     "db.appearence.populated",
		Aliases:  []string{"dbap"},
		Usage:    "Print populated table content only",
		Required: false,
		Value:    false,
	}

	DBNameFlag = cli.StringFlag{
		Name:     "db.name",
		Aliases:  []string{"dbn"},
		Usage:    "DB name",
		Required: false,
		Value:    "",
	}

	DBTableNameFlag = cli.StringFlag{
		Name:     "db.table.name",
		Aliases:  []string{"dbtn"},
		Usage:    "Table name",
		Required: false,
		Value:    "",
	}
)

var Command = cli.Command{
	Action:    printAllDBsInfo,
	Name:      "databases",
	Aliases:   []string{"dbs"},
	Usage:     "Print database tables info.",
	ArgsUsage: "",
	Flags: []cli.Flag{
		&flags.DebugURLFlag,
		&flags.OutputFlag,
		&DBPopulatedFlag,
		&DBTableNameFlag,
		&DBNameFlag,
	},
	Description: ``,
}

func printDBsInfo(cliCtx *cli.Context) error {
	data, err := DBsInfo(cliCtx)

	if cliCtx.String(flags.OutputFlag.DBTableNameFlag != "") {
		for _, db := range data {
			for _, table := range db.tables {
				if table.Name == cliCtx.String(flags.OutputFlag.DBTableNameFlag) {
					fmt.Println(table)
				}
			}
		}
		return nil
	}
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

type PrintableDBInfo struct {
	DBName    string `header:"DB Name"`
	KeysCount int    `header:"Keys Count"`
	Size      string `header:"Size"`
}

func printDBsInfo(data []DBInfo) {
	printer := tableprinter.New(os.Stdout)
	printer.BorderTop, printer.BorderBottom, printer.BorderLeft, printer.BorderRight = true, true, true, true
	printer.CenterSeparator = "│"
	printer.ColumnSeparator = "│"
	printer.RowSeparator = "─"
	printer.HeaderBgColor = tablewriter.BgBlackColor
	printer.HeaderFgColor = tablewriter.FgGreenColor

	printabledata := make([]PrintableDBInfo, 0)
	for _, db := range data {
		pdb := PrintableDBInfo{
			DBName:    db.name,
			KeysCount: db.count,
			Size:      db.size,
		}

		printabledata = append(printabledata, pdb)
	}

	printer.Print(printabledata)

	fmt.Print("\n")
	for _, db := range data {
		printer.Print(db.tables)
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

func DBsInfo(cliCtx *cli.Context) ([]DBInfo, error) {
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

	// filter out empty tables
	if cliCtx.String(flags.DBPopulatedFlag) {
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
	}

	return data, nil
}

func getAllDbsNames(cliCtx *cli.Context) ([]string, error) {
	var data []string
	url := "http://" + cliCtx.String(flags.DebugURLFlag.Name) + "/debug/diag/dbs"

	err := util.MakeHttpGetCall(cliCtx.Context, url, &data)
	if err != nil {
		return data, err
	}

	return data, nil
}

func getDb(cliCtx *cli.Context, dbName string) ([]BDTableInfo, error) {
	var data []BDTableInfo
	url := "http://" + cliCtx.String(flags.DebugURLFlag.Name) + "/debug/diag/dbs/" + dbName + "/tables"

	err := util.MakeHttpGetCall(cliCtx.Context, url, &data)
	if err != nil {
		return data, err
	}

	return data, nil
}
