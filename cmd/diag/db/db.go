package db

import (
	"fmt"
	"os"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cmd/diag/flags"
	"github.com/ledgerwatch/erigon/cmd/diag/util"
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
		Usage:    "DB name to print info about. If not set, all dbs will be printed.",
		Required: false,
		Value:    "",
	}
)

var Command = cli.Command{
	Action:    startPrintDBsInfo,
	Name:      "databases",
	Aliases:   []string{"dbs"},
	Usage:     "Print database tables info.",
	ArgsUsage: "",
	Flags: []cli.Flag{
		&flags.DebugURLFlag,
		&flags.OutputFlag,
		&DBPopulatedFlag,
		&DBNameFlag,
	},
	Description: ``,
}

func startPrintDBsInfo(cliCtx *cli.Context) error {
	data, err := DBsInfo(cliCtx)
	if err != nil {
		return err
	}

	dbToPrint := cliCtx.String(DBNameFlag.Name)

	if dbToPrint != "" {
		for _, db := range data {
			if db.name == dbToPrint {
				printDBsInfo([]DBInfo{db})
				return nil
			}
		}

		fmt.Printf("DB %s not found\n", dbToPrint)
		return nil
	}

	printDBsInfo(data)

	txt := text.Colors{text.BgGreen, text.Bold}
	fmt.Println(txt.Sprint("To get detailed info about Erigon node state use 'diag ui' command."))
	return nil
}

func printDBsInfo(data []DBInfo) {
	txt := text.Colors{text.FgBlue, text.Bold}
	fmt.Println(txt.Sprint("Databases Info:"))
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"DB Name", "Keys Count", "Size"})

	for _, db := range data {
		t.AppendRow(table.Row{db.name, db.count, db.size})
	}

	t.AppendSeparator()
	t.Render()

	t.ResetHeaders()
	t.AppendHeader(table.Row{"Table Name", "Keys Count", "Size"})

	for _, db := range data {
		t.ResetRows()
		fmt.Println(txt.Sprint("DB " + db.name + " tables:"))
		for _, tbl := range db.tables {
			t.AppendRow(table.Row{tbl.Name, tbl.Count, common.ByteCount(tbl.Size)})
		}

		t.AppendSeparator()
		t.Render()
		fmt.Print("\n")
	}
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
	if cliCtx.Bool(DBPopulatedFlag.Name) {
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
	url := "http://" + cliCtx.String(flags.DebugURLFlag.Name) + flags.ApiPath + "/dbs"

	err := util.MakeHttpGetCall(cliCtx.Context, url, &data)
	if err != nil {
		return data, err
	}

	return data, nil
}

func getDb(cliCtx *cli.Context, dbName string) ([]BDTableInfo, error) {
	var data []BDTableInfo
	url := "http://" + cliCtx.String(flags.DebugURLFlag.Name) + flags.ApiPath + "/dbs/" + dbName + "/tables"

	err := util.MakeHttpGetCall(cliCtx.Context, url, &data)
	if err != nil {
		return data, err
	}

	return data, nil
}
