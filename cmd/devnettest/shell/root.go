package shell

import (
	"github.com/abiosoft/ishell/v2"
)

var (
	reqId int
)

func Execute() {
	shell := ishell.New()
	shell.Println("Devnet test tool for Erigon")

	shell.AddCmd(&ishell.Cmd{
		Name: "start",
		Help: "start lets you pick a test command to start with",
		Func: func(c *ishell.Context) {
			choice := c.MultiChoice([]string{
				"get-balance",
				"send-tx",
				"get-transaction-count",
				"logs",
				"parity",
				"mock-request",
				"txpool-content",
			}, "Pick a devnet test command to get up and running")

			switch choice {
			case 0:
				getBalance(c, shell)
			case 1:
				sendTx(c, shell)
			default:
				c.Println("Sorry, you're wrong.")
			}
		},
	})

	// run shell
	shell.Run()
}
