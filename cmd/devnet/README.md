# Devnet

This is an automated tool run on the devnet that simulates p2p connection between nodes and ultimately tests operations on them.
See [DEV_CHAIN](https://github.com/ledgerwatch/erigon/blob/devel/DEV_CHAIN.md) for a manual version.

The devnet code performs 3 main functions:

* It runs a series of internal Erigon nodes which will connect to each other to form an internal P2P network
* It allows for the specification of a series of scenarios which will be run against the nodes on that internal network
* It can optionally run a `support` connection which allows the nodes on the network to be connected to the Erigon diagnostic system

The specification of both nodes and scenarios for the devenet is done by specifying configuraion objects.  These objects are currently build in code using go `structs` but are cabable of being read as configuration.

## Devnet runtime start-up

The devnet runs as a single `go` process which can be started with the following arguments:

| Arg | Required | Default | Description |
| --- | -------- | ------- | ----------- |
| datadir | Y | | The data directory for the devnet contains all the devnet nodes data and logs |
| chain | N | dev | The devnet chain to run currently supported: dev or bor-devnet | 
| bor.withoutheimdall | N | false | Bor specific - tells the devnet to run without a heimdall service.  With this flag only a single validator is supported on the devnet |
| metrics | N | false | Enable metrics collection and reporting from devnet nodes |
| metrics.node | N | 0 | At the moment only one node on the network can produce metrics.  This value specifies index of the node in the cluster to attach to |
| metrics.port | N | 6060 | The network port of the node to connect to for gather ing metrics |
| diagnostics.url | N | | URL of the diagnostics system provided by the support team, include unique session PIN, if this is specified the devnet will start a `support` tunnel and connect to the diagnostics platform to provide metrics from the specified node on the devnet | 
| insecure | N | false | Used if `diagnostics.url` is set to allow communication with diagnostics system using self-signed TLS certificates |

## Network Configuration

Networks configurations are currently specified in code in `main.go` in the `selectNetwork` function.  This contains a series of `structs` with the following structue, for eample:

```go
		return &devnet.Network{
			DataDir:            dataDir,
			Chain:              networkname.DevChainName,
			Logger:             logger,
			BasePrivateApiAddr: "localhost:10090",
			BaseRPCAddr:        "localhost:8545",
			Nodes: []devnet.Node{
				args.Miner{
					Node: args.Node{
						ConsoleVerbosity: "0",
						DirVerbosity:     "5",
					},
					AccountSlots: 200,
				},
				args.NonMiner{
					Node: args.Node{
						ConsoleVerbosity: "0",
						DirVerbosity:     "5",
					},
				},
			},
		}, nil	
```

Base IP's and addresses are iterated for each node in the network - to ensure that when the network starts there are no port clashes as the entire nework operates in a single process, hence shares a common host.  Individual nodes will be configured with a default set of command line arguments dependent on type. To see the default arguments per node look at the `args\node.go` file where these are specified as tags on the struct members.

## Scenario Configuration

Scenarios are similarly specified in code in `main.go` in the `action` function.  This is the initial configration:

```go
    scenarios.Scenario{
        Name: "all",
        Steps: []*scenarios.Step{
            {Text: "InitSubscriptions", Args: []any{[]requests.SubMethod{requests.Methods.ETHNewHeads}}},
            {Text: "PingErigonRpc"},
            {Text: "CheckTxPoolContent", Args: []any{0, 0, 0}},
            {Text: "SendTxWithDynamicFee", Args: []any{recipientAddress, accounts.DevAddress, sendValue}},
            {Text: "AwaitBlocks", Args: []any{2 * time.Second}},
        },
    })
```

Scenarios are created a groups of steps which are created by regestering a `step` handler too see an example of this take a look at the `commands\ping.go` file which adds a ping rpc method (see `PingErigonRpc` above).

This illustrates the registratio process.  The `init` function in the file registers the method with the `scenarios` package - which uses the function name as the default step name.  Others can be added with additional string arguments fo the `StepHandler` call where they will treated as regular expressions to be matched when processing scenario steps.

```go
func init() {
	scenarios.MustRegisterStepHandlers(
		scenarios.StepHandler(PingErigonRpc),
	)
}
```
Each step method will be called with a `context.Context` as its initial argument. This context provides access to the underlying devnet - so the sptep handler can use it for processing.

```go
func PingErigonRpc(ctx context.Context) error {
    ...
}
```
The devnet currently supports the following context methods:

```go
func Logger(ctx context.Context) log.Logger
```

Fetch the devnet logger - which can be used for logging step processing.

```go
func SelectNode(ctx context.Context, selector ...interface{}) 
```

This method selects a node on the network the selector argument can be either an `int` index or an implementation of the `network.NodeSelector` interface.  If no selector is specified a either the `current node` will be returned or a node will be selected at random from the network.

```go
func SelectMiner(ctx context.Context, selector ...interface{})
```

This method selects a mining node on the network the selector argument can be either an `int` index or an implementation of the `network.NodeSelector` interface.  If no selector is specified a either the `current node` will be returned or a miner will be selected at random from the network.

```go
func SelectNonMiner(ctx context.Context, selector ...interface{})
```

This method selects a non mining node on the network the selector argument can be either an `int` index or an implementation of the `network.NodeSelector` interface.  If no selector is specified a either the `current node` will be returned or a non-miner will be selected at random from the network.

```go
func WithCurrentNode(ctx context.Context, selector interface{}) Context
```
This method sets the `current node` on the network.  This can be called to create a context with a fixed node which can be passed to subsequent step functions so that they will operate on a defined network node.

```go
func CurrentNode(ctx context.Context) Node
```

This method returns the current node from the network context.
